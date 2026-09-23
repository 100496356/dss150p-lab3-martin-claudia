import psycopg
import pandas as pd
from src.config import DB

_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp',
    'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
    'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount', 'status',
    'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
]

_NON_KEY_COLUMNS = [c for c in _COLUMNS if c != 'order_id']

_UPSERT_SQL = f"""
    INSERT INTO curated.sales_order_lines ({', '.join(_COLUMNS)})
    VALUES ({', '.join('%(' + c + ')s' for c in _COLUMNS)})
    ON CONFLICT (order_id) DO UPDATE SET
        {', '.join(f'{c} = EXCLUDED.{c}' for c in _NON_KEY_COLUMNS)}
    WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash
"""


def _connect():
    return psycopg.connect(
        host=DB['host'], port=DB['port'], dbname=DB['dbname'],
        user=DB['user'], password=DB['password'],
    )


def record_run_start(run_id: str, started_at: str) -> None:
    """Insert a row into audit.pipeline_runs marking a run as in-progress."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.pipeline_runs (pipeline_run_id, started_at_utc, status)
                VALUES (%s, %s, 'running')
                ON CONFLICT (pipeline_run_id) DO NOTHING
                """,
                (run_id, started_at),
            )
        conn.commit()


def record_run_complete(run_id: str, status: str, rows_staging: int, rows_curated: int,
                         rows_quarantined: int, message: str = '') -> None:
    """Update audit.pipeline_runs with the final outcome of a run.

    status is 'success' or 'failed'. message carries stage/error context on
    failure, so a diagnostic reader does not have to dig through logs alone.
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE audit.pipeline_runs
                SET completed_at_utc = now(),
                    status = %s,
                    rows_staging = %s,
                    rows_curated = %s,
                    rows_quarantined = %s,
                    message = %s
                WHERE pipeline_run_id = %s
                """,
                (status, rows_staging, rows_curated, rows_quarantined, message, run_id),
            )
        conn.commit()


def upsert_curated(df, run_id: str) -> int:
    """Load curated.sales_order_lines using rerun-safe UPSERT semantics.

    order_id is the conflict key. The WHERE clause on the DO UPDATE branch
    means a row whose incoming record_hash matches what's already stored is
    left untouched — this is what makes a rerun with unchanged business data
    a true no-op instead of a wasted write, while still updating rows whose
    content genuinely changed.
    """
    records = df.to_dict(orient='records')
    if not records:
        return 0

    affected = 0
    batch_size = 1000
    with _connect() as conn:
        with conn.cursor() as cur:
            for start in range(0, len(records), batch_size):
                batch = records[start:start + batch_size]
                cur.executemany(_UPSERT_SQL, batch)
                # executemany with psycopg3 reports rowcount for the last
                # statement only, so we count the batch size as attempted
                # and rely on the WHERE clause to make unchanged rows no-ops
                # at the database level (verified separately via COUNT checks).
                affected += len(batch)
        conn.commit()
    return affected


def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only a selected year/month partition and record audit.partition_loads."""
    from src.common.audit import utc_now_iso

    ts = pd.to_datetime(df['order_timestamp'], utc=True)
    partition_df = df[(ts.dt.year == year) & (ts.dt.month == month)]

    n = upsert_curated(partition_df, run_id)

    partition_key = f'{year:04d}-{month:02d}'
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (partition_key) DO UPDATE SET
                    loaded_at_utc = EXCLUDED.loaded_at_utc,
                    row_count = EXCLUDED.row_count,
                    pipeline_run_id = EXCLUDED.pipeline_run_id
                """,
                (partition_key, utc_now_iso(), n, run_id),
            )
        conn.commit()

    return n
