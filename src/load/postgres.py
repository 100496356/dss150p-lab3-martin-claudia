"""Rerun-safe loading into PostgreSQL (curated.sales_order_lines)."""
import pandas as pd

from src.common.db import connect
from src.transform.curated import CURATED_COLUMNS

TABLE = 'curated.sales_order_lines'
UPDATE_COLUMNS = [c for c in CURATED_COLUMNS if c != 'order_id']

UPSERT_SQL = f"""
INSERT INTO {TABLE} ({', '.join(CURATED_COLUMNS)})
SELECT {', '.join(CURATED_COLUMNS)} FROM tmp_sales
ON CONFLICT (order_id) DO UPDATE SET
  {', '.join(f'{c} = EXCLUDED.{c}' for c in UPDATE_COLUMNS)}
WHERE {TABLE}.record_hash IS DISTINCT FROM EXCLUDED.record_hash
RETURNING (xmax = 0) AS inserted
"""


def _prepare(df: pd.DataFrame) -> list[tuple]:
    out = df[CURATED_COLUMNS].copy()
    for col in ['order_timestamp', 'source_updated_at', 'processed_at_utc']:
        out[col] = pd.to_datetime(out[col], utc=True)
    out = out.astype(object).where(out.notna(), None)
    return list(out.itertuples(index=False, name=None))


def _upsert(conn, df: pd.DataFrame) -> dict:
    """Stage rows in a temp table with COPY, then INSERT ... ON CONFLICT DO UPDATE.

    Only rows whose record_hash changed are updated, so a rerun with unchanged data
    affects 0 rows and never creates duplicate order_id values.
    """
    conn.execute(f'CREATE TEMP TABLE tmp_sales (LIKE {TABLE} INCLUDING DEFAULTS) ON COMMIT DROP')
    with conn.cursor() as cur:
        with cur.copy(f"COPY tmp_sales ({', '.join(CURATED_COLUMNS)}) FROM STDIN") as copy:
            for row in _prepare(df):
                copy.write_row(row)
        cur.execute(UPSERT_SQL)
        flags = [r[0] for r in cur.fetchall()]
    inserted = sum(1 for f in flags if f)
    return {'input_rows': len(df), 'inserted': inserted, 'updated': len(flags) - inserted,
            'unchanged': len(df) - len(flags)}


def upsert_curated(df, run_id: str) -> int:
    """Load curated rows with UPSERT on order_id. Returns rows affected (inserted + updated)."""
    with connect() as conn:
        stats = _upsert(conn, df)
    print(f'[load] run_id={run_id} {stats}')
    return stats['inserted'] + stats['updated']


def partition_key(year: int, month: int) -> str:
    return f'order_year={year}/order_month={month:02d}'


def load_partition(df, year: int, month: int, run_id: str) -> int:
    """Load only the selected year/month partition and record it in audit.partition_loads.

    The data load and the audit record are written in the same transaction.
    """
    if not 1 <= month <= 12:
        raise ValueError(f'month must be between 1 and 12, got {month}')
    part = df
    if 'order_year' in df.columns:
        part = df[(df['order_year'] == year) & (df['order_month'] == month)]
    if part.empty:
        raise ValueError(f'No rows found for partition {partition_key(year, month)}')
    with connect() as conn:
        stats = _upsert(conn, part)
        conn.execute(
            """INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
               VALUES (%s, now(), %s, %s)
               ON CONFLICT (partition_key) DO UPDATE
               SET loaded_at_utc = EXCLUDED.loaded_at_utc, row_count = EXCLUDED.row_count,
                   pipeline_run_id = EXCLUDED.pipeline_run_id""",
            (partition_key(year, month), len(part), run_id),
        )
    print(f'[load-partition] {partition_key(year, month)} run_id={run_id} {stats}')
    return stats['inserted'] + stats['updated']


def count_rows(year: int | None = None, month: int | None = None) -> int:
    sql = f'SELECT count(*) FROM {TABLE}'
    params: tuple = ()
    if year is not None:
        sql += (" WHERE EXTRACT(YEAR FROM order_timestamp AT TIME ZONE 'UTC') = %s"
                " AND EXTRACT(MONTH FROM order_timestamp AT TIME ZONE 'UTC') = %s")
        params = (year, month)
    with connect() as conn:
        return conn.execute(sql, params).fetchone()[0]
