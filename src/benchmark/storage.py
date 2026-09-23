import statistics
import time
import pandas as pd
from pathlib import Path


def _median_timed(fn, repeats: int) -> float:
    """Run fn() `repeats` times and return the median elapsed time in seconds.
    Using the median (not the mean) makes the result robust to one-off OS/disk
    cache hiccups skewing a single run."""
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times)


def _benchmark_file_format(name: str, df: pd.DataFrame, path: Path,
                            write_fn, read_fn, filter_status: str, repeats: int) -> dict:
    """Shared measurement routine for CSV / JSON Lines / Parquet."""
    t0 = time.perf_counter()
    write_fn(df, path)
    write_seconds = time.perf_counter() - t0

    file_size_bytes = path.stat().st_size

    full_read_seconds = _median_timed(lambda: read_fn(path), repeats)
    filtered_read_seconds = _median_timed(
        lambda: read_fn(path).pipe(lambda d: d[d['status'] == filter_status]), repeats
    )

    return {
        'storage_type': name,
        'file_size_bytes': file_size_bytes,
        'write_seconds': round(write_seconds, 6),
        'full_read_seconds': round(full_read_seconds, 6),
        'filtered_read_seconds': round(filtered_read_seconds, 6),
        'row_count': len(df),
        'notes': '',
    }


def _benchmark_postgres(repeats: int, filter_status: str) -> dict:
    """PostgreSQL is a server system, not a file — size is measured with a
    PostgreSQL function (pg_total_relation_size), not a file-size comparison,
    and write time is not re-timed here since the table is already loaded by
    the load stage (per the lab's own guidance)."""
    import psycopg
    from src.config import DB

    def _connect():
        return psycopg.connect(
            host=DB['host'], port=DB['port'], dbname=DB['dbname'],
            user=DB['user'], password=DB['password'],
        )

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_total_relation_size('curated.sales_order_lines')")
            size_bytes = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM curated.sales_order_lines")
            row_count = cur.fetchone()[0]

        def _full_query():
            with conn.cursor() as c:
                c.execute("SELECT * FROM curated.sales_order_lines")
                c.fetchall()

        def _filtered_query():
            with conn.cursor() as c:
                c.execute("SELECT * FROM curated.sales_order_lines WHERE status = %s", (filter_status,))
                c.fetchall()

        full_read_seconds = _median_timed(_full_query, repeats)
        filtered_read_seconds = _median_timed(_filtered_query, repeats)

    return {
        'storage_type': 'PostgreSQL',
        'file_size_bytes': size_bytes,
        'write_seconds': '',  # already loaded via UPSERT in the load stage — see notes
        'full_read_seconds': round(full_read_seconds, 6),
        'filtered_read_seconds': round(filtered_read_seconds, 6),
        'row_count': row_count,
        'notes': 'file_size_bytes is pg_total_relation_size (table+index bytes), not a flat file; '
                 'write_seconds intentionally blank — table already loaded via UPSERT in the load stage.',
    }


def run_benchmark(curated_path, output_dir, repeats: int = 5):
    """Compare the same logical dataset in CSV, JSON Lines, Parquet, and PostgreSQL.

    Writes templates/benchmark_results_template.csv-shaped results to
    <output_dir>/benchmark_results.csv and returns the results as a DataFrame.
    """
    from src.config import SETTINGS
    filter_status = SETTINGS['storage_benchmark']['filter_status']

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(curated_path)

    results = []

    results.append(_benchmark_file_format(
        'CSV', df, output_dir / 'sales_order_lines.csv',
        write_fn=lambda d, p: d.to_csv(p, index=False),
        read_fn=lambda p: pd.read_csv(p),
        filter_status=filter_status, repeats=repeats,
    ))

    results.append(_benchmark_file_format(
        'JSON Lines', df, output_dir / 'sales_order_lines.jsonl',
        write_fn=lambda d, p: d.to_json(p, orient='records', lines=True, date_format='iso'),
        read_fn=lambda p: pd.read_json(p, lines=True),
        filter_status=filter_status, repeats=repeats,
    ))

    results.append(_benchmark_file_format(
        'Parquet', df, output_dir / 'sales_order_lines.parquet',
        write_fn=lambda d, p: d.to_parquet(p, index=False, compression='snappy'),
        read_fn=lambda p: pd.read_parquet(p),
        filter_status=filter_status, repeats=repeats,
    ))

    results.append(_benchmark_postgres(repeats, filter_status))

    results_df = pd.DataFrame(results, columns=[
        'storage_type', 'file_size_bytes', 'write_seconds',
        'full_read_seconds', 'filtered_read_seconds', 'row_count', 'notes',
    ])
    results_df.to_csv(output_dir / 'benchmark_results.csv', index=False)
    return results_df


def write_partitioned_parquet(df: pd.DataFrame, output_dir):
    """Write Parquet partitioned by order_year/order_month."""
    output_dir = Path(output_dir)
    df = df.copy()
    ts = pd.to_datetime(df['order_timestamp'], utc=True)
    df['order_year'] = ts.dt.year
    df['order_month'] = ts.dt.month
    df.to_parquet(output_dir, index=False, partition_cols=['order_year', 'order_month'])
    return output_dir
