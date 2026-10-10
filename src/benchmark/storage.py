"""Storage benchmark: CSV vs JSON Lines vs Parquet vs PostgreSQL, plus partitioning."""
from pathlib import Path
from statistics import median
import shutil
import time

import pandas as pd
import pyarrow.dataset as ds

from src.common.db import connect
from src.config import SETTINGS
from src.transform.curated import CURATED_COLUMNS

BENCH_TABLE = 'benchmark.sales_order_lines'
PG_TYPES = {
    'quantity': 'INTEGER', 'unit_price': 'NUMERIC(14,2)', 'discount_pct': 'NUMERIC(6,4)',
    'gross_amount': 'NUMERIC(16,2)', 'discount_amount': 'NUMERIC(16,2)', 'net_amount': 'NUMERIC(16,2)',
    'order_timestamp': 'TIMESTAMPTZ', 'source_updated_at': 'TIMESTAMPTZ', 'processed_at_utc': 'TIMESTAMPTZ',
    'order_year': 'INTEGER', 'order_month': 'INTEGER',
}


def _timed(fn, repeats: int):
    times, result = [], None
    for _ in range(repeats):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    return median(times), result


def _dir_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob('*') if p.is_file())


def _pg_write(df: pd.DataFrame) -> None:
    cols = list(df.columns)
    with connect() as conn:
        conn.execute('CREATE SCHEMA IF NOT EXISTS benchmark')
        conn.execute(f'DROP TABLE IF EXISTS {BENCH_TABLE}')
        col_defs = ', '.join(f'{c} {PG_TYPES.get(c, "TEXT")}' for c in cols)
        conn.execute(f'CREATE TABLE {BENCH_TABLE} ({col_defs})')
        rows = df.astype(object).where(df.notna(), None).itertuples(index=False, name=None)
        with conn.cursor().copy(f"COPY {BENCH_TABLE} ({', '.join(cols)}) FROM STDIN") as copy:
            for r in rows:
                copy.write_row(r)
        conn.execute(f'ANALYZE {BENCH_TABLE}')


def _pg_query(sql: str, params=()) -> int:
    with connect() as conn:
        return len(conn.execute(sql, params).fetchall())


def run_benchmark(curated_path, output_dir, repeats: int = 5) -> pd.DataFrame:
    """Compare the same logical dataset in CSV, JSON Lines, Parquet and PostgreSQL.

    Captures size, write time, full-read time, filtered-read time and row count.
    Every timing is the median of `repeats` runs. Results go to storage_benchmark.csv.
    """
    output_dir = Path(output_dir)
    files_dir = output_dir / 'files'
    files_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(curated_path)
    status = SETTINGS['storage_benchmark']['filter_status']
    expected_filtered = int((df['status'] == status).sum())
    csv_p, json_p, pq_p = files_dir / 'sales.csv', files_dir / 'sales.jsonl', files_dir / 'sales.parquet'

    formats = {
        'CSV': (lambda: df.to_csv(csv_p, index=False), lambda: pd.read_csv(csv_p),
                lambda: (lambda d: d[d['status'] == status])(pd.read_csv(csv_p)), csv_p,
                'Text, row-based; no types stored, the whole file is parsed even for a filter.'),
        'JSON Lines': (lambda: df.to_json(json_p, orient='records', lines=True, date_format='iso'),
                       lambda: pd.read_json(json_p, lines=True),
                       lambda: (lambda d: d[d['status'] == status])(pd.read_json(json_p, lines=True)), json_p,
                       'Text, one object per line; repeats every key name, so it is the largest and slowest.'),
        'Parquet': (lambda: df.to_parquet(pq_p, index=False), lambda: pd.read_parquet(pq_p),
                    lambda: pd.read_parquet(pq_p, filters=[('status', '==', status)]), pq_p,
                    'Columnar, compressed and typed; filters are pushed down to row groups.'),
    }
    results = []
    for name, (write, read_all, read_filtered, path, note) in formats.items():
        w, _ = _timed(write, repeats)
        r, full = _timed(read_all, repeats)
        f, filt = _timed(read_filtered, repeats)
        assert len(full) == len(df) and len(filt) == expected_filtered, f'{name} returned wrong row count'
        results.append([name, _dir_size(path), w, r, f, len(full), note])

    w, _ = _timed(lambda: _pg_write(df), repeats)
    r, n_full = _timed(lambda: _pg_query(f'SELECT * FROM {BENCH_TABLE}'), repeats)
    f, n_filt = _timed(lambda: _pg_query(f'SELECT * FROM {BENCH_TABLE} WHERE status = %s', (status,)), repeats)
    assert n_full == len(df) and n_filt == expected_filtered, 'PostgreSQL returned wrong row count'
    with connect() as conn:
        pg_size = conn.execute("SELECT pg_total_relation_size('benchmark.sales_order_lines')").fetchone()[0]
    results.append(['PostgreSQL', pg_size, w, r, f, n_full,
                    'Table size incl. TOAST; write = COPY; reads go through the network protocol.'])

    out = pd.DataFrame(results, columns=['storage_type', 'file_size_bytes', 'write_seconds', 'full_read_seconds',
                                         'filtered_read_seconds', 'row_count', 'notes'])
    for c in ['write_seconds', 'full_read_seconds', 'filtered_read_seconds']:
        out[c] = out[c].round(4)
    out.to_csv(output_dir / 'storage_benchmark.csv', index=False)
    return out


def write_partitioned_parquet(df, output_dir) -> Path:
    """Write Parquet partitioned by order_year/order_month (hive style: order_year=2026/order_month=1/)."""
    output_dir = Path(output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)  # rewrite cleanly so reruns never leave stale files
    output_dir.mkdir(parents=True)
    cols = SETTINGS['storage_benchmark']['partition_columns']
    df.to_parquet(output_dir, index=False, partition_cols=cols)
    return output_dir


def read_partition(partition_root, year: int, month: int) -> pd.DataFrame:
    """Read a single partition using partition pruning."""
    dataset = ds.dataset(partition_root, format='parquet', partitioning='hive')
    table = dataset.to_table(filter=(ds.field('order_year') == year) & (ds.field('order_month') == month))
    return table.to_pandas()


def benchmark_partition(partition_root, curated_path, year: int, month: int, repeats: int = 5) -> pd.DataFrame:
    """Compare reading one month from the partitioned dataset vs. filtering the full file."""
    root = Path(partition_root)
    part_dir = root / f'order_year={year}' / f'order_month={month}'
    r_part, part = _timed(lambda: read_partition(root, year, month), repeats)
    r_full, full = _timed(lambda: (lambda d: d[(d['order_year'] == year) & (d['order_month'] == month)])(pd.read_parquet(curated_path)), repeats)
    assert len(part) == len(full)
    n_parts = len([p for p in root.glob('order_year=*/order_month=*') if p.is_dir()])
    return pd.DataFrame([
        ['partitioned parquet (pruned)', f'{year}-{month:02d}', len(part), _dir_size(part_dir), round(r_part, 4), n_parts],
        ['single parquet + filter', f'{year}-{month:02d}', len(full), _dir_size(Path(curated_path)), round(r_full, 4), 1],
    ], columns=['method', 'partition', 'row_count', 'bytes_read_or_scanned', 'median_read_seconds', 'files_or_partitions'])
