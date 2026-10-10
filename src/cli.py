"""Command-line entry point. Orchestration stays thin: each command calls one module."""
import argparse
import os
import sys
from pathlib import Path

import pandas as pd

from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common import audit, db


def _run_dir(key: str, run_id: str) -> Path:
    return path_for(key) / f'run_id={run_id}'


def curated_file(run_id: str) -> Path:
    return _run_dir('curated_dir', run_id) / 'sales_order_lines.parquet'


def maybe_fail(step: str) -> None:
    """Deliberate failure hook for the Airflow failure/retry/recovery experiment.

    FAIL_TASK=<step> and FAIL_MODE=always  -> the step fails on every attempt.
    FAIL_TASK=<step> and FAIL_MODE=once    -> the step fails only on try 1 (succeeds on retry).
    """
    if os.getenv('FAIL_TASK', 'none') != step:
        return
    mode = os.getenv('FAIL_MODE', 'none')
    try_number = int(os.getenv('TRY_NUMBER', '1') or 1)
    if mode == 'always' or (mode == 'once' and try_number <= 1):
        raise RuntimeError(f'Deliberate failure injected in step "{step}" (mode={mode}, try={try_number})')


def cmd_extract() -> str:
    run_id = audit.new_run_id()
    maybe_fail('extract')
    from src.extract.files import extract_sources
    raw = extract_sources(run_id)
    audit.remember_run_id(run_id)
    db.start_run(run_id)
    counts = {f.name: sum(1 for _ in f.open(encoding='utf-8')) - 1 for f in raw.glob('*.csv')}
    print(f'[extract] run_id={run_id} raw_dir={raw} csv_data_rows={counts}')
    return run_id


def cmd_transform() -> None:
    from src.transform.staging import build_staging
    from src.transform.curated import build_curated
    from src.benchmark.storage import write_partitioned_parquet
    run_id = audit.current_run_id()
    maybe_fail('transform')
    raw = _run_dir('raw_dir', run_id)
    if not raw.exists():
        raise FileNotFoundError(f'Raw snapshot not found for run {run_id}: {raw}')
    staging, q_stage = build_staging(raw, run_id)
    curated, q_cur = build_curated(staging, run_id)
    quarantine = pd.concat([q_stage, q_cur], ignore_index=True)

    stg_dir = _run_dir('staging_dir', run_id)
    stg_dir.mkdir(parents=True, exist_ok=True)
    for name, df in staging.items():
        df.to_parquet(stg_dir / f'{name}.parquet', index=False)
    cur_path = curated_file(run_id)
    cur_path.parent.mkdir(parents=True, exist_ok=True)
    curated.to_parquet(cur_path, index=False)
    q_dir = path_for('quarantine_dir')
    q_dir.mkdir(parents=True, exist_ok=True)
    quarantine.to_csv(q_dir / f'quarantine_run_id={run_id}.csv', index=False)
    write_partitioned_parquet(curated, path_for('partition_dir') / 'sales_order_lines')

    n_stage = {k: len(v) for k, v in staging.items()}
    db.update_run(run_id, rows_staging=n_stage['orders'], rows_curated=len(curated), rows_quarantined=len(quarantine))
    print(f'[transform] run_id={run_id} staging={n_stage} curated={len(curated)} quarantined={len(quarantine)}')
    print(quarantine.groupby(['entity', 'reason']).size().to_string())


def _read_curated(run_id: str) -> pd.DataFrame:
    path = curated_file(run_id)
    if not path.exists():
        raise FileNotFoundError(f'Curated file not found for run {run_id}: {path}. Run transform first.')
    return pd.read_parquet(path)


def cmd_load() -> None:
    from src.load.postgres import upsert_curated
    run_id = audit.current_run_id()
    maybe_fail('load')
    affected = upsert_curated(_read_curated(run_id), run_id)
    print(f'[load] rows affected (inserted+updated)={affected}')


def cmd_load_partition(year: int, month: int) -> None:
    from src.load.postgres import load_partition
    from src.benchmark.storage import read_partition
    run_id = audit.current_run_id()
    maybe_fail('load')
    root = path_for('partition_dir') / 'sales_order_lines'
    if not root.exists():
        raise FileNotFoundError(f'Partitioned dataset not found: {root}. Run transform first.')
    part = read_partition(root, year, month)  # reads only the selected folder
    part['order_year'], part['order_month'] = year, month
    affected = load_partition(part, year, month, run_id)
    print(f'[load-partition] rows in partition={len(part)} rows affected={affected}')


def cmd_validate(mode: str = 'full', year: int | None = None, month: int | None = None) -> None:
    from src.validate.quality import validate_curated
    from src.load.postgres import count_rows
    run_id = audit.current_run_id()
    maybe_fail('validate')
    curated = _read_curated(run_id)
    errors = validate_curated(curated)
    if mode == 'partition':
        expected = int(((curated['order_year'] == year) & (curated['order_month'] == month)).sum())
        in_db = count_rows(year, month)
        label = f'partition {year}-{month:02d}'
    else:
        expected, in_db, label = len(curated), count_rows(), 'full table'
    if in_db < expected:
        errors.append(f'PostgreSQL {label} has {in_db} rows, expected at least {expected}')
    if errors:
        for e in errors:
            print('[validate] ERROR:', e)
        raise SystemExit(1)
    db.update_run(run_id, status='SUCCESS', message=f'validated {label}: {expected} rows')
    print(f'[validate] run_id={run_id} OK - {label}: curated={expected}, postgres={in_db}')


def cmd_benchmark(repeats: int, year: int | None = None, month: int | None = None) -> None:
    from src.benchmark.storage import run_benchmark, benchmark_partition
    run_id = audit.current_run_id()
    cur = curated_file(run_id)
    out_dir = path_for('benchmark_dir')
    res = run_benchmark(cur, out_dir, repeats)
    print(res.drop(columns='notes').to_string(index=False))
    if year is None or month is None:  # default: the largest month
        year, month = pd.read_parquet(cur, columns=['order_year', 'order_month']).value_counts().idxmax()
    part = benchmark_partition(path_for('partition_dir') / 'sales_order_lines', cur, int(year), int(month), repeats)
    part.to_csv(out_dir / 'partition_benchmark.csv', index=False)
    print(part.to_string(index=False))


def main(argv=None):
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    v = sub.add_parser('validate')
    v.add_argument('--mode', choices=['full', 'partition'], default='full')
    v.add_argument('--year', type=int)
    v.add_argument('--month', type=int)
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=SETTINGS['storage_benchmark']['repeats'])
    b.add_argument('--year', type=int); b.add_argument('--month', type=int)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args(argv)

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        with db.connect() as conn:
            print('PostgreSQL=', conn.execute('SELECT version()').fetchone()[0].split(',')[0])
            tables = conn.execute("SELECT table_schema||'.'||table_name FROM information_schema.tables "
                                  "WHERE table_schema IN ('curated','audit') ORDER BY 1").fetchall()
            print('Tables=', [t[0] for t in tables])
        return

    try:
        if args.command == 'extract':
            cmd_extract()
        elif args.command == 'transform':
            cmd_transform()
        elif args.command == 'load':
            cmd_load()
        elif args.command == 'load-partition':
            cmd_load_partition(args.year, args.month)
        elif args.command == 'validate':
            if args.mode == 'partition' and (args.year is None or args.month is None):
                parser.error('--mode partition requires --year and --month')
            cmd_validate(args.mode, args.year, args.month)
        elif args.command == 'benchmark':
            cmd_benchmark(args.repeats, args.year, args.month)
        elif args.command == 'run-all':
            run_id = cmd_extract()
            os.environ['PIPELINE_RUN_ID'] = run_id
            cmd_transform()
            cmd_load()
            cmd_validate()
    except (Exception, SystemExit) as exc:
        if isinstance(exc, SystemExit) and exc.code in (0, None):
            raise
        msg = f'{args.command} failed: {exc}'
        audit.append_event({'event': 'step_failed', 'command': args.command, 'error': str(exc)})
        try:
            db.update_run(audit.current_run_id(), status='FAILED', message=msg[:500])
        except Exception:
            pass
        print('[error]', msg, file=sys.stderr)
        raise


if __name__ == '__main__':
    main()
