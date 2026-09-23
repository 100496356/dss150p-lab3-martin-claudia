import argparse
import json
import sys
import pandas as pd
from src.config import PROJECT_ROOT, DB, SETTINGS, path_for
from src.common.audit import new_run_id, utc_now_iso
from src.extract.files import extract_sources
from src.transform.staging import build_staging
from src.transform.curated import build_curated
from src.validate.quality import validate_curated
from src.load.postgres import upsert_curated, record_run_start, record_run_complete

# Small state file so that separate CLI invocations (e.g. `transform` after
# `extract`, or a standalone `load` after an earlier `run-all`) can find the
# most recently produced data without needing the caller to pass a run_id by
# hand. It only ever stores the id of the latest completed stage.
_STATE_PATH = PROJECT_ROOT / 'data' / '_pipeline_state.json'


def _read_state() -> dict:
    if _STATE_PATH.exists():
        return json.loads(_STATE_PATH.read_text())
    return {}


def _write_state(**updates):
    state = _read_state()
    state.update(updates)
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STATE_PATH.write_text(json.dumps(state, indent=2))


def _require_run_id(key: str) -> str:
    state = _read_state()
    run_id = state.get(key)
    if not run_id:
        raise RuntimeError(
            f"No {key} found in pipeline state. Run an earlier stage first "
            f"(e.g. 'extract'/'transform' before 'load'), or use 'run-all'."
        )
    return run_id


def _write_parquet_dict(frames: dict, base_dir, run_id: str):
    run_dir = base_dir / f'run_id={run_id}'
    run_dir.mkdir(parents=True, exist_ok=True)
    for name, df in frames.items():
        df.to_parquet(run_dir / f'{name}.parquet', index=False)
    return run_dir


def _write_parquet_single(df: pd.DataFrame, base_dir, run_id: str, filename: str):
    run_dir = base_dir / f'run_id={run_id}'
    run_dir.mkdir(parents=True, exist_ok=True)
    out_path = run_dir / filename
    df.to_parquet(out_path, index=False)
    return out_path


def cmd_extract():
    run_id = new_run_id()
    raw_dir = extract_sources(run_id)
    _write_state(latest_raw_run_id=run_id)
    print(f'[extract] run_id={run_id} raw_dir={raw_dir}')
    return run_id


def cmd_transform(run_id: str = None):
    # Reuse the SAME run_id as the extract stage that produced the raw data
    # we're about to read — this is one logical pipeline execution split
    # across separate CLI calls, not two independent runs, so every stage
    # should stamp records with a consistent pipeline_run_id.
    run_id = run_id or _require_run_id('latest_raw_run_id')
    raw_dir = path_for('raw_dir') / f'run_id={run_id}'
    if not raw_dir.exists():
        raise RuntimeError(f'Expected raw directory not found: {raw_dir}. Run extract first.')

    staging, staging_quarantine = build_staging(raw_dir, run_id)
    curated, curated_quarantine = build_curated(staging, run_id)

    _write_parquet_dict(
        {k: v for k, v in staging.items() if k != 'products_reference'},
        path_for('staging_dir'), run_id,
    )
    curated_path = _write_parquet_single(curated, path_for('curated_dir'), run_id, 'sales_order_lines.parquet')

    quarantine_parts = [q for q in (staging_quarantine, curated_quarantine) if len(q)]
    quarantine = pd.concat(quarantine_parts, ignore_index=True) if quarantine_parts else pd.DataFrame()
    if len(quarantine):
        _write_parquet_single(quarantine, path_for('quarantine_dir'), run_id, 'quarantine.parquet')

    _write_state(latest_transform_run_id=run_id, latest_curated_path=str(curated_path))
    print(f'[transform] run_id={run_id} staging_rows={ {k: len(v) for k, v in staging.items()} } '
          f'curated_rows={len(curated)} quarantine_rows={len(quarantine)}')
    return run_id, curated, staging, quarantine


def cmd_load(run_id: str = None, curated_df: pd.DataFrame = None):
    if curated_df is None:
        transform_run_id = run_id or _require_run_id('latest_transform_run_id')
        curated_path = path_for('curated_dir') / f'run_id={transform_run_id}' / 'sales_order_lines.parquet'
        if not curated_path.exists():
            raise RuntimeError(f'Expected curated file not found: {curated_path}. Run transform first.')
        curated_df = pd.read_parquet(curated_path)
        run_id = transform_run_id
    n = upsert_curated(curated_df, run_id)
    print(f'[load] run_id={run_id} rows_upserted={n}')
    return n


def cmd_validate(run_id: str = None, curated_df: pd.DataFrame = None):
    if curated_df is None:
        transform_run_id = run_id or _require_run_id('latest_transform_run_id')
        curated_path = path_for('curated_dir') / f'run_id={transform_run_id}' / 'sales_order_lines.parquet'
        curated_df = pd.read_parquet(curated_path)
    errors = validate_curated(curated_df)
    if errors:
        print('[validate] FAILED:')
        for e in errors:
            print(f'  - {e}')
    else:
        print('[validate] PASSED: no issues found')
    return errors


def cmd_run_all():
    run_id = new_run_id()
    started_at = utc_now_iso()

    stage = 'record_start'
    try:
        record_run_start(run_id, started_at)

        stage = 'extract'
        raw_dir = extract_sources(run_id)
        print(f'[run-all] extract complete: {raw_dir}')

        stage = 'transform'
        staging, staging_quarantine = build_staging(raw_dir, run_id)
        curated, curated_quarantine = build_curated(staging, run_id)
        _write_parquet_dict(
            {k: v for k, v in staging.items() if k != 'products_reference'},
            path_for('staging_dir'), run_id,
        )
        _write_parquet_single(curated, path_for('curated_dir'), run_id, 'sales_order_lines.parquet')
        quarantine_parts = [q for q in (staging_quarantine, curated_quarantine) if len(q)]
        quarantine = pd.concat(quarantine_parts, ignore_index=True) if quarantine_parts else pd.DataFrame()
        if len(quarantine):
            _write_parquet_single(quarantine, path_for('quarantine_dir'), run_id, 'quarantine.parquet')
        _write_state(latest_raw_run_id=run_id, latest_transform_run_id=run_id)
        print(f'[run-all] transform complete: curated_rows={len(curated)} quarantine_rows={len(quarantine)}')

        stage = 'validate'
        errors = validate_curated(curated)
        if errors:
            raise RuntimeError(f'Validation failed: {"; ".join(errors)}')
        print('[run-all] validate complete: no issues found')

        stage = 'load'
        n = upsert_curated(curated, run_id)
        print(f'[run-all] load complete: rows_upserted={n}')

        record_run_complete(
            run_id, status='success',
            rows_staging=sum(len(v) for k, v in staging.items() if k != 'products_reference'),
            rows_curated=len(curated), rows_quarantined=len(quarantine),
            message='ok',
        )
        print(f'[run-all] SUCCESS run_id={run_id}')
    except Exception as e:
        error_message = f'failed at stage={stage}: {type(e).__name__}: {e}'
        try:
            record_run_complete(
                run_id, status='failed',
                rows_staging=0, rows_curated=0, rows_quarantined=0,
                message=error_message,
            )
        except Exception as audit_error:
            # The database itself may be unreachable (the same reason the
            # pipeline failed) — don't let a failed audit write hide the
            # real, original error behind a second, more confusing traceback.
            print(f'[run-all] (also could not write failure to audit.pipeline_runs: '
                  f'{type(audit_error).__name__}: {audit_error})', file=sys.stderr)
        print(f'[run-all] FAILED at stage={stage}: {type(e).__name__}: {e}', file=sys.stderr)
        raise


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        print('PROJECT_ROOT=', PROJECT_ROOT)
        print('DB host/database=', DB['host'], DB['dbname'])
        print('Configured source=', SETTINGS['pipeline']['source_dir'])
        return

    if args.command == 'extract':
        cmd_extract(); return
    if args.command == 'transform':
        cmd_transform(); return
    if args.command == 'load':
        cmd_load(); return
    if args.command == 'validate':
        cmd_validate(); return
    if args.command == 'run-all':
        cmd_run_all(); return

    # benchmark / load-partition are implemented in Goal 3
    raise NotImplementedError(f'Command "{args.command}" is implemented in Goal 3')

if __name__ == '__main__':
    main()
