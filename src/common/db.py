"""PostgreSQL connection and audit.pipeline_runs bookkeeping."""
import psycopg

from src.config import DB


def connect(autocommit: bool = False) -> psycopg.Connection:
    if not DB['password']:
        raise RuntimeError('POSTGRES_PASSWORD is not set. Copy .env.example to .env and set a password.')
    return psycopg.connect(
        host=DB['host'], port=DB['port'], dbname=DB['dbname'],
        user=DB['user'], password=DB['password'], autocommit=autocommit, connect_timeout=10,
    )


def start_run(run_id: str) -> None:
    with connect() as conn:
        conn.execute(
            """INSERT INTO audit.pipeline_runs (pipeline_run_id, started_at_utc, status)
               VALUES (%s, now(), 'RUNNING')
               ON CONFLICT (pipeline_run_id) DO UPDATE
               SET status = 'RUNNING', completed_at_utc = NULL, message = NULL""",
            (run_id,),
        )


def update_run(run_id: str, **fields) -> None:
    """Update counters/status for a run. Creates the row if extract was skipped."""
    allowed = {'status', 'rows_staging', 'rows_curated', 'rows_quarantined', 'message'}
    fields = {k: v for k, v in fields.items() if k in allowed}
    if not fields:
        return
    sets = ', '.join(f'{k} = %({k})s' for k in fields)
    done = ", completed_at_utc = now()" if fields.get('status') in ('SUCCESS', 'FAILED') else ''
    with connect() as conn:
        conn.execute(
            """INSERT INTO audit.pipeline_runs (pipeline_run_id, started_at_utc, status)
               VALUES (%(run_id)s, now(), 'RUNNING') ON CONFLICT (pipeline_run_id) DO NOTHING""",
            {'run_id': run_id},
        )
        conn.execute(
            f'UPDATE audit.pipeline_runs SET {sets}{done} WHERE pipeline_run_id = %(run_id)s',
            {**fields, 'run_id': run_id},
        )
