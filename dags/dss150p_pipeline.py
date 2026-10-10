"""DSS150P sales pipeline: extract -> transform -> load -> validate.

Parameters (Trigger DAG w/ config):
  run_mode     full | partition   full = UPSERT every curated row; partition = load only year/month
  year, month  partition to load/validate when run_mode = partition
  fail_task    none | extract | transform | load | validate   step where a failure is injected
  failure_mode none | once | always
               once   -> the step fails on its first try and succeeds on the automatic retry
               always -> the step fails on every try, so the run ends FAILED (then re-run with none)
"""
from datetime import datetime, timedelta
import json
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'
EVENTS_FILE = Path(PROJECT) / 'data' / 'runs' / 'airflow_events.jsonl'


def _record(event: str, context) -> None:
    """Write a concise JSON line for every failure/retry so the evidence survives log rotation."""
    ti = context['task_instance']
    exc = context.get('exception')
    rec = {
        'event': event,
        'dag_id': ti.dag_id,
        'run_id': context['run_id'],
        'task_id': ti.task_id,
        'try_number': ti.try_number,
        'max_tries': ti.max_tries,
        'params': dict(context.get('params') or {}),
        'error': str(exc)[:300] if exc else None,
        'logged_at_utc': datetime.utcnow().isoformat() + 'Z',
    }
    print(f'{event.upper()}:', json.dumps(rec))
    try:
        EVENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with EVENTS_FILE.open('a', encoding='utf-8') as f:
            f.write(json.dumps(rec) + '\n')
    except OSError as err:  # never let the callback hide the original error
        print('could not write events file:', err)
    if event == 'task_failed':
        try:
            from src.common.audit import safe_run_id
            from src.common.db import update_run
            update_run(safe_run_id(context['run_id']), status='FAILED',
                       message=f"{ti.task_id} failed after {ti.try_number} tries: {rec['error']}")
        except Exception as err:
            print('could not update audit.pipeline_runs:', err)


def failure_callback(context):
    _record('task_failed', context)


def retry_callback(context):
    _record('task_retry', context)


DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(minutes=10),
    'on_failure_callback': failure_callback,
    'on_retry_callback': retry_callback,
}

# Shared environment for every task: one run id per DAG run + failure-injection settings.
ENV = (
    f'cd {PROJECT} && '
    'export PIPELINE_RUN_ID="{{ run_id }}" '
    'FAIL_TASK="{{ params.fail_task }}" FAIL_MODE="{{ params.failure_mode }}" '
    'TRY_NUMBER="{{ ti.try_number }}" && '
)

LOAD_CMD = (
    "{% if params.run_mode == 'partition' %}"
    'python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }}'
    '{% else %}python -m src.cli load{% endif %}'
)
VALIDATE_CMD = (
    "{% if params.run_mode == 'partition' %}"
    'python -m src.cli validate --mode partition --year {{ params.year }} --month {{ params.month }}'
    '{% else %}python -m src.cli validate{% endif %}'
)

with DAG(
    dag_id='dss150p_sales_pipeline',
    description='Raw -> staging -> curated sales pipeline with rerun-safe PostgreSQL load',
    doc_md=__doc__,
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition'], description='full load or one partition'),
        'year': Param(2026, type='integer', minimum=2000, maximum=2100),
        'month': Param(1, type='integer', minimum=1, maximum=12),
        'fail_task': Param('none', enum=['none', 'extract', 'transform', 'load', 'validate'],
                           description='step where a deliberate failure is injected'),
        'failure_mode': Param('none', enum=['none', 'once', 'always'],
                              description='once = fail first try then recover on retry; always = fail all tries'),
    },
    tags=['DSS150P'],
) as dag:
    extract = BashOperator(task_id='extract', bash_command=ENV + 'python -m src.cli extract')
    transform = BashOperator(task_id='transform', bash_command=ENV + 'python -m src.cli transform')
    load = BashOperator(task_id='load', bash_command=ENV + LOAD_CMD)
    validate = BashOperator(task_id='validate', bash_command=ENV + VALIDATE_CMD)

    extract >> transform >> load >> validate
