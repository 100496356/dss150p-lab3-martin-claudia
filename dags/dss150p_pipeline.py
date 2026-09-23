from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'

def failure_callback(context):
    """Print concise, diagnosable failure context: which task, which DAG
    run, and the underlying exception. This is deliberately just a print
    (visible in the task's own log and the scheduler log) rather than a
    write to our own audit.pipeline_runs table, since a failed task here
    means one CLI subcommand failed — the ETL-level audit trail for a
    'run-all'-style full pipeline execution is handled separately, inside
    src/cli.py's cmd_run_all(), for the case where the pipeline runs
    outside Airflow entirely."""
    ti = context['task_instance']
    print(
        'TASK FAILED: '
        f"dag_id={ti.dag_id} task_id={ti.task_id} run_id={context['run_id']} "
        f"try_number={ti.try_number} "
        f"exception={context.get('exception')}"
    )

DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': failure_callback,
    # Every task gets a hard ceiling so a stuck/hung process (e.g. a lost
    # database connection that never times out on its own) cannot block
    # the DAG run indefinitely.
    'execution_timeout': timedelta(minutes=15),
}

with DAG(
    dag_id='dss150p_sales_pipeline',
    start_date=datetime(2026, 1, 1),
    # Once daily at 02:00 UTC: after the prior day's order activity is
    # essentially complete, and well before typical business-hours load on
    # both the source systems and this pipeline's own PostgreSQL instance.
    schedule='0 2 * * *',
    # Backfilling every day since start_date would replay months of
    # already-superseded runs on first deploy for a job whose only useful
    # output is "today's latest curated snapshot" — there is no analytical
    # value in re-running yesterday's version of a dataset that gets fully
    # regenerated on each run, so catchup is explicitly disabled.
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition']),
        'year': Param(2026, type='integer'),
        'month': Param(1, type='integer', minimum=1, maximum=12),
    },
    tags=['DSS150P'],
) as dag:
    # PIPELINE_RUN_ID is set to Airflow's own {{ run_id }} on every task, so
    # all four stages of one DAG run share a single pipeline_run_id — the
    # same identity used throughout src/cli.py and stamped into every
    # staging/curated row and audit table entry for that run.
    extract = BashOperator(
        task_id='extract',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli extract',
    )
    transform = BashOperator(
        task_id='transform',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli transform',
    )
    # The load step branches on the run_mode parameter: 'full' loads every
    # curated row via the regular rerun-safe UPSERT; 'partition' loads only
    # the selected year/month partition via load-partition, using the DAG's
    # own year/month params rather than any value hard-coded in the DAG.
    load = BashOperator(
        task_id='load',
        bash_command=(
            f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli '
            '{% if params.run_mode == "partition" %}'
            'load-partition --year {{ params.year }} --month {{ params.month }}'
            '{% else %}'
            'load'
            '{% endif %}'
        ),
    )
    validate = BashOperator(
        task_id='validate',
        bash_command=f'cd {PROJECT} && PIPELINE_RUN_ID="{{{{ run_id }}}}" python -m src.cli validate',
    )

    extract >> transform >> load >> validate
