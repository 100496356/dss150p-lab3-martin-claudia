# Run Evidence

Raw outputs are in [docs/evidence/](evidence/). All runs below were executed with Apache Airflow 2.10.5
(LocalExecutor, Python 3.11) against PostgreSQL 16, using the DAG and code in this repository.

## Goal 1: Environment and configuration
- Python: 3.11 (`python:3.11-slim` image; Airflow image `apache/airflow:2.10.5-python3.11`)
- Configuration is external: database settings in `.env` (template: `.env.example`), pipeline paths and quality rules in `config/settings.yml`. No credentials are hard-coded.
- `.gitignore` excludes `.env`, virtual environments, generated `data/` layers and Airflow logs.
- Setup from a clean checkout: see README section 1 (`cp .env.example .env`, `docker compose up -d postgres`, `python -m src.cli validate-env`).
- Screenshots of `git log`, `docker compose ps` and `validate-env`: see "Screenshots" at the end.

## Goal 2: Pipeline row counts and rerun safety

| Layer | Customers | Products | Orders |
|---|---|---|---|
| Raw (physical rows) | 3,003 | 601 | 50,005 |
| Staging (deduplicated, valid) | 3,000 | 599 | 49,998 |
| Curated (`sales_order_lines`) | - | - | **49,897** |
| Quarantined | 0 | 1 | 103 (104 records total) |

Quarantine reasons: 1 product with negative price, 99 orders of that product, 1 invalid quantity,
1 invalid status, 1 orphan customer, 1 orphan product.

| Load | Input rows | Inserted | Updated | Unchanged |
|---|---|---|---|---|
| Partition load 2026-01 (`evidence_01_partition_2026_01b`) | 2,506 | 2,506 | 0 | 0 |
| First full load (`evidence_02_full_success`) | 49,897 | 47,391 | 0 | 2,506 (already loaded by the partition run) |
| **Rerun with the same data** (`evidence_03_full_rerun_idempotent`) | 49,897 | **0** | **0** | **49,897** |

After all runs: `SELECT count(*), count(DISTINCT order_id) FROM curated.sales_order_lines` → **49,897 / 49,897** (no duplicates).

## Goal 3: Storage and partitions
- Benchmark table: [benchmark_results.csv](benchmark_results.csv) and [partition_benchmark_results.csv](partition_benchmark_results.csv) (5 repetitions, median).
- Partition selected: `order_year=2026/order_month=01`
- Partition row count: 2,506
- PostgreSQL verification query:
  ```sql
  SELECT count(*) FROM curated.sales_order_lines
  WHERE order_timestamp >= '2026-01-01T00:00:00Z' AND order_timestamp < '2026-02-01T00:00:00Z';  -- 2506
  SELECT * FROM audit.partition_loads;  -- order_year=2026/order_month=01 | 2506 | evidence_01_partition_2026_01b
  ```

## Goal 4: Airflow
- DAG ID: `dss150p_sales_pipeline`
- Schedule: `0 2 * * *` (daily 02:00 UTC), `catchup=False`, `max_active_runs=1`
- Retries: 2, retry delay 30 s, execution timeout 10 min, retry and failure callbacks

| # | Run ID | Config | Result | What it shows |
|---|---|---|---|---|
| 0 | `scheduled__2026-10-09T02:00:00+00:00` | defaults | success | Scheduled run picked up after unpausing |
| 1 | `evidence_01_partition_2026_01b` | `{"run_mode":"partition","year":2026,"month":1}` | success | Parameters change the commands: `load-partition` + partition validation |
| 2 | `evidence_02_full_success` | `{"run_mode":"full"}` | **success** | Full extract → transform → load → validate |
| 3 | `evidence_03_full_rerun_idempotent` | `{"run_mode":"full"}` | success | Rerun: 0 rows inserted/updated |
| 4 | `evidence_04_retry_transform_once` | `{"fail_task":"transform","failure_mode":"once"}` | **success after retry** | `transform` try 1 → UP_FOR_RETRY, try 2 → SUCCESS |
| 5 | `evidence_05_failed_load_always` | `{"fail_task":"load","failure_mode":"always"}` | **failed** | `load` failed 3 times (1 + 2 retries), `validate` upstream_failed, run marked FAILED in `audit.pipeline_runs` |
| 6 | `evidence_06_recovery` | `{"run_mode":"full"}` | **success (recovered)** | Same pipeline re-run after removing the injected failure; data intact (0 changes) |

- Successful run ID: `evidence_02_full_success`
- Deliberate failure run ID: `evidence_05_failed_load_always`
- Retry/failure-handling evidence: `evidence_04_retry_transform_once` (retried and succeeded) and the retry/failure callback records in [evidence/05_airflow_failure_retry_events.jsonl](evidence/05_airflow_failure_retry_events.jsonl)
- Final recovery run ID: `evidence_06_recovery`

Evidence files:
- [01_airflow_dag_runs.txt](evidence/01_airflow_dag_runs.txt): all DAG runs and their final state
- [02](evidence/02_task_states_retry_run.txt), [03](evidence/03_task_states_failed_run.txt), [04](evidence/04_task_states_recovery_run.txt): task states of the retried, failed and recovered runs
- [06_failure_retry_log_excerpts.txt](evidence/06_failure_retry_log_excerpts.txt): task logs per attempt (UP_FOR_RETRY / SUCCESS / FAILED)
- [07_successful_run_log_excerpts.txt](evidence/07_successful_run_log_excerpts.txt): row counts printed by each step
- [08_postgres_audit_and_verification.txt](evidence/08_postgres_audit_and_verification.txt): `audit.pipeline_runs`, `audit.partition_loads`, duplicate check
- [09_pytest_output.txt](evidence/09_pytest_output.txt): 17 tests passed

## Screenshots
Screenshots from running the project with Docker Compose (Airflow UI grid view of runs 2, 4, 5 and 6,
the task log of a retried attempt, `docker compose ps`, and `git log`) are in [screenshots/](screenshots/).
