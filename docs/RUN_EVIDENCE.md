# Run Evidence

Two sets of runs are documented, both with the DAG and code in this repository (Apache Airflow 2.10.5,
LocalExecutor, Python 3.11, PostgreSQL 16):

1. **Docker Compose runs on my machine**: the setup described in the README, shown in the screenshots in [screenshots/](screenshots/).
2. **Reference runs** (`evidence_*` run IDs), executed in a separate Linux development environment while building the
   project, used to export the detailed logs, row counts and database queries in [evidence/](evidence/).
   The benchmark numbers, partition load and duplicate checks reported above are from my own machine.

## Goal 1: Environment and configuration
- Python: 3.11 (`python:3.11-slim` image; Airflow image `apache/airflow:2.10.5-python3.11`)
- Configuration is external: database settings in `.env` (template: `.env.example`), pipeline paths and quality rules in `config/settings.yml`. No credentials are hard-coded.
- `.gitignore` excludes `.env`, virtual environments, generated `data/` layers and Airflow logs.
- Setup from a clean checkout: see README section 1 (`cp .env.example .env`, `docker compose up -d postgres`, `python -m src.cli validate-env`).
- Screenshots of `docker compose ps`, `validate-env` and `git log`: see [screenshots/](screenshots/) (listed in Goal 4).

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

### Measurement context
Benchmark numbers depend on the hardware, so they are only valid together with this context.
- Machine / OS: my laptop, Windows, Docker Desktop. Project folder inside OneDrive, mounted into the container.
- CPU / RAM: Intel Core i7-1165G7 (11th Gen, 2.80 GHz), 16 GB RAM
- Command: `docker compose run --rm pipeline python -m src.cli benchmark --repeats 5` (Python 3.11 container)
- Repetitions: 5, median reported.
- Run before the benchmark: `run-all` with run id `run_20261011T033038Z_7a3f000e` → load `inserted 0, updated 0, unchanged 49897` (rerun-safe: data already loaded), validate OK `curated=49897, postgres=49897`.

### Partition tree
Output of `Get-ChildItem -Recurse -Directory data\partitioned\sales_order_lines -Name` on my machine
(21 month partitions, 2025-01 to 2026-09):
```
order_year=2025\order_month=1 ... order_month=12   (12 partitions)
order_year=2026\order_month=1 ... order_month=9    (9 partitions)
```
<details><summary>Full listing</summary>

```
order_year=2025\order_month=1
order_year=2025\order_month=2
order_year=2025\order_month=3
order_year=2025\order_month=4
order_year=2025\order_month=5
order_year=2025\order_month=6
order_year=2025\order_month=7
order_year=2025\order_month=8
order_year=2025\order_month=9
order_year=2025\order_month=10
order_year=2025\order_month=11
order_year=2025\order_month=12
order_year=2026\order_month=1
order_year=2026\order_month=2
order_year=2026\order_month=3
order_year=2026\order_month=4
order_year=2026\order_month=5
order_year=2026\order_month=6
order_year=2026\order_month=7
order_year=2026\order_month=8
order_year=2026\order_month=9
```
</details>

### Duplicate check (my machine, after loading the same data twice)
```
SELECT count(*), count(DISTINCT order_id) FROM curated.sales_order_lines;
 count | count
-------+-------
 49897 | 49897
```

## Goal 4: Airflow
- DAG ID: `dss150p_sales_pipeline`
- Schedule: `0 2 * * *` (daily 02:00 UTC), `catchup=False`, `max_active_runs=1`
- Retries: 2, retry delay 30 s, execution timeout 10 min, retry and failure callbacks

### Docker Compose runs (screenshots)

| # | Run ID | Config | Result | What it shows |
|---|---|---|---|---|
| 0 | `scheduled__2026-10-09T02:00:00+00:00`, `scheduled__2026-10-10T02:00:00+00:00` | defaults | success | Scheduled runs picked up after unpausing the DAG |
| 1 | `manual__2026-10-10T17:25:22+00:00` | `{"run_mode":"full"}` | **success** | Full extract → transform → load → validate |
| 2 | `manual__2026-10-10T17:41:20+00:00` | `{"fail_task":"transform","failure_mode":"once"}` | **success after retry** | `transform` try 1 → UP_FOR_RETRY, try 2 → SUCCESS |
| 3 | `manual__2026-10-11T02:47:09+00:00` | `{"fail_task":"load","failure_mode":"always"}` | **failed** | `load` failed on every try (1 + 2 retries), `validate` upstream_failed |
| 4 | `manual__2026-10-11T02:49:17+00:00` | `{"run_mode":"full"}` | **success (recovered)** | Same pipeline re-run without the injected failure |
| 5 | 2 runs, e.g. `manual__2026-10-11T03:44:35+00:00` | `{"run_mode":"partition","year":2026,"month":1}` | **failed (real error)** | `transform` failed all 3 tries with `PermissionError` (see below) |
| 6 | first partition run after the fix | `{"run_mode":"partition","year":2026,"month":1}` | **success** | `load-partition` loaded only 2026-01 (2,506 rows) |
| 7 | `manual__2026-10-11T03:49:01+00:00` | `{"run_mode":"partition","year":2026,"month":1}` | **success (rerun)** | Same partition loaded again: no duplicates |

- Successful run ID: `manual__2026-10-10T17:25:22+00:00`
- Deliberate failure run ID: `manual__2026-10-11T02:47:09+00:00`
- Retry/failure-handling evidence: `manual__2026-10-10T17:41:20+00:00` (screenshot `05_retry_log.png`: try 1 failed with the injected error, try 2 succeeded)
- Final recovery run ID: `manual__2026-10-11T02:49:17+00:00`
- Partition run ID: `manual__2026-10-11T03:49:01+00:00` (second load of the same partition)

**Partition load result on my machine** (after loading 2026-01 twice):
```
SELECT * FROM audit.partition_loads;
         partition_key          |         loaded_at_utc         | row_count |          pipeline_run_id
--------------------------------+-------------------------------+-----------+-----------------------------------
 order_year=2026/order_month=01 | 2026-10-11 03:49:50.864132+00 |      2506 | manual__2026-10-11T03_49_01_00_00

SELECT count(*), count(DISTINCT order_id) FROM curated.sales_order_lines;   -- 49897 | 49897
```
The audit row is updated (not duplicated) on every load of the same partition, and the table still has no duplicate orders.

**Real failure found and fixed.** The first two partition runs failed in `transform` with
`PermissionError: [Errno 13] Permission denied: '...-0.parquet'` inside `shutil.rmtree(data/partitioned/...)`.
I had just run `run-all` and `benchmark` with `docker compose run pipeline`, so the partition files belonged to the
container user of that service, and the Airflow worker (a different user) was not allowed to delete them on the
Windows bind mount. Airflow retried the task twice and then marked it failed, and the failure callback logged the
event with the params. Fix: delete `data/partitioned` (`Remove-Item -Recurse -Force data\partitioned`) and
trigger the DAG again. Since every step is rerun-safe, no database cleanup was needed. In a production setup, the
pipeline container and Airflow should run with the same user id (or write to a shared volume) to avoid this.

Screenshots:
- `01_docker_compose_ps.png`: PostgreSQL container healthy
- `02_validate_env.png`: pipeline container connected to PostgreSQL, tables listed
- `04_airflow_grid.png`: grid view with the successful, retried, failed and recovered runs
- `05_retry_log.png`: log of the retried `transform` task (try 1 red, try 2 green)
- `06_git_log.png`: commit history
- `10_airflow_partition_runs.png`: grid view with the failed partition runs (real permission error) and the two successful partition runs

### Reference runs (detailed log exports)

The same experiment, plus the partition run and the idempotent rerun, with the full outputs exported to [evidence/](evidence/):

| # | Run ID | Config | Result |
|---|---|---|---|
| 1 | `evidence_01_partition_2026_01b` | `{"run_mode":"partition","year":2026,"month":1}` | success (`load-partition` + partition validation) |
| 2 | `evidence_02_full_success` | `{"run_mode":"full"}` | success |
| 3 | `evidence_03_full_rerun_idempotent` | `{"run_mode":"full"}` | success, 0 rows inserted/updated |
| 4 | `evidence_04_retry_transform_once` | `{"fail_task":"transform","failure_mode":"once"}` | success after retry |
| 5 | `evidence_05_failed_load_always` | `{"fail_task":"load","failure_mode":"always"}` | failed, run marked FAILED in `audit.pipeline_runs` |
| 6 | `evidence_06_recovery` | `{"run_mode":"full"}` | success (recovered) |

Evidence files:
- [01_airflow_dag_runs.txt](evidence/01_airflow_dag_runs.txt): all DAG runs and their final state
- [02](evidence/02_task_states_retry_run.txt), [03](evidence/03_task_states_failed_run.txt), [04](evidence/04_task_states_recovery_run.txt): task states of the retried, failed and recovered runs
- [06_failure_retry_log_excerpts.txt](evidence/06_failure_retry_log_excerpts.txt): task logs per attempt (UP_FOR_RETRY / SUCCESS / FAILED)
- [07_successful_run_log_excerpts.txt](evidence/07_successful_run_log_excerpts.txt): row counts printed by each step
- [08_postgres_audit_and_verification.txt](evidence/08_postgres_audit_and_verification.txt): `audit.pipeline_runs`, `audit.partition_loads`, duplicate check
- [09_pytest_output.txt](evidence/09_pytest_output.txt): 17 tests passed
