# Run Evidence

## Week 4 (Goal 1 — Reproducible Environment)
- Python version: 3.12.3 (sandbox) / 3.11 (project's Docker image, per Dockerfile)
- Git status/log evidence: branch `goal1-reproducible-environment`, commit "feat: add reproducible pipeline environment"
- Docker image/container evidence: `dss150p-postgres` (Up, healthy), `dss150p-pipeline` built successfully; `docker compose run --rm pipeline python -m src.cli validate-env` prints `DB host/database= postgres dss150p` inside the container vs `localhost dss150p` on host — confirms config correctly overridden per environment
- External configuration evidence: `.env.example` template created (was missing from starter repo); `.env` confirmed untracked via `git check-ignore .env`; `config/settings.yml` holds non-secret defaults, `src/config.py` is the sole module combining both sources

## Week 5 (Goal 2 — ETL Pipeline)
- Raw row counts: customers.csv 3,003 rows / products.json 601 rows / orders.csv 50,005 rows (physical, pre-cleaning)
- Staging row counts: customers 3,000 (3 duplicate versions resolved) / products 599 valid + 1 quarantined (negative price) / orders 49,998 (2 invalid rows quarantined: 1 bad quantity, 1 bad status)
- Curated row counts: 49,996 (2 additional rows quarantined at the join stage: 1 orphan customer reference, 1 orphan product reference — genuinely nonexistent IDs, not the price-quarantined product)
- Quarantine row counts: 5 total (1 invalid product price, 1 invalid order quantity, 1 invalid order status, 1 orphan customer reference, 1 orphan product reference)
- First load affected rows: 49,996 (`run-all`)
- Second rerun affected rows / evidence of idempotency: 49,996 again on two subsequent standalone `load` invocations; `COUNT(*) = COUNT(DISTINCT order_id) = 49996` confirmed after each; PostgreSQL `xmin` (row version) unchanged between first and second load, confirming the UPSERT's `WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash` clause made the rerun a true no-op at the row level, not just duplicate-free

## Week 6 (Goal 3 — Storage Benchmark & Partitioning)
- Benchmark table attached: yes — `data/benchmarks/benchmark_results.csv`

| storage_type | file_size_bytes | write_seconds | full_read_seconds | filtered_read_seconds | row_count |
|---|---:|---:|---:|---:|---:|
| CSV | 15,256,740 | 0.81 | 0.22 | 0.22 | 49,996 |
| JSON Lines | 30,768,046 | 0.65 | 0.54 | 0.54 | 49,996 |
| Parquet | 5,502,182 | 0.09 | 0.045 | 0.048 | 49,996 |
| PostgreSQL | 16,613,376 (table+index) | N/A (already loaded via UPSERT) | 0.34 | 0.052 | 49,996 |

  (repeats=5, median reported; filter_status=DELIVERED; machine-dependent, not a universal claim)

- Partition selected: order_year=2026, order_month=1
- Partition row count: 2,513
- PostgreSQL verification query: `SELECT * FROM audit.partition_loads;` → one row, `partition_key='2026-01'`, `row_count=2513`; rerun of `load-partition` for the same partition produced the same row_count with no new duplicate rows in `curated.sales_order_lines` (confirmed via `COUNT(*) = COUNT(DISTINCT order_id)`)

## Week 7 (Goal 4 — Airflow Orchestration)
- DAG ID: `dss150p_sales_pipeline`
- Schedule: `0 2 * * *` (daily, 02:00 UTC)
- Parameters used: `run_mode=full` (first successful run), then `run_mode=partition, year=2026, month=1` (second successful run)
- Successful run ID (full mode): `manual__2026-09-23T09:03:13+00:00` — all 4 tasks (`extract`, `transform`, `load`, `validate`) succeeded
- Successful run ID (partition mode): `manual__2026-09-23T09:12:17+00:00` — confirmed via `audit.partition_loads` showing `pipeline_run_id='manual__2026-09-23T09:12:17+00:00'`
- Deliberate failure run ID: `manual__2026-09-23T08:24:12+00:00` — `extract` task failed after 3 attempts with `PermissionError: [Errno 13] Permission denied` on `data/raw/run_id=...`, caused by the `data/` directory having been created with root ownership by an earlier `docker compose run` invocation, which the Airflow container's own user could not write into
- Retry/failure-handling evidence: task retried automatically per `retries=2` (3 total attempts observed in task logs, `attempt=1.log` through `attempt=3.log`); `on_failure_callback` printed `TASK FAILED: dag_id=... task_id=extract run_id=... try_number=3 exception=...` to the task log on final failure; downstream tasks (`transform`, `load`, `validate`) correctly marked `upstream_failed` rather than attempting to run
- Final recovery run ID: `manual__2026-09-23T09:03:13+00:00` — after running `chmod -R 777 data/` to fix the ownership/permission mismatch between the `pipeline` and Airflow containers, this run succeeded end-to-end with no code changes needed
