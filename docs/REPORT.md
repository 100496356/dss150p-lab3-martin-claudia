# DSS150P Laboratory 3: Technical Report

**Student:** Claudia González Martín

This report covers the explanations, analysis questions and reflection required by the lab. Commands and
setup are in the [README](../README.md), and run evidence is in [RUN_EVIDENCE.md](RUN_EVIDENCE.md).

---

## Goal 1: Reproducible environment

**Environment.** Python 3.11 (`python:3.11-slim` in Docker). The packages are pinned in `requirements.txt`:
pandas 2.2.3, pyarrow 17.0.0, psycopg 3.2.3, python-dotenv 1.0.1, PyYAML 6.0.2 and pytest. Airflow uses the image
`apache/airflow:2.10.5-python3.11` plus `requirements-airflow.txt`.

**Why the virtual environment is not committed.** A `.venv` folder contains thousands of files compiled for one
operating system and one Python path, so it would not work on another machine and would make the repository huge.
What makes the environment reproducible is `requirements.txt` with pinned versions: anyone can recreate the same
environment with `python -m venv .venv` and `pip install -r requirements.txt`. That is why `.venv/` is in `.gitignore`.

**Modules.** Each package under `src/` has one responsibility: `extract` only copies source snapshots, `transform`
contains the staging and curated rules, `load` only writes to PostgreSQL, `validate` only checks data, `benchmark`
only materializes formats and measures them, and `cli.py` only connects the steps (one command per step).

**How configuration is separated from code.**
- `config/settings.yml` holds non-secret settings: folder paths, allowed statuses, quantity range, benchmark repetitions and partition columns.
- `.env` holds environment-specific values and secrets (host, port, database, user, password). Only `.env.example` is committed; `.env` is ignored by Git.
- `src/config.py` is the only place that reads both and exposes `SETTINGS`, `DB` and `path_for()`. No module hard-codes a path, host or password: searching the repository for the training password only finds it in `.env.example`.
- The same code runs on the host and in Docker: on the host `POSTGRES_HOST=localhost`, and Docker Compose overrides it to `postgres` (the service name) for containers.

## Goal 2: Pipeline and error handling

**Layers.** Raw keeps an untouched copy of the sources per run (`data/raw/run_id=<id>/`). Staging types,
normalizes and deduplicates each source. Curated joins the three sources and computes the amounts. Quarantine
keeps every rejected record with its reason. Counts: 50,005 raw orders → 49,998 staging → 49,897 curated, with 104
records quarantined (details in RUN_EVIDENCE.md).

**Data-quality rejection vs system exception.**
- A bad record (negative price, quantity 0, unknown customer) is a *data-quality* problem: the pipeline keeps running and sends that record to quarantine with a reason.
- A missing file, a database that is down or a wrong path is a *system* failure: the step raises an exception with a clear message. The CLI logs the failed step and error to `data/runs/events.jsonl`, marks the run as `FAILED` in `audit.pipeline_runs` and exits with an error code, so Airflow can retry it.
- No `except: pass` is used anywhere.

## Goal 3: Storage, benchmark and partitioning

### Benchmark interpretation

The same 49,897 curated rows were written to CSV, JSON Lines, Parquet (snappy compression) and a PostgreSQL table.
Each read was repeated 5 times and the **median** is reported. The filtered read uses `status = 'DELIVERED'`.
Results depend on the machine (see "Measurement context" in RUN_EVIDENCE.md), so they show trade-offs on this
setup, not universal truths.

| Storage | Size | Write (s) | Full read (s) | Filtered read (s) |
|---|---|---|---|---|
| CSV | 15.3 MB | 1.409 | 0.282 | 0.295 |
| JSON Lines | 31.9 MB | 1.970 | 1.874 | 1.178 |
| Parquet | 5.5 MB | 0.181 | 0.118 | 0.052 |
| PostgreSQL | 14.7 MB (`pg_total_relation_size`) | 1.707 (COPY) | 0.526 | 0.076 |

- **CSV** is plain text with no types: every read parses every character and guesses the types again, and filtering
  still requires reading the whole file. That is why its full and filtered reads take almost the same time.
- **JSON Lines** repeats every column name in every row, so it is more than twice the size of CSV and the slowest to
  read. Its advantage is flexibility: each line is an independent record.
- **Parquet** stores each column separately, with its type, and compresses it. Repeated values like `status`,
  `category` or `brand` compress very well. It was the smallest and fastest, and the filtered read was more than twice as
  fast as the full read, because only the matching row groups are decoded.
- **PostgreSQL** is slower for a full read because all rows travel through the client–server protocol and are
  converted into Python objects. However, it is the only option with concurrent access, transactions, constraints
  and UPSERT, so it is the right serving layer, while Parquet is better as an analytical file format.

### Partitioning

`transform` writes `data/partitioned/sales_order_lines/order_year=YYYY/order_month=M/`, a hive-style folder per
month (listing in RUN_EVIDENCE.md). Reading one month (2025-05, 2,540 rows) from the partitioned dataset read only 346 KB, against 5.5 MB when
filtering the single Parquet file, so partitioning reduced the data read by about 16×. This is *partition pruning*:
the reader only opens the folders that match the filter and skips the other files completely.

However, on my machine the pruned read was **slower** (0.529 s vs 0.093 s). I think the reason is that the whole
dataset is only 5.5 MB, and the project folder is a Windows folder mounted into the Docker container. Every
directory listing and file open crosses the Windows–Linux file sharing layer, and discovering the hive dataset
means listing all 21 partition folders. At this size that fixed per-file overhead is bigger than the time saved by
reading less data. With gigabytes of data, or data in cloud storage where every byte costs time and money, reading
16× less data would clearly win. The lesson is that partitioning is a trade-off: it saves I/O but adds metadata
and file overhead, so it only pays off when partitions are large enough.

### Goal 3 analysis questions

1. **Smallest format.** Parquet (5.5 MB vs 15.3 MB CSV and 31.9 MB JSONL). It is columnar, so similar values are
   stored together, and it uses dictionary encoding (for example, `status` has only 6 values) plus snappy compression.
   It also stores numbers and dates in binary instead of as text.
2. **Fastest full read.** Parquet. That does not make it best for every workload. It is not good for frequent
   single-row updates or many concurrent writers, and a person cannot open it in a text editor. For those cases a
   database or CSV is more practical.
3. **Filtered retrieval, Parquet vs PostgreSQL.** Parquet was faster here (0.052 s vs 0.076 s), but PostgreSQL had to
   scan the whole table because there is no index on `status`. Adding `CREATE INDEX ON curated.sales_order_lines
   (status)` (or a partial index for `DELIVERED`) would help more selective filters. With about 50% of rows matching,
   a sequential scan can still win, so an index matters most for selective queries.
4. **JSON Lines vs one big JSON array.** In JSON Lines every line is a complete record. New records can be appended
   to the end of the file, and a reader can process it line by line in a stream without loading everything into
   memory. A single JSON array must be read completely to be valid, and appending requires rewriting the closing
   bracket.
5. **Very high-cardinality partition key.** Partitioning by something like `order_id` or a timestamp would create
   thousands of tiny folders and files. Every query would then spend more time opening files and reading metadata
   than reading data, and the file system and query planner would slow down. If queries do not filter on the
   partition key (poor locality), pruning never helps and all partitions are read anyway.

## Goal 4: Orchestration

- **Schedule `0 2 * * *`.** The sources are daily exports, so one run per day is enough. 02:00 UTC runs after the
  day's data is complete and before analysts start working.
- **`catchup=False`.** Each run reprocesses the full current snapshot of the sources, so running all the missed days
  since `start_date` would repeat the same work many times. Only the latest run is needed.
- **`max_active_runs=1`.** Two runs cannot load at the same time.
- **Retries and timeout.** `retries=2` with `retry_delay=30s` absorbs temporary errors (for example, the database
  restarting), and `execution_timeout=10min` stops a task that hangs.
- **Failure handling.** `on_retry_callback` and `on_failure_callback` print and save the DAG, run, task, try number,
  parameters and error to `data/runs/airflow_events.jsonl`. The final failure also marks the run as `FAILED` in
  `audit.pipeline_runs`.
- **Business logic separation.** Every task is a `BashOperator` that calls `python -m src.cli <command>`. The DAG
  contains no transformation logic, only which command to run with which parameters.
- **Run identity.** Every task receives `PIPELINE_RUN_ID="{{ run_id }}"`, so the raw folder, staging, curated,
  quarantine and audit rows of one DAG run all share the same id.
- **Controlled failure.** I used the DAG parameters `fail_task` and `failure_mode` instead of renaming
  `orders.csv`. This produces the same kind of failure without touching the source data, and it is repeatable:
  `once` shows a retry that recovers, `always` shows a task that fails all its tries.

**A real failure.** Apart from the injected failures, I hit a real one: the first partition runs failed in
`transform` with a `PermissionError`, because files created earlier by the `pipeline` container could not be deleted
by the Airflow user. Airflow retried twice, marked the task failed and logged it through the callback. After
deleting the folder I just triggered the DAG again, without any database cleanup (details in RUN_EVIDENCE.md).

**Which steps are safe to rerun, and why.**
- `extract` copies the sources into the folder of that run id, overwriting it, so a rerun produces the same snapshot.
- `transform` only reads raw and rewrites its own outputs for that run id, so it can be repeated.
- `load` is an UPSERT on `order_id` that only updates rows whose `record_hash` changed. Running it again inserts 0
  rows and never creates duplicates, and `load-partition` updates the same `audit.partition_loads` key.
- `validate` only reads.

So any failed task can be cleared and rerun, or a new run triggered, without manual database cleanup.

**Backfill (optional).** To rebuild a historical month I would trigger the DAG with `run_mode=partition` and that
year/month, or use `airflow dags backfill -s <start> -e <end>`, where Airflow's *data interval* tells each run which
period it covers. Because the load is an idempotent UPSERT keyed on `order_id`, rerunning a period that was already
loaded updates rows instead of duplicating them. Running the same period twice is therefore safe, and
`max_active_runs=1` avoids two backfill runs writing at the same time.

## Technical questions

1. **Why `record_hash` helps, and what it should exclude.** It summarizes the business content of a row. During the
   UPSERT, a row is only updated when its hash changes, so an unchanged rerun does no writes and it is easy to tell
   which rows really changed. It must exclude volatile audit columns such as `pipeline_run_id` and
   `processed_at_utc`; otherwise every run would produce a different hash and update every row.
2. **Why preserve raw data.** If a transformation rule turns out to be wrong, or a new rule is needed, the curated
   data can be rebuilt from exactly what was received, without asking the source again. It also allows auditing
   and debugging: you can always prove what the source sent on a given run.
3. **Data-quality rejection vs system exception.** A rejection is about one bad record: it goes to quarantine with a
   reason and the pipeline continues. A system exception is about the pipeline itself not being able to work (file
   missing, database down): it stops the step, is logged and is retried. Mixing them would either hide
   infrastructure problems or stop the whole pipeline because of one bad row.
4. **Why Parquet can beat CSV with the same rows.** Parquet reads only the columns a query needs, keeps types so
   nothing has to be parsed, compresses data so less is read from disk, and can skip row groups using min/max
   statistics. CSV always has to read and parse every character of every row.
5. **Why putting all logic in the DAG is harder to maintain.** The logic could then only run inside Airflow, so it
   cannot be tested with pytest or run by hand, scheduling and business code get mixed, and the scheduler has to
   parse heavier files. Keeping logic in `src/` lets the same code run from the CLI, the tests, Docker and Airflow.
6. **Retries and idempotency.** A retry runs the same task again, so the task must give the same result if it runs
   twice. With a plain `INSERT` load that failed halfway, the retry would insert again the rows that were already
   written, and the table would end up with duplicate orders and double-counted sales. An UPSERT on `order_id` makes
   the retry safe.
7. **Partitioning too aggressively.** It creates too many small files and folders: more metadata to read, slower
   listing and planning, worse compression per file and more overhead than data. Queries that do not filter on the
   partition key get slower, because they have to open every small file.
8. **Source as an API or database.** Only the `extract` module would change. It would call the API, handling
   pagination, authentication, rate limits and retries, or query the database incrementally using `updated_at`. It
   would then write the result into the same `data/raw/run_id=<id>/` folder. Staging, curated, load and the DAG would
   stay the same, because they only depend on the raw layer. Credentials would go in `.env`.

## Technical reflection

**Modularity.** Separating extract, transform, load and validate made each part testable on its own and let the
same code run from the CLI, Docker and Airflow. When something failed, the error always pointed to one step.

**Idempotency.** Run-specific raw folders and the hash-based UPSERT mean any step can be repeated safely. This is
what made the retry and recovery experiments in Airflow work without cleaning the database by hand.

**Storage trade-offs.** No format won everything. Parquet is best for analytical reading and storage, PostgreSQL for
serving data with updates and concurrent access, CSV for simple exchange, and JSON Lines for streaming and flexible
records.

**Orchestration vs business logic.** Airflow decides *when* and *in what order* things run, how many times to retry
and what to do on failure. The pipeline code decides *what* happens to the data. Keeping them apart made the DAG
short and easy to read.

## Use of AI tools

In line with section 16 of the lab guide, I acknowledge that I used an AI assistant (Claude, by Anthropic) as a
support tool while completing this laboratory. It helped with implementing and debugging parts of the code and
with drafting and refining the written documentation. I ran the pipeline, Docker and Airflow on my own machine,
reviewed the results and screenshots, and I am responsible for every part of the submission.
