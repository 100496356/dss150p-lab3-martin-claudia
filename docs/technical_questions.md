# Section 15 — Technical Questions

**1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?**

It lets the UPSERT decide "did anything actually change?" by comparing one value instead of 20 columns. It's computed only from the business fields (order_id, customer_id, product_id, order_timestamp, quantity, unit_price, discount_pct, status) — deliberately excluding `pipeline_run_id` and `processed_at_utc`, since those change on every run regardless of whether the underlying data did. I checked this wasn't just theoretical: ran `build_curated` twice with different run_ids on the same source data and confirmed the hash came out identical both times.

**2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?**

Because staging and curated are both the result of decisions — cleaning rules, join logic, quarantine thresholds — and those decisions can turn out wrong. If the raw copy didn't exist, fixing a bad transformation rule would mean going back to the original source system, which might not even have that exact data anymore (source systems get overwritten, APIs don't keep history forever). Raw is the one layer that lets you reprocess from scratch without depending on the source still having what it had before.

**3. What is the difference between a data-quality rejection and a system exception?**

A data-quality rejection is about one row being wrong (negative price, quantity out of range) — the row goes to quarantine with a reason, the pipeline keeps going. A system exception is about the pipeline itself being unable to run (DB unreachable, disk full) — that has to stop everything, because continuing without knowing if writes are landing risks silently producing wrong or incomplete results. I actually hit a real system exception by accident (a permissions error when Airflow tried writing to a directory `docker compose run` had created as root) — that's a good real example of the second category: nothing to do with row-level data quality, the whole task just couldn't run.

**4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?**

Same data, different physical layout. Parquet stores column-by-column, CSV row-by-row. If a query only touches a few columns, Parquet only has to read those columns' data; CSV has to read and parse every row start to finish regardless. In my benchmark this showed up directly — Parquet's full read was ~5x faster than CSV's on the same 49,996 rows.

**5. Why might a DAG that contains all transformation logic directly be considered harder to maintain?**

Two reasons mainly. First, you can't test the transformation logic without spinning up Airflow — if the join logic lives in a BashOperator's inline Python, you can't just call the function from a unit test. Second, the same logic becomes Airflow-only — can't reuse it in a one-off script or a different orchestrator later. Keeping it in `src/` and having the DAG just call `python -m src.cli transform` means the actual logic is testable and reusable independent of however it happens to get triggered.

**6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.**

Retries assume rerunning a failed step is safe. That's only true if the step is idempotent. If `load` used a plain `INSERT` instead of an `UPSERT ... ON CONFLICT`, and a retry happened because the *response* to a successful insert got lost on the network (not because the insert itself failed) — Airflow would see it as failed and retry, and the retry would insert the same rows again, creating duplicates. That's exactly why `order_id` is the ON CONFLICT key here — a retry after an already-successful write just re-applies the same values and changes nothing, instead of duplicating.

**7. What trade-off is introduced by partitioning too aggressively?**

Too many small partitions means the overhead of listing and opening all those separate files/folders can end up costing more than what filtering was supposed to save. And if the partition key doesn't match how queries actually filter (e.g. partitioning by customer_id but querying by date), the engine still ends up touching nearly every partition anyway — so you pay the overhead without getting the benefit.

**8. How would you adapt the pipeline if the source became an API or database instead of local files?**

The extract stage is the only one that would need to change — `extract_sources()` currently copies files; it'd instead need to page through an API (like the pagination pattern from Lab 2's `local_api_server.py`) or run a SELECT against the source DB, most likely filtered by an incremental watermark instead of pulling everything each time. Staging, curated, load, and validate wouldn't need to change at all, since they only care about the shape of the raw data once it lands, not where it came from — that separation is the whole reason extract is its own module instead of being mixed into the rest of the pipeline.
