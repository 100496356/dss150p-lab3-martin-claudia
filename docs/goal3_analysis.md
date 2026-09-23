# Goal 3 — Storage Analysis Questions

**1. Which file format was smallest on your machine, and what encoding/compression characteristics help explain the result?**

Parquet, by a wide margin (5.5 MB vs 15.2 MB for CSV and 30.7 MB for JSON Lines). This is explained by Parquet's columnar storage: values from the same column are stored contiguously, so compression can exploit repeated patterns within a column (e.g. the `status` column has only 6 distinct values repeated 49,996 times) far more effectively than row-oriented formats that interleave every column's values together.

**2. Which representation was fastest for a full dataset read? Does that imply it is best for every workload?**

Parquet again (0.045s vs 0.22s for CSV and 0.54s for JSON Lines) — the same columnar layout that saves space also speeds up reads, since pandas can read contiguous memory blocks instead of parsing text row by row. This does NOT mean Parquet is best for every workload: it is optimized for large-batch analytical reads, not for appending one row at a time in real time (e.g. a continuously-arriving event log), where row-oriented formats like CSV or JSON Lines that can simply be appended to are operationally simpler, even though they are larger and slower to read.

**3. How did filtered retrieval differ between Parquet and PostgreSQL? What additional PostgreSQL design (such as an index) could change the result?**

In Parquet, filtered read (0.048s) took essentially the same time as full read (0.045s), because pandas must still read the entire file into memory before it can filter — there is no way to skip rows without reading them first. In PostgreSQL, the filtered query (0.052s) was about 6x faster than the full query (0.34s) — but this speedup currently comes from transferring less data over the wire (roughly 1/6 of rows, given 6 roughly evenly-distributed status values), not from an index, since none exists yet on `status`. Adding `CREATE INDEX ON curated.sales_order_lines(status)` would let PostgreSQL use an index scan instead of a sequential scan, reducing the actual server-side search cost as well — this is the real structural advantage a database has over a flat file: it can build access structures a file format cannot.

**4. Why is JSON Lines generally more pipeline-friendly than one giant JSON array for append/stream-oriented processing?**

A single large JSON array (`[{...}, {...}, {...}]`) is one indivisible syntactic structure: appending a new record technically requires reopening the file, removing the closing `]`, adding a comma and the new object, and re-closing the array. Reading it often requires loading the whole file before any single record can be parsed, since the parser cannot know an object is complete until it sees the following comma or the closing bracket. JSON Lines is one complete JSON object per line: appending a new record is a plain file append with no need to touch anything already written, and reading can process one line at a time without loading the entire file into memory — exactly the profile needed for streaming event logs or a continuously-growing pipeline source.

**5. What happens if a partition key has extremely high cardinality or poor query locality?**

Partitioning by something with very many distinct values (e.g. `customer_id` with 3,000 distinct customers instead of year/month) produces thousands of tiny partitions. This is counterproductive for two reasons: (1) metadata/listing overhead — with too many small partitions, the time spent simply listing and opening all the partition files/directories can exceed the time saved by filtering; (2) poor locality — if typical queries do not align with the partition key (e.g. queries usually filter by date, but the data is partitioned by customer), the query engine still has to open nearly every partition to find matching rows, losing the entire benefit partitioning is meant to provide. `order_year`/`order_month` was chosen here because it has reasonable cardinality (about 20 combined values across this dataset's date range) and matches the typical query pattern for a sales pipeline (analysis by time period).
