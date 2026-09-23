# Goal 3 — Storage Analysis Questions

**1. Which file format was smallest on your machine, and what encoding/compression characteristics help explain the result?**

Parquet, not even close — 5.5 MB vs 15.2 MB for CSV and 30.7 MB for JSON Lines. Makes sense once you think about how it stores data: columnar, so all the values of one column sit together instead of being scattered across rows. The `status` column only has 6 possible values repeated ~50,000 times, so compressing that column on its own works a lot better than compressing it mixed in with everything else, which is basically what CSV/JSON force you to do.

**2. Which representation was fastest for a full dataset read? Does that imply it is best for every workload?**

Parquet again — 0.045s vs 0.22s (CSV) and 0.54s (JSON Lines). Same reason as above really: reading contiguous columnar blocks beats parsing text line by line.

No, though, it doesn't mean Parquet wins everywhere. It's built for big batch reads, not for writing one row at a time as events come in. If I had a live log that needed a new line appended every few seconds, I'd rather use CSV or JSON Lines — worse on size/speed, but you can just append to them, which Parquet isn't really meant for.

**3. How did filtered retrieval differ between Parquet and PostgreSQL? What additional PostgreSQL design (such as an index) could change the result?**

This one actually surprised me a bit. In Parquet, filtering barely changed anything — 0.048s filtered vs 0.045s full read, basically the same. Makes sense in hindsight: pandas has to load the whole file before it can even check the `status` column, so there's no shortcut.

PostgreSQL was the opposite — the filtered query (0.052s) was around 6x faster than pulling everything (0.34s). I assumed at first this meant there was an index doing the work, but there isn't one on `status` yet — the speedup is really just from sending less data back over the wire (roughly 1/6 of the rows, since there are 6 status values spread fairly evenly). If I added `CREATE INDEX ON curated.sales_order_lines(status)`, the database wouldn't need to scan every row to find matches either, so the query itself would get faster too, not just the data transfer. That's really the advantage a database has over a flat file — it can build structures like indexes that a file format has no equivalent for.

**4. Why is JSON Lines generally more pipeline-friendly than one giant JSON array for append/stream-oriented processing?**

Because a single JSON array is one big structure — to add a record you'd have to reopen the file, delete the closing `]`, add a comma, insert the object, close it again. JSON Lines is just one JSON object per line, so adding a record is a plain append, nothing else needs to change. Same story for reading: you can go line by line instead of loading the whole file first. That's exactly the shape you want for something like a growing event log.

**5. What happens if a partition key has extremely high cardinality or poor query locality?**

You end up with a ton of tiny partitions. Two problems with that: first, just listing/opening all those little files becomes its own overhead — could easily cost more than what you saved by filtering in the first place. Second, if your queries don't actually match the partition key (say you partitioned by `customer_id` but usually query by date), the engine still has to check almost every partition anyway, so you lose the whole point of partitioning.

`order_year`/`order_month` worked fine here — around 20 combined values across the dataset's date range, and it lines up with how a sales dataset actually gets queried (by time period), so it's not just a "safe default," it fits the access pattern.

