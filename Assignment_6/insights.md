Insights - Spark Architecture, Performance, and File Formats

Architecture
- Running with .master("local[*]") puts the driver and all executors on
  this one machine as threads - fine for development, but on a real
  cluster the driver plans the work and executors (spread across many
  machines) actually run it.

Lazy evaluation
- Chaining .filter().select() didn't do any work by itself - .explain()
  showed the plan Spark built, then .count() (an action) is what actually
  triggered the read and filter. Confirmed the same pattern on the
  "orders sold at a loss" filter: 1,871 out of 9,994 orders (about 19%)
  have negative profit.

Transformations vs actions / wide transformations
- The groupBy("Category").sum("Sales") plan showed an "Exchange
  hashpartitioning" step - that's the shuffle. A plain filter/select plan
  has no such step, confirming narrow transformations don't move data
  between partitions the way groupBy does.

CSV vs Parquet
- Same 9,994 rows, written and read both ways:
    CSV:     write 1.72s, read 1.52s, 2,264 KB
    Parquet: write 2.41s, read 0.67s,   444 KB
- Parquet took a bit longer to write (it has to organize data by column
  and add metadata), but read back more than 2x faster and took up about
  a fifth of the disk space. For a pipeline that writes once and reads
  many times, Parquet wins by a wide margin.

Predicate pushdown
- Filtering the Parquet file by Category showed "PushedFilters:
  [IsNotNull(Category), EqualTo(Category,Furniture)]" right in the
  FileScan step - the filter was applied while reading, not after. CSV
  showed a PushedFilters line too here since it's a simple equality
  filter, but CSV has no column-level structure to skip on the way
  Parquet's row groups do, so the practical benefit is much bigger with
  Parquet, especially on wider files.

Nulls
- This dataset came in clean (0 nulls across the key columns checked),
  so dropna() didn't remove anything here - but the pipeline handles it
  with Spark's built-in dropna rather than any row-by-row Python logic,
  so it stays distributed and would scale to a much larger, messier file.

Pipeline
- The full read -> transform -> filter -> write pipeline ran on all
  9,994 rows and wrote a Parquet file without ever calling collect() -
  only .show() was used to preview results, which only pulls back a
  handful of rows instead of the whole dataset.
