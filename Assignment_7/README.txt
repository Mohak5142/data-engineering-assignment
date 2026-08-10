Spark Fundamentals - Data Cleaning, Transformation & Aggregation
Dataset: Sample - Superstore.csv (9,994 rows)

Requires: pyspark, Java (needed by Spark under the hood)
Install pyspark if you don't have it: pip install pyspark

How to run:

1. Make sure "Sample - Superstore.csv" is in the same folder as
   spark_pipeline.py (it's saved as UTF-8 here - the original download is
   often Latin-1/ISO-8859-1, so if you swap in a fresh copy of the file
   and see garbled characters, re-save it as UTF-8 first).

2. python3 spark_pipeline.py
   -> starts a Spark session, loads the CSV, prints the first few rows /
      schema, cleans the data, runs the filtering + aggregation +
      groupBy examples, prints an explanation of wide vs narrow
      transformations, and writes:
        output/cleaned_superstore/   - the cleaned dataset
        output/category_summary/    - profit/sales grouped by category

3. insights.md
   -> short writeup of what the results actually showed

Files:
  spark_pipeline.py         - the full Spark pipeline (load, clean, filter,
                                  aggregate, group, transformation notes)
  Sample - Superstore.csv - the dataset
  insights.md                 - brief insights on the results
  output/                        - cleaned data + category summary (created
                                  when spark_pipeline.py runs)

Note on this dataset: it came in already clean (no duplicates or nulls),
so the cleaning step doesn't remove much - but it's still written to
handle duplicates/missing values defensively, and it did catch one real
formatting quirk (a product name with an embedded escaped quote mark that
needed explicit CSV parser options to read correctly - see the comment in
load_data()).
