Spark Assignment - Architecture, Lazy Evaluation, File Formats, Pipelines
Dataset: Sample - Superstore.csv (9,994 rows)

Requires: pyspark, Java (needed by Spark under the hood)
Install pyspark if you don't have it: pip install pyspark

How to run:

1. Make sure "Sample - Superstore.csv" is in the same folder as
   spark_pipeline.py.

2. python3 spark_pipeline.py
   -> runs through every step in order and prints its output:
        - architecture notes
        - schema + row count
        - lazy evaluation demo (.explain() before/after an action)
        - filter + select
        - rename/cast/add columns
        - transformations vs actions demo
        - wide transformation / shuffle demo (groupBy .explain())
        - CSV vs Parquet write/read timing and file size comparison
        - predicate pushdown demo on the Parquet file
        - null handling
        - the full read -> transform -> filter -> write pipeline

   Writes to output/:
     output/superstore_csv/            - same data re-saved as CSV
     output/superstore.parquet/       - same data saved as Parquet
     output/pipeline_result.parquet/ - final pipeline output

3. insights.md
   -> write-up of the actual results (shuffle plan, CSV vs Parquet
      timing/size, predicate pushdown, etc.)

Files:
  spark_pipeline.py         - the full script, one function per step
  Sample - Superstore.csv - the dataset
  insights.md                 - insights on performance and architecture
  output/                        - written files (created when
                                  spark_pipeline.py runs)
