"""
Spark Assignment - Architecture, Lazy Evaluation, File Formats, Pipelines
Dataset: Sample - Superstore.csv

Covers:
  1. Spark architecture (Driver, Cluster Manager, Executors, execution modes)
  2. Lazy evaluation and the DAG / lineage graph
  3. Reading data with an explicit schema (CSV, then Parquet)
  4. Filtering and column selection
  5. Modifying DataFrames (rename, cast, add columns)
  6. Transformations vs actions
  7. Wide transformations, shuffle, predicate pushdown
  8. CSV vs Parquet performance comparison
  9. Handling nulls efficiently
  10. A full read -> transform -> filter -> write pipeline
  11. Best practices for large datasets (avoid collect(), use show())

Run: python3 spark_pipeline.py
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, upper
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

DATA_PATH = "Sample - Superstore.csv"


def get_spark():
    return (
        SparkSession.builder
        .appName("SparkArchitectureAssignment")
        .master("local[*]")
        .getOrCreate()
    )


def explain_architecture():
    """
    Step 1: Spark architecture.

    Driver - runs the main program, builds the DAG of transformations,
    and hands out tasks. It doesn't process data itself, it just plans
    and coordinates.

    Cluster Manager - decides which machines (executors) the job gets to
    run on. Can be Spark's own standalone manager, YARN, Kubernetes, etc.
    Running with .master("local[*]") like this script does means the
    driver and all executors are just threads on this one machine -
    useful for development, but a real cluster would have the driver on
    one node and executors spread across many.

    Executors - the worker processes that actually run the tasks
    (reading data, filtering rows, doing the aggregation) and report
    results/status back to the driver. Each executor runs multiple
    tasks in parallel across its CPU cores.

    Execution modes - client mode runs the driver on the machine that
    submitted the job (e.g. your laptop, a notebook); cluster mode runs
    the driver on the cluster itself. Client mode is more common for
    interactive work, cluster mode for scheduled production jobs.
    """
    print("\nSTEP 1: Spark architecture")
    print(explain_architecture.__doc__)


def explain_lazy_evaluation(df):
    """
    Step 2: lazy evaluation and the DAG.

    Transformations (filter, select, withColumn, groupBy...) don't run
    immediately - Spark just records them as a plan, building up a DAG
    (a graph of steps with no cycles) also called the lineage graph.
    Nothing actually executes until an action is called (count, show,
    collect, write...). This lets Spark's optimizer look at the *whole*
    chain of steps before running anything, so it can reorder or combine
    steps for efficiency instead of running each one naively as it's
    written.
    """
    print("\nSTEP 2: Lazy evaluation and the DAG")
    print(explain_lazy_evaluation.__doc__)

    # build a chain of transformations - nothing runs yet
    plan = df.filter(col("Category") == "Furniture").select("Category", "Sales")
    print("Transformations were just chained above - nothing has run yet.")
    print("Calling .explain() shows the plan Spark built without executing it:")
    plan.explain()

    print("Now calling .count(), an action, actually triggers execution:")
    result = plan.count()
    print(f"Furniture row count: {result}")


SUPERSTORE_SCHEMA = StructType([
    StructField("Row ID", IntegerType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Sales", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True),
])


def load_data(spark, path=DATA_PATH):
    """
    Step 3: read data with proper schema handling.

    An explicit schema is passed in instead of relying on inferSchema.
    inferSchema has to scan the file (or a sample of it) first just to
    guess types, which is extra work on top of the actual read - and on
    this particular file it guessed wrong for a couple of columns. A
    hand-written schema is a bit more typing but is faster and reliable.

    Postal Code is deliberately typed as a string, not a number - it's
    an identifier, not a quantity that should ever be summed or averaged.

    multiLine and an explicit escape character are needed here because
    one product name has an embedded escaped quote mark (14 7/8"" x 11"")
    that Spark's default CSV settings misread otherwise.
    """
    df = spark.read.csv(
        path, header=True, schema=SUPERSTORE_SCHEMA,
        multiLine=True, quote='"', escape='"'
    )
    print("\nSTEP 3: Load with explicit schema")
    print("Schema (data types):")
    df.printSchema()
    print(f"Row count: {df.count()}")
    return df


def select_and_filter(df):
    """Step 4: filtering and column selection."""
    print("\nSTEP 4: Filter and select")
    result = (
        df.select("Order ID", "Category", "Region", "Sales", "Profit")
        .filter((col("Category") == "Technology") & (col("Region") == "West"))
    )
    print(f"Technology orders in the West region: {result.count()}")
    result.show(5, truncate=False)
    return result


def modify_dataframe(df):
    """Step 5: modify DataFrames - rename columns, cast types, add columns."""
    print("\nSTEP 5: Modify the DataFrame")
    modified = (
        df.withColumnRenamed("Sub-Category", "sub_category")
        .withColumnRenamed("Order ID", "order_id")
        .withColumn("Postal Code", trim(col("Postal Code")))
        .withColumn("profit_margin", when(col("Sales") > 0, col("Profit") / col("Sales")).otherwise(None))
        .withColumn("category_upper", upper(col("Category")))
    )
    modified.select("order_id", "sub_category", "Sales", "Profit", "profit_margin", "category_upper").show(5, truncate=False)
    return modified


def demonstrate_transformations_vs_actions(df):
    """
    Step 6: transformations vs actions.

    Transformations (filter, select, withColumn, groupBy, join) build up
    the plan and return a new DataFrame - lazy, nothing runs.

    Actions (count, show, collect, write) actually trigger execution of
    everything queued up so far and return a real result.
    """
    print("\nSTEP 6: Transformations vs actions")
    print(demonstrate_transformations_vs_actions.__doc__)

    transformed = df.filter(col("Profit") < 0)  # transformation, still lazy
    print("Transformation (.filter) chained - nothing executed yet.")

    count = transformed.count()  # action, triggers execution
    print(f"Action (.count) triggered execution - found {count} orders sold at a loss.")


def demonstrate_wide_transformation_and_shuffle(df):
    """
    Step 7a: wide transformations and shuffle.

    filter/select/withColumn are narrow - each output partition only
    needs data from one input partition, no data movement required.

    groupBy is wide - rows with the same key can be scattered across
    different partitions, so Spark has to shuffle (redistribute data
    across the cluster) to bring matching rows together before it can
    aggregate. .explain() shows an "Exchange" step in the plan wherever
    a shuffle happens - that's the expensive part of a Spark job.
    """
    print("\nSTEP 7a: Wide transformation and shuffle")
    print(demonstrate_wide_transformation_and_shuffle.__doc__)

    grouped = df.groupBy("Category").sum("Sales")
    print("Physical plan for a groupBy (look for the 'Exchange' step - that's the shuffle):")
    grouped.explain()


def demonstrate_predicate_pushdown(spark):
    """
    Step 7b: predicate pushdown.

    When a filter is applied on a column-oriented format like Parquet,
    Spark can push the filter down into the file reader itself, so
    entire row groups that can't match get skipped without ever being
    fully read into memory. CSV is just plain text rows, so a CSV reader
    has no way to skip data early - it has to read every row and only
    then apply the filter. This is one of the reasons Parquet reads are
    often much faster than CSV reads once a filter is involved.
    """
    print("\nSTEP 7b: Predicate pushdown (Parquet vs CSV)")
    print(demonstrate_predicate_pushdown.__doc__)

    parquet_df = spark.read.parquet("output/superstore.parquet")
    filtered = parquet_df.filter(col("Category") == "Furniture")
    print("Physical plan for a filtered Parquet read (look for 'PushedFilters'):")
    filtered.explain()


def compare_csv_vs_parquet(spark, df):
    """Step 8: CSV vs Parquet - write and read timing plus file size."""
    print("\nSTEP 8: CSV vs Parquet performance comparison")

    start = time.time()
    df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/superstore_csv")
    csv_write_time = time.time() - start

    start = time.time()
    df.coalesce(1).write.mode("overwrite").parquet("output/superstore.parquet")
    parquet_write_time = time.time() - start

    start = time.time()
    csv_count = spark.read.csv("output/superstore_csv", header=True, inferSchema=True).count()
    csv_read_time = time.time() - start

    start = time.time()
    parquet_count = spark.read.parquet("output/superstore.parquet").count()
    parquet_read_time = time.time() - start

    import subprocess
    csv_size = int(subprocess.check_output(["du", "-sk", "output/superstore_csv"]).split()[0])
    parquet_size = int(subprocess.check_output(["du", "-sk", "output/superstore.parquet"]).split()[0])

    print(f"CSV     write: {csv_write_time:.2f}s  read: {csv_read_time:.2f}s  size: {csv_size} KB  rows: {csv_count}")
    print(f"Parquet write: {parquet_write_time:.2f}s  read: {parquet_read_time:.2f}s  size: {parquet_size} KB  rows: {parquet_count}")

    return {
        "csv_write_time": csv_write_time, "csv_read_time": csv_read_time, "csv_size_kb": csv_size,
        "parquet_write_time": parquet_write_time, "parquet_read_time": parquet_read_time, "parquet_size_kb": parquet_size,
    }


def handle_nulls(df):
    """Step 9: handle nulls and filter datasets efficiently."""
    print("\nSTEP 9: Handle nulls efficiently")

    null_counts = df.select([
        col(c).isNull().cast("int").alias(c) for c in ["Sales", "Category", "Region", "Postal Code"]
    ])
    from pyspark.sql.functions import sum as spark_sum
    null_summary = null_counts.agg(*[spark_sum(c).alias(c) for c in null_counts.columns])
    print("Null counts in key columns (0 expected, this dataset came in clean):")
    null_summary.show()

    # drop rows missing anything essential to the analysis, rather than
    # scanning the whole DataFrame with a Python loop row by row - letting
    # Spark's built-in dropna do this keeps the work distributed
    cleaned = df.dropna(subset=["Sales", "Category", "Region"])
    print(f"Rows after dropping nulls in essential columns: {cleaned.count()}")
    return cleaned


def build_pipeline(spark):
    """
    Step 10: full pipeline - read -> transform -> filter -> write.
    Step 11: best practices - avoid collect() on large results, use
    show()/limit() to preview instead, and coalesce(1) only at the very
    end for a single output file (not mid-pipeline, which would remove
    parallelism too early).
    """
    print("\nSTEP 10/11: Full pipeline (read -> transform -> filter -> write)")

    df = spark.read.csv(
        DATA_PATH, header=True, schema=SUPERSTORE_SCHEMA,
        multiLine=True, quote='"', escape='"'
    )

    pipeline_result = (
        df.dropna(subset=["Sales", "Category", "Region"])
        .withColumnRenamed("Sub-Category", "sub_category")
        .withColumn("profit_margin", when(col("Sales") > 0, col("Profit") / col("Sales")).otherwise(None))
        .filter(col("Sales") > 0)
    )

    # preview with show() instead of collect() - collect() pulls every row
    # back to the driver's memory, which doesn't scale on a large dataset;
    # show() only pulls back the handful of rows it displays
    print("Preview of the pipeline result (using .show(), not .collect()):")
    pipeline_result.select("Order ID", "Category", "sub_category", "Sales", "profit_margin").show(5, truncate=False)

    pipeline_result.coalesce(1).write.mode("overwrite").parquet("output/pipeline_result.parquet")
    print("Pipeline output written to output/pipeline_result.parquet")

    return pipeline_result


def run():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    explain_architecture()

    df = load_data(spark)
    explain_lazy_evaluation(df)
    select_and_filter(df)
    modify_dataframe(df)
    demonstrate_transformations_vs_actions(df)
    demonstrate_wide_transformation_and_shuffle(df)

    perf_stats = compare_csv_vs_parquet(spark, df)
    demonstrate_predicate_pushdown(spark)

    handle_nulls(df)
    build_pipeline(spark)

    print("\nDone. See output/ for the written files and insights.md for the write-up.")
    print(f"Performance summary: {perf_stats}")

    spark.stop()


if __name__ == "__main__":
    run()
