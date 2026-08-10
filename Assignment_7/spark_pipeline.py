"""
Spark Fundamentals - Data Cleaning, Transformation, and Aggregation
Dataset: Sample - Superstore.csv

Covers:
  Step 1/2: start a Spark session
  Step 3: load the CSV, look at the raw data
  Step 4: clean the data (duplicates, nulls, column names/types)
  Step 5: filter the data (quantity range, category, region)
  Step 6: aggregations (count, sum, avg, min, max)
  Step 7: groupBy with a condition on the aggregated result
  Step 8: notes on wide vs narrow transformations / shuffle
  Step 9: full pipeline combining cleaning + aggregation, with results
          and a written insights file

Run: python3 spark_pipeline.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, count, sum as spark_sum, avg, min as spark_min,
    max as spark_max, to_date
)
from pyspark.sql.types import DoubleType, IntegerType

DATA_PATH = "Sample - Superstore.csv"

# the raw file uses "Order ID", "Sub-Category", etc - map those to plain
# snake_case names so the rest of the pipeline is easier to read/type
COLUMN_RENAMES = {
    "Row ID": "row_id",
    "Order ID": "order_id",
    "Order Date": "order_date",
    "Ship Date": "ship_date",
    "Ship Mode": "ship_mode",
    "Customer ID": "customer_id",
    "Customer Name": "customer_name",
    "Segment": "segment",
    "Country": "country",
    "City": "city",
    "State": "state",
    "Postal Code": "postal_code",
    "Region": "region",
    "Product ID": "product_id",
    "Category": "category",
    "Sub-Category": "sub_category",
    "Product Name": "product_name",
    "Sales": "sales",
    "Quantity": "quantity",
    "Discount": "discount",
    "Profit": "profit",
}


def get_spark():
    return (
        SparkSession.builder
        .appName("SuperstoreAnalysis")
        .master("local[*]")
        .getOrCreate()
    )


def load_data(spark, path=DATA_PATH):
    """Step 3: load the CSV into a DataFrame and take a first look at it."""
    # note: the original Kaggle download of this file is saved as Latin-1
    # (ISO-8859-1). Passing encoding="ISO-8859-1" straight to Spark's CSV
    # reader caused it to misparse quoted fields (product names containing
    # commas) and shift columns on some rows, so the file is transcoded to
    # UTF-8 ahead of time instead (see the top of this repo / README) and
    # read here with Spark's normal UTF-8 default.
    # this file has a product name with an embedded escaped quote
    # (14 7/8"" x 11""), written the standard CSV way (a doubled quote).
    # Spark's CSV reader defaults to backslash as the escape character,
    # not a doubled quote, so without escape='"' it misreads that field
    # and shifts every column after it for that row. multiLine is turned
    # on too since the field spans a quoted value with embedded commas.
    # inferSchema was also unreliable on this file, so everything is read
    # in as a string and the numeric columns are cast explicitly below.
    df = spark.read.csv(path, header=True, inferSchema=False,
                         multiLine=True, quote='"', escape='"')

    print("\nFirst 5 rows:")
    df.show(5, truncate=False)

    print("Column names:", df.columns)

    print("\nSchema (data types):")
    df.printSchema()

    return df


def clean_data(df):
    """
    Step 4: data cleaning
    - remove exact duplicate rows (none in this dataset, but the step is
      still here since a raw data source shouldn't be trusted blindly)
    - rename columns to snake_case so they don't need backticks everywhere
    - trim whitespace on text columns
    - drop rows missing category/region/sales, since those are needed for
      the aggregations below (this dataset happens to have none, but the
      pipeline shouldn't assume that will always be true)
    - parse order_date/ship_date from text into real date columns
    - cast postal_code to a string, since it's an identifier, not a
      quantity that should ever be summed or averaged
    """
    before_count = df.count()

    df = df.dropDuplicates()

    for old_name, new_name in COLUMN_RENAMES.items():
        df = df.withColumnRenamed(old_name, new_name)

    text_columns = ["ship_mode", "segment", "country", "city", "state",
                     "region", "category", "sub_category", "product_name"]
    for column in text_columns:
        df = df.withColumn(column, trim(col(column)))

    df = df.dropna(subset=["category", "region", "sales"])

    df = df.withColumn("order_date", to_date(col("order_date"), "M/d/yyyy"))
    df = df.withColumn("ship_date", to_date(col("ship_date"), "M/d/yyyy"))
    df = df.withColumn("postal_code", col("postal_code").cast("string"))

    # cast the numeric columns explicitly rather than trusting inferSchema -
    # inferSchema got these wrong (sales/quantity/discount came back as
    # strings) once the encoding option was set, so it's safer to cast
    # by hand than assume the inferred types are right
    df = df.withColumn("sales", col("sales").cast(DoubleType()))
    df = df.withColumn("quantity", col("quantity").cast(IntegerType()))
    df = df.withColumn("discount", col("discount").cast(DoubleType()))
    df = df.withColumn("profit", col("profit").cast(DoubleType()))
    df = df.withColumn("row_id", col("row_id").cast(IntegerType()))

    after_count = df.count()
    print(f"\nCleaning: {before_count} rows -> {after_count} rows "
          f"({before_count - after_count} removed as duplicates/unusable)")

    return df


def filter_data(df):
    """Step 5: apply filtering conditions - quantity range, category, region."""
    filtered = df.filter(
        (col("quantity") >= 2) & (col("quantity") <= 10)
        & (col("category") == "Furniture")
        & (col("region") == "West")
    )
    print(f"\nFiltered (quantity 2-10, category=Furniture, region=West): {filtered.count()} rows")
    filtered.select("order_id", "category", "region", "quantity", "sales", "profit").show(5, truncate=False)
    return filtered


def aggregate_data(df):
    """Step 6: aggregation functions - count, sum, avg, min, max."""
    summary = df.agg(
        count("*").alias("total_orders"),
        spark_sum("sales").alias("total_sales"),
        avg("sales").alias("avg_sales"),
        spark_min("sales").alias("min_sales"),
        spark_max("sales").alias("max_sales"),
    )
    print("\nOverall sales summary:")
    summary.show(truncate=False)
    return summary


def group_and_filter(df):
    """
    Step 7: group by category, then apply a condition on the aggregated
    result (only keep categories with total profit above 100,000 -
    similar to a SQL HAVING clause).
    """
    grouped = (
        df.groupBy("category")
        .agg(
            count("*").alias("order_count"),
            spark_sum("sales").alias("total_sales"),
            spark_sum("profit").alias("total_profit"),
            avg("discount").alias("avg_discount"),
        )
        .filter(col("total_profit") > 100000)
        .orderBy(col("total_profit").desc())
    )
    print("\nCategories with total profit over 100,000:")
    grouped.show(truncate=False)
    return grouped


def explain_transformations():
    """
    Step 8: notes on wide vs narrow transformations.

    Narrow transformations (filter, select, withColumn) - each output
    partition only depends on one input partition, so Spark doesn't need
    to move data between nodes.

    Wide transformations (groupBy, join, orderBy, distinct) - rows with
    the same key can live on different partitions, so Spark has to shuffle
    data across the cluster to bring matching rows together before it can
    compute the result. Shuffles are the expensive part of a Spark job,
    which is why groupBy/join heavy pipelines are slower than pipelines
    that only filter and select.
    """
    print("\nWide vs narrow transformations:")
    print(explain_transformations.__doc__)


def run_pipeline():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    raw_df = load_data(spark)
    clean_df = clean_data(raw_df)

    filter_data(clean_df)
    aggregate_data(clean_df)
    grouped_df = group_and_filter(clean_df)
    explain_transformations()

    clean_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/cleaned_superstore")
    grouped_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/category_summary")

    print("\nPipeline finished. Cleaned data and category summary written to output/")

    spark.stop()


if __name__ == "__main__":
    run_pipeline()
