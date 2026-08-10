from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, min, max

spark = SparkSession.builder.appName("CustomerSalesAnalysis").master("local[*]").getOrCreate()

# Load CSV
df = spark.read.option("header", True).option("inferSchema", True).csv("customer_sales.csv")

print("Original Data:")
df.show()
df.printSchema()

# Remove duplicates
df_clean = df.dropDuplicates()

# Handle missing values
df_clean = df_clean.fillna({
    "category": "Unknown",
    "region": "Unknown"
})

# Transform columns
df_clean = df_clean.withColumn("sales", col("sales").cast("double"))
df_clean = df_clean.withColumnRenamed("sales", "total_sales")

# Filter: age 25-40 and Clothing/Electronics
filtered_df = df_clean.filter(
    (col("age").between(25, 40)) &
    (col("category").isin("Clothing", "Electronics"))
)

print("Filtered Data:")
filtered_df.show()

# Category-wise aggregation
category_summary = df_clean.groupBy("category").agg(
    count("id").alias("customer_count"),
    sum("total_sales").alias("total_sales"),
    avg("total_sales").alias("average_sales"),
    min("total_sales").alias("minimum_sales"),
    max("total_sales").alias("maximum_sales")
)

print("Category-wise Summary:")
category_summary.orderBy("category").show()

# Region-wise aggregation
region_summary = df_clean.groupBy("region").agg(
    count("id").alias("customer_count"),
    sum("total_sales").alias("total_sales"),
    avg("total_sales").alias("average_sales")
)

print("Region-wise Summary:")
region_summary.orderBy("region").show()

# Aggregated condition
high_sales_categories = category_summary.filter(col("total_sales") > 90000)

print("Categories with Sales > 90,000:")
high_sales_categories.orderBy("total_sales").show()

spark.stop()
