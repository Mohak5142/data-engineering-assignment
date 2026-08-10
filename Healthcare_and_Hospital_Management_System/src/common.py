from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timezone
import uuid

PIPELINE_VERSION = "2.0.0"

def utc_now():
    return datetime.now(timezone.utc)

def new_batch_id():
    return str(uuid.uuid4())

def add_bronze_metadata(df, source_name, batch_id, source_file):
    return (df.withColumn("_ingestion_timestamp", F.current_timestamp())
              .withColumn("_source_file_name", F.lit(source_file))
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_layer", F.lit("BRONZE"))
              .withColumn("_record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in df.columns])),256))
              .withColumn("_ingestion_date", F.current_date())
              .withColumn("_pipeline_version", F.lit(PIPELINE_VERSION))
              .withColumn("_source_system", F.lit(source_name))
              .withColumn("_raw_row_number", F.monotonically_increasing_id())
              .withColumn("_is_duplicate", F.lit(False)))

def add_silver_metadata(df, batch_id):
    return (df.withColumn("_silver_load_timestamp",F.current_timestamp())
              .withColumn("_silver_batch_id",F.lit(batch_id))
              .withColumn("_record_version",F.coalesce(F.col("_record_version"),F.lit(1))))

def dq_score(df, checks):
    # checks is list of boolean Column expressions. 1.0 means all checks pass.
    if not checks: return F.lit(1.0)
    return sum([F.when(c,1.0).otherwise(0.0) for c in checks]) / F.lit(float(len(checks)))

def hash_col(col_name):
    return F.sha2(F.col(col_name).cast("string"),256)

def sla_met(end_ts_col, cutoff="06:00"):
    # Evaluates whether completion time is before today's configured UTC cutoff.
    hh,mm=map(int,cutoff.split(":"))
    cutoff_ts=F.to_timestamp(F.concat_ws(" ",F.current_date(),F.lit(f"{hh:02d}:{mm:02d}:00")))
    return end_ts_col <= cutoff_ts

def write_audit(spark, rows):
    spark.createDataFrame(rows).write.format("delta").mode("append").saveAsTable("healthcare_ops.audit_log")
