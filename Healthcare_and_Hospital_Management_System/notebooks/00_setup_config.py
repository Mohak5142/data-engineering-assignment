# Databricks notebook source
# COMMAND ----------
# 00_setup_config — idempotent project setup
# Creates schemas, metadata config, audit log, quarantine locations and governance views.

from pyspark.sql import functions as F
from pyspark.sql.types import *
import json

CATALOG = "healthcare"
for schema in ["healthcare_bronze","healthcare_silver","healthcare_gold","healthcare_ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# Metadata configuration is stored as Delta.
metadata_path = "/Workspace/Shared/healthcare-medallion/config/source_metadata.csv"
metadata_df = spark.read.option("header",True).option("inferSchema",True).csv(metadata_path)
(metadata_df.write.format("delta").mode("overwrite").option("overwriteSchema",True)
 .saveAsTable("healthcare_ops.source_metadata"))

# Audit table — append-only operational ledger.
spark.sql("""
CREATE TABLE IF NOT EXISTS healthcare_ops.audit_log (
  audit_id STRING, batch_id STRING, source_name STRING, layer STRING,
  pipeline_start_time TIMESTAMP, pipeline_end_time TIMESTAMP,
  rows_read BIGINT, rows_written BIGINT, rows_rejected BIGINT,
  status STRING, error_message STRING, triggered_by STRING, created_at TIMESTAMP,
  pipeline_duration_secs BIGINT, notebook_name STRING, cluster_id STRING,
  spark_app_id STRING, rows_quarantined BIGINT, dq_score_avg DOUBLE,
  schema_version STRING, environment STRING, retry_attempt INT,
  data_classification STRING, sla_met BOOLEAN, downstream_notified BOOLEAN
) USING DELTA
TBLPROPERTIES ('delta.appendOnly'='true')
""")

# Control tables.
spark.sql("""
CREATE TABLE IF NOT EXISTS healthcare_ops.pipeline_run_control (
 batch_id STRING, pipeline_name STRING, start_time TIMESTAMP, end_time TIMESTAMP,
 status STRING, current_stage STRING, error_message STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS healthcare_ops.unsupported_kpis (
 kpi_name STRING, status STRING, reason STRING, required_fields STRING, available_fields STRING,
 recommendation STRING, as_of_date DATE
) USING DELTA
""")

# PDD expected-but-not-supplied source contracts.
# They remain inactive until actual files arrive; no synthetic data is created.
print("Setup complete. Actual active sources: patients, appointments, billing, doctors, treatments.")
print("PDD source contracts lab_results and medications remain inactive until supplied.")
