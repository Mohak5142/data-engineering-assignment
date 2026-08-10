# Databricks notebook source
# 07_audit_report — operational monitoring and DQ/SLA trends.
from pyspark.sql import functions as F

a=spark.table('healthcare_ops.audit_log')
summary=(a.groupBy('batch_id','status').agg(F.min('pipeline_start_time').alias('start_time'),F.max('pipeline_end_time').alias('end_time'),F.sum('rows_read').alias('rows_read'),F.sum('rows_written').alias('rows_written'),F.sum('rows_rejected').alias('rows_rejected'),F.sum('rows_quarantined').alias('rows_quarantined'),F.avg('dq_score_avg').alias('dq_score_avg'),F.max('pipeline_duration_secs').alias('duration_secs'),F.min(F.col('sla_met').cast('int')).alias('sla_met')))
summary.write.format('delta').mode('overwrite').saveAsTable('healthcare_gold.audit_run_summary')

a.filter(F.col('status')!='SUCCESS').orderBy(F.col('pipeline_end_time').desc()).createOrReplaceTempView('recent_failures')
display(summary.orderBy(F.col('start_time').desc()))
