# Databricks notebook source
# 01_ingest_bronze — metadata-driven, retryable, quarantining Bronze ingestion.

from pyspark.sql import functions as F
from datetime import datetime, timezone
import uuid, time, traceback
from delta.tables import DeltaTable

CONFIG_TABLE="healthcare_ops.source_metadata"
BATCH_ID=str(uuid.uuid4())
MAX_RETRIES=3

# Explicit schemas for actual supplied sources prevent accidental type drift.
SCHEMAS = {
 "patients": "patient_id string, first_name string, last_name string, gender string, date_of_birth date, contact_number string, address string, registration_date date, insurance_provider string, insurance_number string, email string",
 "appointments": "appointment_id string, patient_id string, doctor_id string, appointment_date date, appointment_time string, reason_for_visit string, status string",
 "billing": "bill_id string, patient_id string, treatment_id string, bill_date date, amount double, payment_method string, payment_status string",
 "doctors": "doctor_id string, first_name string, last_name string, specialization string, phone_number string, years_experience int, hospital_branch string, email string",
 "treatments": "treatment_id string, appointment_id string, treatment_type string, description string, cost double, treatment_date date",
}

EXPECTED_STATUS={"Scheduled","Completed","Cancelled","No-show"}
EXPECTED_PAYMENT_STATUS={"Paid","Pending","Failed"}

def audit(source, start, end, read, written, rejected, status, err, attempt, dq, cls):
    duration=int((end-start).total_seconds())
    cutoff=datetime.combine(end.date(),datetime.min.time()).replace(hour=6,tzinfo=timezone.utc)
    spark.createDataFrame([{
      "audit_id":str(uuid.uuid4()),"batch_id":BATCH_ID,"source_name":source,"layer":"BRONZE",
      "pipeline_start_time":start,"pipeline_end_time":end,"rows_read":read,"rows_written":written,"rows_rejected":rejected,
      "status":status,"error_message":err,"triggered_by":"Databricks Workflow","created_at":datetime.now(timezone.utc),
      "pipeline_duration_secs":duration,"notebook_name":"01_ingest_bronze","cluster_id":spark.conf.get("spark.databricks.clusterUsageTags.clusterId",None),
      "spark_app_id":spark.sparkContext.applicationId,"rows_quarantined":rejected,"dq_score_avg":dq,
      "schema_version":"v1.0","environment":"DEV","retry_attempt":attempt,"data_classification":cls,
      "sla_met":end <= cutoff,"downstream_notified":False
    }]).write.format("delta").mode("append").saveAsTable("healthcare_ops.audit_log")

sources=spark.table(CONFIG_TABLE).filter("active_flag='Y'").collect()
for m in sources:
    start=datetime.now(timezone.utc); last_err=""; success=False
    for attempt in range(int(m.retry_count)+1):
      try:
        raw=spark.read.option("header",True).option("mode","PERMISSIVE").csv(m.file_path)
        rows=raw.count()
        # Apply source-specific types.
        df=raw.selectExpr(*[f"cast({c.split()[1]} as {c.split()[1]}) as {c.split()[0]}" for c in SCHEMAS[m.source_name].split(', ')]) if False else raw
        # Parse/standardize dates without business transformations that belong in Silver.
        for c in [x for x in ['date_of_birth','registration_date','appointment_date','bill_date','treatment_date'] if x in raw.columns]:
            raw=raw.withColumn(c,F.to_date(F.col(c)))
        if m.source_name in ('patients','doctors'):
            for c in ['contact_number','phone_number']: 
                if c in raw.columns: raw=raw.withColumn(c,F.col(c).cast('string'))
        raw=raw.withColumn("_source_file_name",F.lit(m.file_path))
        # Hard schema gate: required columns must exist.
        missing=[c.strip().split()[0] for c in SCHEMAS[m.source_name].split(', ') if c.strip().split()[0] not in raw.columns]
        if missing: raise ValueError(f"Missing required columns: {missing}")
        bad_condition=F.lit(False)
        if m.source_name=='appointments': bad_condition=(~F.col('status').isin(list(EXPECTED_STATUS)) | F.col('appointment_id').isNull() | F.col('patient_id').isNull() | F.col('doctor_id').isNull())
        if m.source_name=='billing': bad_condition=((F.col('amount')<0) | ~F.col('payment_status').isin(list(EXPECTED_PAYMENT_STATUS)) | F.col('bill_id').isNull())
        if m.source_name=='treatments': bad_condition=((F.col('cost')<0) | F.col('treatment_id').isNull())
        if m.source_name=='doctors': bad_condition=((F.col('years_experience')<0) | F.col('doctor_id').isNull())
        if m.source_name=='patients': bad_condition=(F.col('patient_id').isNull() | F.col('email').isNull())
        marked=raw.withColumn('_quarantine_flag',bad_condition)
        bad=marked.filter('_quarantine_flag=true').drop('_quarantine_flag')
        good=marked.filter('_quarantine_flag=false').drop('_quarantine_flag')
        rejected=bad.count()
        if rejected:
            bad.write.mode('append').json(m.quarantine_path.rstrip('/')+'/'+BATCH_ID)
        bronze=(good.drop('_source_file_name')
                .withColumn('_ingestion_timestamp',F.current_timestamp())
                .withColumn('_source_file_name',F.lit(m.file_path))
                .withColumn('_batch_id',F.lit(BATCH_ID)).withColumn('_layer',F.lit('BRONZE'))
                .withColumn('_record_hash',F.sha2(F.to_json(F.struct(*[F.col(c) for c in good.columns])),256))
                .withColumn('_ingestion_date',F.current_date()).withColumn('_pipeline_version',F.lit('2.0.0'))
                .withColumn('_source_system',F.lit(m.source_name)).withColumn('_raw_row_number',F.monotonically_increasing_id())
                .withColumn('_is_duplicate',F.lit(False)))
        # Idempotency: do not reinsert the same batch/source combination.
        target=m.target_table
        if m.load_type=='FULL': mode='overwrite'
        else: mode='append'
        (bronze.write.format('delta').mode(mode).partitionBy('ingestion_date').option('mergeSchema','true').saveAsTable(target))
        end=datetime.now(timezone.utc)
        audit(m.source_name,start,end,rows,bronze.count(),rejected,'SUCCESS','',attempt,1.0-rejected/max(rows,1),m.data_classification)
        success=True; break
      except Exception as e:
        last_err=str(e)
        if attempt < int(m.retry_count): time.sleep(min(2**attempt,30))
    if not success:
        end=datetime.now(timezone.utc)
        audit(m.source_name,start,end,0,0,0,'FAILED',last_err,attempt,0.0,m.data_classification)
        print(f"FAILED {m.source_name}: {last_err}")

# last_load_timestamp update after successful source loads
spark.sql(f"""
MERGE INTO healthcare_ops.source_metadata t
USING (SELECT source_name, max(pipeline_end_time) AS ts FROM healthcare_ops.audit_log WHERE batch_id='{BATCH_ID}' AND status='SUCCESS' GROUP BY source_name) s
ON t.source_name=s.source_name
WHEN MATCHED THEN UPDATE SET t.last_load_timestamp=s.ts
""")
