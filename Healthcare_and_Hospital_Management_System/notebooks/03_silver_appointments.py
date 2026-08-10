# Databricks notebook source
# 03_silver_appointments — validation, enrichment, no-show logic, DQ.
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import uuid

batch=str(uuid.uuid4())
a=spark.table('healthcare_bronze.appointments')
p=spark.table('healthcare_bronze.patients').select('patient_id','insurance_provider')
d=spark.table('healthcare_bronze.doctors').select('doctor_id','first_name','last_name','specialization','hospital_branch')
valid={'Scheduled','Completed','Cancelled','No-show'}

x=(a.join(p,'patient_id','left').join(d,'doctor_id','left')
 .withColumn('appointment_date',F.to_date('appointment_date'))
 .withColumn('appointment_time',F.to_timestamp(F.concat_ws(' ',F.col('appointment_date').cast('string'),F.col('appointment_time'))))
 .withColumn('no_show_flag',F.when(F.col('status')=='No-show',1).otherwise(0))
 .withColumn('_dq_passed',F.col('appointment_id').isNotNull() & F.col('patient_id').isNotNull() & F.col('doctor_id').isNotNull() & F.col('appointment_date').isNotNull() & F.col('status').isin(list(valid)) & F.col('insurance_provider').isNotNull())
 .withColumn('_dq_failure_reason',F.when(F.col('appointment_id').isNull(),'MISSING_APPOINTMENT_ID').when(~F.col('status').isin(list(valid)),'INVALID_STATUS').when(F.col('patient_id').isNull(),'MISSING_PATIENT').when(F.col('doctor_id').isNull(),'MISSING_DOCTOR').when(F.col('insurance_provider').isNull(),'PATIENT_LOOKUP_FAILED').otherwise(''))
 .withColumn('_dq_score',F.when(F.col('_dq_passed'),1.0).otherwise(0.0))
 .withColumn('_silver_load_timestamp',F.current_timestamp()).withColumn('_silver_batch_id',F.lit(batch)).withColumn('_masked_fields',F.lit('PATIENT_PII_NOT_PROJECTED')))
x.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('healthcare_silver.appointments')
