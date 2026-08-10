# Databricks notebook source
# 05_silver_doctors_treatments — actual supplied provider + treatment sources.
from pyspark.sql import functions as F
import uuid
batch=str(uuid.uuid4())

d=spark.table('healthcare_bronze.doctors')
d=(d.withColumn('phone_number_hash',F.sha2(F.col('phone_number').cast('string'),256))
   .withColumn('email_hash',F.sha2(F.lower(F.trim('email')),256))
   .drop('phone_number','email')
   .withColumn('_dq_passed',F.col('doctor_id').isNotNull() & (F.col('years_experience')>=0))
   .withColumn('_dq_failure_reason',F.when(F.col('doctor_id').isNull(),'MISSING_DOCTOR_ID').when(F.col('years_experience')<0,'NEGATIVE_EXPERIENCE').otherwise(''))
   .withColumn('_dq_score',F.when(F.col('_dq_passed'),1.0).otherwise(0.0)).withColumn('_silver_load_timestamp',F.current_timestamp()).withColumn('_silver_batch_id',F.lit(batch)))
d.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('healthcare_silver.doctors')

t=spark.table('healthcare_bronze.treatments')
a=spark.table('healthcare_bronze.appointments').select('appointment_id','patient_id','doctor_id','appointment_date','status')
t=(t.join(a,'appointment_id','left').withColumn('treatment_date',F.to_date('treatment_date')).withColumn('cost',F.col('cost').cast('double'))
 .withColumn('_dq_passed',F.col('treatment_id').isNotNull() & F.col('appointment_id').isNotNull() & (F.col('cost')>=0) & F.col('treatment_date').isNotNull())
 .withColumn('_dq_failure_reason',F.when(F.col('treatment_id').isNull(),'MISSING_TREATMENT_ID').when(F.col('appointment_id').isNull(),'MISSING_APPOINTMENT').when(F.col('cost')<0,'NEGATIVE_COST').otherwise(''))
 .withColumn('_dq_score',F.when(F.col('_dq_passed'),1.0).otherwise(0.0)).withColumn('_silver_load_timestamp',F.current_timestamp()).withColumn('_silver_batch_id',F.lit(batch)))
t.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('healthcare_silver.treatments')

# Optional PDD sources: if files arrive, use this same pattern; currently disabled in metadata.
print('PDD lab_results and medications contracts are inactive because files were not supplied.')
