# Databricks notebook source
# 02_silver_patients — cleansing + SHA-256 PII protection + real SCD Type 2.
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from datetime import datetime

src=spark.table('healthcare_bronze.patients')
# Select latest record per natural key from the current batch/source history.
w=Window.partitionBy('patient_id').orderBy(F.col('_ingestion_timestamp').desc())
df=(src.withColumn('rn',F.row_number().over(w)).filter('rn=1').drop('rn')
    .withColumn('date_of_birth',F.to_date('date_of_birth'))
    .withColumn('registration_date',F.to_date('registration_date'))
    .withColumn('email',F.lower(F.trim('email')))
    .withColumn('gender',F.upper(F.trim('gender'))))

# Business validations.
valid_gender=F.col('gender').isin('M','F','O','OTHER')
valid_email=F.col('email').rlike(r'^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')
mandatory=F.col('patient_id').isNotNull() & F.col('date_of_birth').isNotNull() & F.col('registration_date').isNotNull()

silver=(df.withColumn('_dq_passed',mandatory & valid_email & valid_gender)
 .withColumn('_dq_failure_reason',F.when(~mandatory,'MANDATORY_FIELD_MISSING').when(~valid_email,'INVALID_EMAIL').when(~valid_gender,'INVALID_GENDER').otherwise(''))
 .withColumn('_dq_score',F.when(mandatory & valid_email & valid_gender,1.0).otherwise(0.0))
 # PII protection required by PDD: SSN, DOB, Phone. SSN not supplied; DOB is hashed below.
 .withColumn('date_of_birth_hash',F.sha2(F.col('date_of_birth').cast('string'),256))
 .withColumn('contact_number_hash',F.sha2(F.col('contact_number').cast('string'),256))
 .withColumn('email_hash',F.sha2('email',256))
 .withColumn('address_hash',F.sha2('address',256))
 .withColumn('insurance_number_hash',F.sha2('insurance_number',256))
 .drop('date_of_birth','contact_number','email','address','insurance_number')
 .withColumn('_silver_load_timestamp',F.current_timestamp())
 .withColumn('_silver_batch_id',F.first('_batch_id').over(Window.orderBy(F.lit(1))))
 .withColumn('effective_from',F.current_timestamp())
 .withColumn('effective_to',F.to_timestamp(F.lit('9999-12-31 23:59:59')))
 .withColumn('_is_current',F.lit(True))
 .withColumn('_record_version',F.lit(1))
 .withColumn('_masked_fields',F.lit('DOB,PHONE,EMAIL,ADDRESS,INSURANCE_NUMBER')))

# Real SCD2 using Delta MERGE.
target='healthcare_silver.patients'
if not spark.catalog.tableExists(target):
    silver.write.format('delta').mode('overwrite').saveAsTable(target)
else:
    tgt=DeltaTable.forName(spark,target)
    current=tgt.toDF().filter('_is_current=true').select('patient_id',*[c for c in silver.columns if c not in ['patient_id','effective_from','effective_to','_is_current','_record_version']])
    # Hash of business attributes excluding operational metadata determines change.
    attrs=['first_name','last_name','gender','registration_date','insurance_provider','date_of_birth_hash','contact_number_hash','email_hash','address_hash','insurance_number_hash']
    incoming=silver.withColumn('_business_hash',F.sha2(F.concat_ws('||',*[F.coalesce(F.col(c).cast('string'),F.lit('')) for c in attrs]),256))
    existing=tgt.toDF().filter('_is_current=true').withColumn('_business_hash',F.sha2(F.concat_ws('||',*[F.coalesce(F.col(c).cast('string'),F.lit('')) for c in attrs]),256))
    changed=incoming.alias('s').join(existing.select('patient_id','_business_hash').alias('t'),F.col('s.patient_id')==F.col('t.patient_id'),'left')
    changed=changed.filter(F.col('t.patient_id').isNull() | (F.col('s._business_hash')!=F.col('t._business_hash'))).select('s.*')
    # Expire old current records for changed keys.
    changed_keys=changed.select('patient_id').distinct()
    (tgt.alias('t').merge(changed_keys.alias('s'),'t.patient_id=s.patient_id AND t._is_current=true')
       .whenMatchedUpdate(set={'_is_current':'false','effective_to':'current_timestamp()'})
       .execute())
    # Insert new versions.
    (changed.drop('_business_hash').alias('c').join(existing.select('patient_id','_record_version').alias('e'),'patient_id','left')
       .withColumn('_record_version',F.coalesce(F.col('e._record_version'),F.lit(0))+1)
       .drop('e._record_version')
       .write.format('delta').mode('append').saveAsTable(target))
