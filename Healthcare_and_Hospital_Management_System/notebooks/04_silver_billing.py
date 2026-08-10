# Databricks notebook source
# 04_silver_billing — billing cleansing and payment-status enrichment.
from pyspark.sql import functions as F
import uuid
b=spark.table('healthcare_bronze.billing')
t=spark.table('healthcare_bronze.treatments')
x=(b.join(t.select('treatment_id','treatment_type','cost').dropDuplicates(['treatment_id']),'treatment_id','left')
 .withColumn('bill_date',F.to_date('bill_date')).withColumn('amount',F.col('amount').cast('double'))
 .withColumn('payment_status',F.initcap(F.trim('payment_status')))
 .withColumn('payment_success_flag',F.when(F.col('payment_status')=='Paid',1).otherwise(0))
 .withColumn('_dq_passed',F.col('bill_id').isNotNull() & F.col('patient_id').isNotNull() & F.col('treatment_id').isNotNull() & (F.col('amount')>=0) & F.col('payment_status').isin('Paid','Pending','Failed'))
 .withColumn('_dq_failure_reason',F.when(F.col('bill_id').isNull(),'MISSING_BILL_ID').when(F.col('amount')<0,'NEGATIVE_AMOUNT').when(~F.col('payment_status').isin('Paid','Pending','Failed'),'INVALID_PAYMENT_STATUS').otherwise(''))
 .withColumn('_dq_score',F.when(F.col('_dq_passed'),1.0).otherwise(0.0)).withColumn('_silver_load_timestamp',F.current_timestamp()).withColumn('_silver_batch_id',F.lit(str(uuid.uuid4()))))
x.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('healthcare_silver.billing')
