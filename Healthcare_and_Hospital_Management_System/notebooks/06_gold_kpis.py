# Databricks notebook source
# 06_gold_kpis — all PDD KPIs plus dataset-compatible KPIs. Unsupported KPIs are explicit, never fabricated.
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

ap=spark.table('healthcare_silver.appointments')
b=spark.table('healthcare_silver.billing')
t=spark.table('healthcare_silver.treatments')
d=spark.table('healthcare_silver.doctors')

# Gold metadata helper.
def meta(df, period='ALL'):
    return (df.withColumn('_gold_load_timestamp',F.current_timestamp())
              .withColumn('_gold_batch_id',F.lit('${batch_id}'))
              .withColumn('_kpi_period',F.lit(period))
              .withColumn('_kpi_version',F.lit('2.0.0'))
              .withColumn('_silver_source_tables',F.lit('healthcare_silver.appointments,billing,treatments,doctors'))
              .withColumn('_department_id',F.lit(None).cast('string'))
              .withColumn('_report_as_of_date',F.current_date())
              .withColumn('_threshold_status',F.lit('NOT_EVALUATED'))
              .withColumn('_is_restatement',F.lit(False)))

# KPI summary.
tot=ap.agg(F.count('*').alias('total')).first()['total']
summary=(ap.agg(
 F.lit('Patient No-Show Rate').alias('kpi_name'),
 (F.avg(F.when(F.col('status')=='No-show',1).otherwise(0))*100).alias('kpi_value'),
 F.lit('PDD target < 10%').alias('target'),
 F.lit('percentage').alias('unit')))

def one(name, value_expr, target, unit, status_expr=F.lit('CALCULATED')):
 return F.select(F.lit(name).alias('kpi_name'),value_expr.alias('kpi_value'),F.lit(target).alias('target'),F.lit(unit).alias('unit'),status_expr.alias('status'))

kpis=[
 one('Patient No-Show Rate',F.avg(F.when(F.col('status')=='No-show',1).otherwise(0))*100,'< 10%','%'),
 one('Appointment Completion Rate',F.avg(F.when(F.col('status')=='Completed',1).otherwise(0))*100,'Informational','%'),
 one('Appointment Cancellation Rate',F.avg(F.when(F.col('status')=='Cancelled',1).otherwise(0))*100,'Informational','%'),
 one('Appointment Scheduled Rate',F.avg(F.when(F.col('status')=='Scheduled',1).otherwise(0))*100,'Informational','%'),
]
paid=b.agg(F.avg(F.when(F.col('payment_status')=='Paid',1).otherwise(0))*100).first()[0]
base=spark.createDataFrame([('Payment Success Rate (Proxy)',float(paid) if paid is not None else None,'Informational','%','CALCULATED')],['kpi_name','kpi_value','target','unit','status'])
for q in kpis: base=base.unionByName(q)
base=base.unionByName(spark.createDataFrame([('Total Billed Amount',float(b.agg(F.sum('amount')).first()[0]),'Informational','currency','CALCULATED'),('Average Treatment Cost',float(t.agg(F.avg('cost')).first()[0]),'Informational','currency','CALCULATED'),('Patients With Appointments',float(ap.select('patient_id').distinct().count()),'Informational','count','CALCULATED')],base.schema))
meta(base).write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('healthcare_gold.gold_kpi_summary')

# Individual PDD-compatible KPI tables — unavailable KPIs carry null and explicit status.
unsupported=[
 ('Avg Length of Stay (ALOS)','NOT COMPUTABLE FROM SUPPLIED DATA','Missing admit/discharge dates'),
 ('HCAHPS Satisfaction Score','NOT COMPUTABLE FROM SUPPLIED DATA','Missing satisfaction survey score'),
 ('Billing Accuracy Rate','NOT COMPUTABLE FROM SUPPLIED DATA','Missing authoritative billing accuracy rule/flag'),
 ('Insurance Claim Approval Rate','NOT COMPUTABLE FROM SUPPLIED DATA','billing.csv contains payment_status, not claim_status'),
 ('Medication Inventory Turnover','NOT COMPUTABLE FROM SUPPLIED DATA','medications.csv not supplied'),
 ('Operating Cost per Patient','NOT COMPUTABLE FROM SUPPLIED DATA','Operating-cost source not supplied'),
 ('Patient Readmission Rate','NOT COMPUTABLE FROM SUPPLIED DATA','Admission/discharge history not supplied')]
spark.createDataFrame([(x[0],None,'',x[1],x[2]) for x in unsupported],['kpi_name','kpi_value','target','status','reason']).write.format('delta').mode('overwrite').saveAsTable('healthcare_gold.pdd_kpi_gap')

# Individual actual KPI tables.
ap.groupBy(F.date_format('appointment_date','yyyy-MM').alias('month')).agg(F.count('*').alias('total_appointments'),F.sum(F.when(F.col('status')=='Completed',1).otherwise(0)).alias('completed'),F.sum(F.when(F.col('status')=='Cancelled',1).otherwise(0)).alias('cancelled'),F.sum(F.when(F.col('status')=='No-show',1).otherwise(0)).alias('no_show'),F.sum(F.when(F.col('status')=='Scheduled',1).otherwise(0)).alias('scheduled')).withColumn('no_show_rate',F.col('no_show')/F.col('total_appointments')*100).withColumn('completion_rate',F.col('completed')/F.col('total_appointments')*100).write.format('delta').mode('overwrite').saveAsTable('healthcare_gold.monthly_appointment_kpis')

doc_ap=ap.join(d.select('doctor_id','first_name','last_name','specialization','hospital_branch'),'doctor_id','left')
doc_ap.groupBy('doctor_id','first_name','last_name','specialization','hospital_branch').agg(F.count('*').alias('appointment_count'),F.sum(F.when(F.col('status')=='Completed',1).otherwise(0)).alias('completed_count'),F.sum(F.when(F.col('status')=='Cancelled',1).otherwise(0)).alias('cancelled_count'),F.sum(F.when(F.col('status')=='No-show',1).otherwise(0)).alias('no_show_count')).withColumn('no_show_rate',F.col('no_show_count')/F.col('appointment_count')*100).withColumn('share_of_total_appointments',F.col('appointment_count')/F.lit(tot)*100).write.format('delta').mode('overwrite').saveAsTable('healthcare_gold.doctor_workload')

t.groupBy('treatment_type').agg(F.count('*').alias('treatment_count'),F.sum('cost').alias('total_cost'),F.avg('cost').alias('average_cost'),F.min('cost').alias('minimum_cost'),F.max('cost').alias('maximum_cost')).write.format('delta').mode('overwrite').saveAsTable('healthcare_gold.treatment_summary')
b.groupBy('payment_method','payment_status').agg(F.count('*').alias('bill_count'),F.sum('amount').alias('total_amount'),F.avg('amount').alias('average_amount')).write.format('delta').mode('overwrite').saveAsTable('healthcare_gold.billing_summary')
