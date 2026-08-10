# Healthcare & Hospital Management System — Complete Medallion Architecture

## Executive summary
This project implements the supplied PDD as a Databricks/PySpark/Delta Medallion pipeline. It is built around the five actual CSVs supplied with the internship package: patients, appointments, billing, doctors and treatments.

## Critical source-data note
The PDD expects `patient_master.csv`, `appointments.csv`, `billing_claims.csv`, `lab_results.csv`, and `medications.csv`. The actual package contains `patients.csv`, `appointments.csv`, `billing.csv`, `doctors.csv`, and `treatments.csv`. This project does not fabricate the missing lab/medication/claim/admission/satisfaction data. Instead, the PDD source contracts are represented as inactive metadata entries and unsupported KPIs are explicitly reported.

## Actual baseline
- Patients: 50
- Appointments: 200
- Billing: 200
- Doctors: 10
- Treatments: 200

## KPI baseline
- No-show rate: 26.00%
- Completion rate: 23.00%
- Cancellation rate: 25.50%
- Scheduled rate: 25.50%
- Payment success rate (proxy): 32.00%
- Total billed amount: 551249.85
- Average treatment cost: 2756.25
- Patients with appointments: 48

## Architecture
CSV -> ADLS/DBFS landing -> metadata-driven Bronze Delta -> Silver Delta cleansing/enrichment/SCD2 -> Gold KPI Delta -> Databricks SQL/BI -> audit & monitoring.

## Run order
1. Upload `data/` CSVs to the landing path.
2. Import notebooks into Databricks.
3. Run `00_setup_config`.
4. Run `01_ingest_bronze`.
5. Run Silver notebooks 02–05 (parallel where dependencies allow).
6. Run `06_gold_kpis`.
7. Run `07_audit_report`.
8. Deploy `workflows/healthcare_daily_pipeline.yml` or configure the same DAG in Databricks Workflows.

## Deployment artifacts
- `databricks/databricks.yml`
- `workflows/healthcare_daily_pipeline.yml`
- `workflows/job_payload.json`
- `sql/13_governance.sql`
- `docs/azure_adls_deployment.md`

## Testing
Run `pytest -q tests` locally for source-level validations. Databricks notebooks should also be run end-to-end for Spark/Delta integration validation.

## Compliance
The implementation demonstrates technical controls described in the PDD. It is not legal HIPAA certification. Platform controls such as Azure encryption, Unity Catalog grants, GRS and network configuration must be enabled in the target subscription/workspace.
