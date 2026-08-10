# PDD Requirement Matrix

| Requirement | Implementation | Status |
|---|---|---|
| Bronze/Silver/Gold | Delta tables + notebooks | Implemented |
| Metadata-driven ingestion | source_metadata Delta + generic notebook | Implemented |
| Daily workflow | Databricks Asset Bundle YAML | Implemented as deployment artifact |
| Audit log | healthcare_ops.audit_log | Implemented |
| Retry count | metadata + workflow + notebook retry loop | Implemented |
| Quarantine | per-source quarantine path | Implemented |
| SCD Type 2 patients | Delta MERGE + effective dates/current flag | Implemented |
| SHA-256 PII masking | DOB/phone/email/address/insurance | Implemented; SSN absent |
| 8 PDD KPIs | KPI engine + unsupported KPI table | Implemented without fabrication; 7 unavailable from actual files |
| DQ | validations + score + audit | Implemented |
| Referential integrity | all supplied FK relationships | Implemented |
| ADLS Gen2 | deployment guide/configurable paths | Deployment dependent |
| Unity Catalog/RBAC | governance SQL + design | Deployment dependent |
| Encryption/TLS | Azure deployment controls | Deployment dependent |
| Retention 7/5/3 years | config + deployment guide | Deployment dependent |
| GRS/DR | deployment guide | Deployment dependent |
| Incident response | audit + alert design | Deployment dependent |
| Tests | pytest suite | Implemented |
| BI-ready Gold | KPI, monthly, doctor, treatment, billing tables | Implemented |
| Actual source data | all 5 supplied CSVs | Implemented |
| PDD lab/med sources | inactive contracts because files absent | Blocked by source availability |
