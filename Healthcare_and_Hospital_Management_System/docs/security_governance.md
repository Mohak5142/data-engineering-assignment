# Security & Governance

## PDD controls mapped to implementation
- PII/PHI classification is stored in source metadata and audit logs.
- SHA-256 hashing is applied to DOB, phone, email, address and insurance number in the supplied patient/doctor flows. SSN is not present in the supplied dataset.
- Gold tables avoid direct patient PII.
- Delta tables provide ACID transaction semantics.
- Unity Catalog/RBAC statements are supplied in `sql/13_governance.sql`; actual grants require the hospital Databricks workspace and identities.
- ADLS encryption at rest and TLS 1.2+ are platform controls to be enabled in Azure.
- Retention targets: Bronze 7 years, Silver 5 years, Gold 3 years.
- GRS/DR, incident response and alert routing are deployment controls; configuration guidance is provided, but cannot be activated without the target Azure subscription.

## HIPAA note
This project demonstrates technical controls aligned to the PDD. It does not constitute legal HIPAA certification or a complete compliance program.
