-- Example Unity Catalog/RBAC controls. Execute only in a governed Databricks environment.
CREATE CATALOG IF NOT EXISTS healthcare;
CREATE SCHEMA IF NOT EXISTS healthcare_bronze;
CREATE SCHEMA IF NOT EXISTS healthcare_silver;
CREATE SCHEMA IF NOT EXISTS healthcare_gold;
CREATE SCHEMA IF NOT EXISTS healthcare_ops;
-- GRANT USE CATALOG ON CATALOG healthcare TO `data-engineers`;
-- GRANT SELECT ON SCHEMA healthcare_gold TO `bi-analysts`;
-- REVOKE SELECT ON SCHEMA healthcare_bronze FROM `bi-analysts`;
-- Apply column masks to sensitive Silver columns if retained by policy. Gold contains aggregate metrics only.
