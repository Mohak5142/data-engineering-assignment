# Azure ADLS Gen2 Deployment

1. Create storage account with hierarchical namespace.
2. Create containers: landing, bronze, silver, gold, quarantine.
3. Enable encryption at rest and soft delete/versioning according to policy.
4. Configure managed identity/service principal with Storage Blob Data Contributor for the pipeline.
5. Configure Databricks secret scope for any required credentials.
6. Mount/use ABFS paths; replace `/mnt/...` paths in metadata.
7. Enable GRS if required by the PDD.
8. Apply lifecycle policies for 7/5/3 year retention.
9. Configure Unity Catalog external locations and storage credentials.
10. Enable TLS and private networking according to enterprise policy.
