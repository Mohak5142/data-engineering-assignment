# Pipeline Design

## Execution
00 setup -> 01 Bronze -> 02/03/04/05 Silver in parallel -> 06 Gold -> 07 Audit.

## Incremental strategy
Bronze uses batch IDs and record hashes. Source metadata tracks last successful load. Silver deduplicates by business key and uses SCD Type 2 for patients.

## Failure strategy
Source-level retries are driven by retry_count; invalid records are written to quarantine; successful independent sources continue; audit status is SUCCESS/PARTIAL/FAILED.

## Idempotency
Batch IDs, record hashes and Delta tables support replay. Production deployments should add a Delta MERGE condition on source file + record hash to guarantee duplicate-free replay.
