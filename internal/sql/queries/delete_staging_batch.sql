-- delete_staging_batch.sql
-- $1 = ingest_batch_id
DELETE FROM ingest.stage_charge_rows
WHERE ingest_batch_id = $1;
