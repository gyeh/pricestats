-- name: DeleteStagingBatch :execresult
DELETE FROM ingest.stage_charge_rows
WHERE ingest_batch_id = sqlc.arg(ingest_batch_id);
