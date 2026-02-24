-- name: DeleteStagingByFile :exec
DELETE FROM ingest.stage_charge_rows WHERE mrf_file_id = sqlc.arg(mrf_file_id);
