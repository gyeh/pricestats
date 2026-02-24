-- name: UpdateMRFStatus :exec
UPDATE ingest.mrf_files
SET status = sqlc.arg(status)
WHERE mrf_file_id = sqlc.arg(mrf_file_id);
