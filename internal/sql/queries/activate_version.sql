-- name: ActivateVersion :exec
UPDATE ingest.mrf_files
SET is_active = true, status = 'active'
WHERE mrf_file_id = sqlc.arg(mrf_file_id);
