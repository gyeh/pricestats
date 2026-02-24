-- name: DeactivateOlderVersions :execresult
UPDATE ingest.mrf_files
SET is_active = false
WHERE hospital_id = sqlc.arg(hospital_id)
  AND mrf_file_id <> sqlc.arg(mrf_file_id)
  AND is_active = true;
