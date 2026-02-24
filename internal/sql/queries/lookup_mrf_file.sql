-- name: LookupMRFFile :one
SELECT mrf_file_id, status FROM ingest.mrf_files
WHERE hospital_id = sqlc.arg(hospital_id)
  AND source_file_sha256 = sqlc.arg(source_file_sha256);
