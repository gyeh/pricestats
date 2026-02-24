-- name: RegisterMRFFile :one
INSERT INTO ingest.mrf_files (hospital_id, source_file_name, source_file_sha256, version, last_updated_on, affirmation, file_size_bytes)
VALUES (sqlc.arg(hospital_id), sqlc.arg(source_file_name), sqlc.arg(source_file_sha256), sqlc.arg(version), sqlc.arg(last_updated_on), sqlc.arg(affirmation), sqlc.arg(file_size_bytes))
ON CONFLICT (hospital_id, source_file_sha256) DO NOTHING
RETURNING mrf_file_id, status;
