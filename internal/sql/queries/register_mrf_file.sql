-- register_mrf_file.sql
-- Registers a new MRF file or returns existing if already imported.
-- $1 = hospital_id, $2 = source_file_name, $3 = source_file_sha256,
-- $4 = version, $5 = last_updated_on, $6 = affirmation, $7 = file_size_bytes
INSERT INTO ingest.mrf_files (hospital_id, source_file_name, source_file_sha256, version, last_updated_on, affirmation, file_size_bytes)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (hospital_id, source_file_sha256) DO NOTHING
RETURNING mrf_file_id, status;
