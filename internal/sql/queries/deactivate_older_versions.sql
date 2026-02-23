-- deactivate_older_versions.sql
-- Deactivates all older MRF files for the same hospital.
-- $1 = hospital_id, $2 = mrf_file_id (the one to keep active)
UPDATE ingest.mrf_files
SET is_active = false
WHERE hospital_id = $1
  AND mrf_file_id <> $2
  AND is_active = true;
