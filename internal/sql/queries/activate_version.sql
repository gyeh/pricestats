-- activate_version.sql
-- $1 = mrf_file_id
UPDATE ingest.mrf_files
SET is_active = true, status = 'active'
WHERE mrf_file_id = $1;
