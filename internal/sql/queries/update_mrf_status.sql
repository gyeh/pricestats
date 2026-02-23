-- update_mrf_status.sql
-- $1 = mrf_file_id, $2 = new status
UPDATE ingest.mrf_files
SET status = $2
WHERE mrf_file_id = $1;
