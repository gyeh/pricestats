CREATE INDEX IF NOT EXISTS stage_charge_rows_batch_idx
  ON ingest.stage_charge_rows (ingest_batch_id);

CREATE INDEX IF NOT EXISTS stage_charge_rows_file_idx
  ON ingest.stage_charge_rows (mrf_file_id);

CREATE UNIQUE INDEX IF NOT EXISTS stage_charge_rows_batch_rowhash_uq
  ON ingest.stage_charge_rows (ingest_batch_id, source_row_hash);
