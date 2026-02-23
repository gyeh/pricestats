-- upsert_payers.sql
-- $1 = ingest_batch_id
INSERT INTO ref.payers (payer_name, payer_name_norm)
SELECT DISTINCT s.payer_name, s.payer_name_norm
FROM ingest.stage_charge_rows s
WHERE s.ingest_batch_id = $1
  AND s.payer_name_norm IS NOT NULL
ON CONFLICT (payer_name_norm) DO NOTHING;
