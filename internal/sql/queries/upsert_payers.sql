-- name: UpsertPayers :execresult
INSERT INTO ref.payers (payer_name, payer_name_norm)
SELECT DISTINCT s.payer_name, s.payer_name_norm
FROM ingest.stage_charge_rows s
WHERE s.ingest_batch_id = sqlc.arg(ingest_batch_id)
  AND s.payer_name_norm IS NOT NULL
ON CONFLICT (payer_name_norm) DO NOTHING;
