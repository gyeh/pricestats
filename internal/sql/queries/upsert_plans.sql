-- name: UpsertPlans :execresult
INSERT INTO ref.plans (payer_id, plan_name, plan_name_norm)
SELECT DISTINCT p.payer_id, s.plan_name, s.plan_name_norm
FROM ingest.stage_charge_rows s
JOIN ref.payers p ON p.payer_name_norm = s.payer_name_norm
WHERE s.ingest_batch_id = sqlc.arg(ingest_batch_id)
  AND s.plan_name_norm IS NOT NULL
ON CONFLICT (payer_id, plan_name_norm) DO NOTHING;
