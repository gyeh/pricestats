-- transform_wide_to_long.sql
-- Explodes wide code columns into long rows in the serving table.
-- $1 = ingest_batch_id
INSERT INTO mrf.prices_by_code (
  mrf_file_id,
  hospital_id,
  code_type,
  code_raw,
  code_norm,
  description,
  setting,
  billing_class,
  payer_id,
  plan_id,
  payer_name_raw,
  plan_name_raw,
  gross_charge_cents,
  discounted_cash_cents,
  negotiated_dollar_cents,
  negotiated_percentage_bps,
  estimated_amount_cents,
  min_charge_cents,
  max_charge_cents,
  methodology,
  negotiated_algorithm,
  drug_unit,
  drug_unit_type,
  modifiers,
  additional_generic_notes,
  additional_payer_notes,
  source_row_hash
)
SELECT
  s.mrf_file_id,
  f.hospital_id,
  c.code_type,
  c.code_raw,
  upper(regexp_replace(c.code_raw, '[^A-Za-z0-9]', '', 'g')) AS code_norm,
  s.description,
  s.setting,
  s.billing_class,
  p.payer_id,
  pl.plan_id,
  s.payer_name,
  s.plan_name,
  s.gross_charge_cents,
  s.discounted_cash_cents,
  s.negotiated_dollar_cents,
  s.negotiated_percentage_bps,
  s.estimated_amount_cents,
  s.min_charge_cents,
  s.max_charge_cents,
  s.methodology,
  s.negotiated_algorithm,
  s.drug_unit,
  s.drug_unit_type,
  s.modifiers,
  s.additional_generic_notes,
  s.additional_payer_notes,
  s.source_row_hash
FROM ingest.stage_charge_rows s
JOIN ingest.mrf_files f ON f.mrf_file_id = s.mrf_file_id
LEFT JOIN ref.payers p ON p.payer_name_norm = s.payer_name_norm
LEFT JOIN ref.plans pl
  ON pl.payer_id = p.payer_id
 AND pl.plan_name_norm = s.plan_name_norm
CROSS JOIN LATERAL (
  VALUES
    ('CPT',      s.cpt_code),
    ('HCPCS',    s.hcpcs_code),
    ('MS-DRG',   s.ms_drg_code),
    ('NDC',      s.ndc_code),
    ('RC',       s.rc_code),
    ('ICD',      s.icd_code),
    ('DRG',      s.drg_code),
    ('CDM',      s.cdm_code),
    ('LOCAL',    s.local_code),
    ('APC',      s.apc_code),
    ('EAPG',     s.eapg_code),
    ('HIPPS',    s.hipps_code),
    ('CDT',      s.cdt_code),
    ('R-DRG',    s.r_drg_code),
    ('S-DRG',    s.s_drg_code),
    ('APS-DRG',  s.aps_drg_code),
    ('AP-DRG',   s.ap_drg_code),
    ('APR-DRG',  s.apr_drg_code),
    ('TRIS-DRG', s.tris_drg_code)
) AS c(code_type, code_raw)
WHERE s.ingest_batch_id = $1
  AND c.code_raw IS NOT NULL
  AND c.code_raw <> '';
