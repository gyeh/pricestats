CREATE UNLOGGED TABLE IF NOT EXISTS ingest.stage_charge_rows (
  ingest_batch_id    uuid   NOT NULL,
  mrf_file_id        bigint NOT NULL REFERENCES ingest.mrf_files(mrf_file_id),

  source_row_number  bigint NOT NULL,
  source_row_hash    bytea  NOT NULL,

  hospital_name      text,
  hospital_location  text,
  hospital_address   text,
  license_number     text,
  license_state      text,
  version            text,
  last_updated_on    date,
  affirmation        boolean,

  description        text NOT NULL,
  setting            text,
  billing_class      text,

  cpt_code           text,
  hcpcs_code         text,
  ms_drg_code        text,
  ndc_code           text,
  rc_code            text,
  icd_code           text,
  drg_code           text,
  cdm_code           text,
  local_code         text,
  apc_code           text,
  eapg_code          text,
  hipps_code         text,
  cdt_code           text,
  r_drg_code         text,
  s_drg_code         text,
  aps_drg_code       text,
  ap_drg_code        text,
  apr_drg_code       text,
  tris_drg_code      text,

  payer_name         text,
  payer_name_norm    text,
  plan_name          text,
  plan_name_norm     text,

  gross_charge_cents        bigint,
  discounted_cash_cents     bigint,
  negotiated_dollar_cents   bigint,
  negotiated_percentage_bps integer,
  estimated_amount_cents    bigint,
  min_charge_cents          bigint,
  max_charge_cents          bigint,

  methodology            text,
  negotiated_algorithm   text,

  drug_unit              numeric(18,6),
  drug_unit_type         text,

  modifiers              text,
  additional_generic_notes text,
  additional_payer_notes   text
);
