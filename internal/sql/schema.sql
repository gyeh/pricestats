-- schema.sql
-- Composite schema for sqlc type-checking.
-- Concatenates migrations 001-007 + 009 (skipping 008 which uses PL/pgSQL).

-- 001_create_schemas.sql
CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS mrf;

-- 002_create_ref_hospitals.sql
CREATE TABLE IF NOT EXISTS ref.hospitals (
  hospital_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  hospital_name text   NOT NULL,
  hospital_location text,
  hospital_address  text,
  license_number    text,
  license_state     text,
  npi_list          text[],
  created_at        timestamptz NOT NULL DEFAULT now()
);

-- 003_create_ref_payers_plans.sql
CREATE TABLE IF NOT EXISTS ref.payers (
  payer_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payer_name      text   NOT NULL,
  payer_name_norm text   NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ref.plans (
  plan_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  payer_id       bigint REFERENCES ref.payers(payer_id),
  plan_name      text   NOT NULL,
  plan_name_norm text   NOT NULL,
  UNIQUE (payer_id, plan_name_norm)
);

-- 004_create_ingest_mrf_files.sql
CREATE TABLE IF NOT EXISTS ingest.mrf_files (
  mrf_file_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  hospital_id       bigint      NOT NULL REFERENCES ref.hospitals(hospital_id),
  source_file_name  text        NOT NULL,
  source_file_sha256 text       NOT NULL,
  version           text,
  last_updated_on   date,
  affirmation       boolean,
  file_size_bytes   bigint,
  status            text        NOT NULL DEFAULT 'pending',
  imported_at       timestamptz NOT NULL DEFAULT now(),
  is_active         boolean     NOT NULL DEFAULT false,
  UNIQUE (hospital_id, source_file_sha256)
);

-- 005_create_ingest_stage.sql
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

-- 006_create_mrf_prices_by_code.sql
CREATE TABLE IF NOT EXISTS mrf.prices_by_code (
  price_row_id     bigint GENERATED ALWAYS AS IDENTITY,
  mrf_file_id      bigint      NOT NULL,
  hospital_id      bigint      NOT NULL,

  code_type        text        NOT NULL,
  code_raw         text        NOT NULL,
  code_norm        text        NOT NULL,

  description      text        NOT NULL,
  setting          text,
  billing_class    text,

  payer_id         bigint,
  plan_id          bigint,
  payer_name_raw   text,
  plan_name_raw    text,

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
  additional_payer_notes   text,

  source_row_hash  bytea       NOT NULL,
  imported_at      timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (price_row_id, code_type)
) PARTITION BY LIST (code_type);

-- 007_create_partitions.sql
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_cpt
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('CPT');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_hcpcs
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('HCPCS');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_ms_drg
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('MS-DRG');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_ndc
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('NDC');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_rc
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('RC');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_icd
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('ICD');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_drg
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('DRG');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_cdm
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('CDM');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_local
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('LOCAL');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_apc
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('APC');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_eapg
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('EAPG');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_hipps
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('HIPPS');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_cdt
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('CDT');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_r_drg
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('R-DRG');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_s_drg
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('S-DRG');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_aps_drg
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('APS-DRG');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_ap_drg
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('AP-DRG');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_apr_drg
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('APR-DRG');
CREATE TABLE IF NOT EXISTS mrf.prices_by_code_tris_drg
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('TRIS-DRG');

-- 009_create_staging_indexes.sql
CREATE INDEX IF NOT EXISTS stage_charge_rows_batch_idx
  ON ingest.stage_charge_rows (ingest_batch_id);

CREATE INDEX IF NOT EXISTS stage_charge_rows_file_idx
  ON ingest.stage_charge_rows (mrf_file_id);

CREATE UNIQUE INDEX IF NOT EXISTS stage_charge_rows_batch_rowhash_uq
  ON ingest.stage_charge_rows (ingest_batch_id, source_row_hash);
