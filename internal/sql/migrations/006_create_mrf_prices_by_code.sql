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
