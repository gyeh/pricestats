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
