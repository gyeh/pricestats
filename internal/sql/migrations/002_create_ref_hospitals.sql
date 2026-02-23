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
