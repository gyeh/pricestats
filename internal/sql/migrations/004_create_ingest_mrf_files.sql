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
