DO $$
DECLARE
  parts text[] := ARRAY[
    'cpt','hcpcs','ms_drg','ndc','rc','icd','drg','cdm','local', 'apc','eapg','hipps','cdt','r_drg','s_drg','aps_drg','ap_drg','apr_drg','tris_drg'
  ];
  p text;
BEGIN
  FOREACH p IN ARRAY parts LOOP
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS prices_by_code_%s_code_idx ON mrf.prices_by_code_%s (code_norm)',
      p, p
    );
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS prices_by_code_%s_code_hospital_idx ON mrf.prices_by_code_%s (code_norm, hospital_id)',
      p, p
    );
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS prices_by_code_%s_file_idx ON mrf.prices_by_code_%s (mrf_file_id)',
      p, p
    );
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS prices_by_code_%s_payer_idx ON mrf.prices_by_code_%s (payer_id)',
      p, p
    );
  END LOOP;
END
$$;
