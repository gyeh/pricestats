CREATE TABLE IF NOT EXISTS mrf.prices_by_code_cpt
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('CPT');

CREATE TABLE IF NOT EXISTS mrf.prices_by_code_hcpcs
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('HCPCS');

CREATE TABLE IF NOT EXISTS mrf.prices_by_code_ms_drg
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('MS-DRG');

CREATE TABLE IF NOT EXISTS mrf.prices_by_code_ndc
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('NDC');

CREATE TABLE IF NOT EXISTS mrf.prices_by_code_cdt
  PARTITION OF mrf.prices_by_code FOR VALUES IN ('CDT');
