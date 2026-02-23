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
