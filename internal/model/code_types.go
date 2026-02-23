package model

// CodeType represents one of the 19 CMS-defined billing code types.
type CodeType struct {
	Name       string // e.g. "CPT"
	Column     string // staging/parquet column name, e.g. "cpt_code"
	Partition  string // partition table suffix, e.g. "cpt"
}

// AllCodeTypes lists the 19 CMS-defined code types in canonical order.
var AllCodeTypes = []CodeType{
	{Name: "CPT", Column: "cpt_code", Partition: "cpt"},
	{Name: "HCPCS", Column: "hcpcs_code", Partition: "hcpcs"},
	{Name: "MS-DRG", Column: "ms_drg_code", Partition: "ms_drg"},
	{Name: "NDC", Column: "ndc_code", Partition: "ndc"},
	{Name: "RC", Column: "rc_code", Partition: "rc"},
	{Name: "ICD", Column: "icd_code", Partition: "icd"},
	{Name: "DRG", Column: "drg_code", Partition: "drg"},
	{Name: "CDM", Column: "cdm_code", Partition: "cdm"},
	{Name: "LOCAL", Column: "local_code", Partition: "local"},
	{Name: "APC", Column: "apc_code", Partition: "apc"},
	{Name: "EAPG", Column: "eapg_code", Partition: "eapg"},
	{Name: "HIPPS", Column: "hipps_code", Partition: "hipps"},
	{Name: "CDT", Column: "cdt_code", Partition: "cdt"},
	{Name: "R-DRG", Column: "r_drg_code", Partition: "r_drg"},
	{Name: "S-DRG", Column: "s_drg_code", Partition: "s_drg"},
	{Name: "APS-DRG", Column: "aps_drg_code", Partition: "aps_drg"},
	{Name: "AP-DRG", Column: "ap_drg_code", Partition: "ap_drg"},
	{Name: "APR-DRG", Column: "apr_drg_code", Partition: "apr_drg"},
	{Name: "TRIS-DRG", Column: "tris_drg_code", Partition: "tris_drg"},
}

// CodeTypeColumns returns just the column names for all code types.
func CodeTypeColumns() []string {
	cols := make([]string, len(AllCodeTypes))
	for i, ct := range AllCodeTypes {
		cols[i] = ct.Column
	}
	return cols
}

// CodeTypeByName returns the CodeType for the given name, or ok=false.
func CodeTypeByName(name string) (CodeType, bool) {
	for _, ct := range AllCodeTypes {
		if ct.Name == name {
			return ct, true
		}
	}
	return CodeType{}, false
}
