package model

// CodeType represents one of the supported CMS-defined billing code types.
type CodeType struct {
	Name       string // e.g. "CPT"
	Column     string // staging/parquet column name, e.g. "cpt_code"
	Partition  string // partition table suffix, e.g. "cpt"
}

// AllCodeTypes lists the supported CMS-defined code types in canonical order.
var AllCodeTypes = []CodeType{
	{Name: "CPT", Column: "cpt_code", Partition: "cpt"},
	{Name: "HCPCS", Column: "hcpcs_code", Partition: "hcpcs"},
	{Name: "MS-DRG", Column: "ms_drg_code", Partition: "ms_drg"},
	{Name: "NDC", Column: "ndc_code", Partition: "ndc"},
	{Name: "CDT", Column: "cdt_code", Partition: "cdt"},
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
