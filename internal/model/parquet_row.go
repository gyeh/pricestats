package model

// HospitalChargeRow mirrors the Parquet schema for a single charge line.
// Money fields are float64 matching Parquet representation; they get
// converted to integer cents during normalization.
type HospitalChargeRow struct {
	Description string  `parquet:"description"`
	Setting     string  `parquet:"setting"`

	// 19 CMS-defined code columns
	CPTCode     *string `parquet:"cpt_code,optional"`
	HCPCSCode   *string `parquet:"hcpcs_code,optional"`
	MSDRGCode   *string `parquet:"ms_drg_code,optional"`
	NDCCode     *string `parquet:"ndc_code,optional"`
	RCCode      *string `parquet:"rc_code,optional"`
	ICDCode     *string `parquet:"icd_code,optional"`
	DRGCode     *string `parquet:"drg_code,optional"`
	CDMCode     *string `parquet:"cdm_code,optional"`
	LOCALCode   *string `parquet:"local_code,optional"`
	APCCode     *string `parquet:"apc_code,optional"`
	EAPGCode    *string `parquet:"eapg_code,optional"`
	HIPPSCode   *string `parquet:"hipps_code,optional"`
	CDTCode     *string `parquet:"cdt_code,optional"`
	RDRGCode    *string `parquet:"r_drg_code,optional"`
	SDRGCode    *string `parquet:"s_drg_code,optional"`
	APSDRGCode  *string `parquet:"aps_drg_code,optional"`
	APDRGCode   *string `parquet:"ap_drg_code,optional"`
	APRDRGCode  *string `parquet:"apr_drg_code,optional"`
	TRISDRGCode *string `parquet:"tris_drg_code,optional"`

	// Payer identification
	PayerName *string `parquet:"payer_name,optional"`
	PlanName  *string `parquet:"plan_name,optional"`

	// Charge amounts (float64 from Parquet, converted to cents in normalize)
	GrossCharge          *float64 `parquet:"gross_charge,optional"`
	DiscountedCash       *float64 `parquet:"discounted_cash,optional"`
	NegotiatedDollar     *float64 `parquet:"negotiated_dollar,optional"`
	NegotiatedPercentage *float64 `parquet:"negotiated_percentage,optional"`
	NegotiatedAlgorithm  *string  `parquet:"negotiated_algorithm,optional"`
	EstimatedAmount      *float64 `parquet:"estimated_amount,optional"`
	MinCharge            *float64 `parquet:"min_charge,optional"`
	MaxCharge            *float64 `parquet:"max_charge,optional"`
	Methodology          *string  `parquet:"methodology,optional"`

	// Drug info
	DrugUnitOfMeasurement  *float64 `parquet:"drug_unit_of_measurement,optional"`
	DrugTypeOfMeasurement  *string  `parquet:"drug_type_of_measurement,optional"`

	// Modifiers & notes
	Modifiers              *string `parquet:"modifiers,optional"`
	AdditionalGenericNotes *string `parquet:"additional_generic_notes,optional"`
	AdditionalPayerNotes   *string `parquet:"additional_payer_notes,optional"`

	// Optional fields (v2.1+)
	BillingClass              *string `parquet:"billing_class,optional"`
	FinancialAidPolicy        *string `parquet:"financial_aid_policy,optional"`
	GeneralContractProvisions *string `parquet:"general_contract_provisions,optional"`

	// Hospital metadata
	HospitalName     string  `parquet:"hospital_name"`
	LastUpdatedOn    string  `parquet:"last_updated_on"`
	Version          string  `parquet:"version"`
	HospitalLocation string  `parquet:"hospital_location"`
	HospitalAddress  string  `parquet:"hospital_address"`
	LicenseNumber    *string `parquet:"license_number,optional"`
	LicenseState     *string `parquet:"license_state,optional"`
	Affirmation      bool    `parquet:"affirmation"`
}

// CodeValue returns a map of code_type_name -> *string for all 19 code columns.
func (r *HospitalChargeRow) CodeValues() map[string]*string {
	return map[string]*string{
		"CPT":      r.CPTCode,
		"HCPCS":    r.HCPCSCode,
		"MS-DRG":   r.MSDRGCode,
		"NDC":      r.NDCCode,
		"RC":       r.RCCode,
		"ICD":      r.ICDCode,
		"DRG":      r.DRGCode,
		"CDM":      r.CDMCode,
		"LOCAL":    r.LOCALCode,
		"APC":      r.APCCode,
		"EAPG":     r.EAPGCode,
		"HIPPS":    r.HIPPSCode,
		"CDT":      r.CDTCode,
		"R-DRG":    r.RDRGCode,
		"S-DRG":    r.SDRGCode,
		"APS-DRG":  r.APSDRGCode,
		"AP-DRG":   r.APDRGCode,
		"APR-DRG":  r.APRDRGCode,
		"TRIS-DRG": r.TRISDRGCode,
	}
}
