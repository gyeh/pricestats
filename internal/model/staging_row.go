package model

import (
	"time"

	"github.com/google/uuid"
)

// StagingRow is the normalized, DB-ready representation of a single charge line.
// Money values are stored as int64 cents; percentages as int32 basis points.
type StagingRow struct {
	IngestBatchID uuid.UUID
	MRFFileID     int64

	SourceRowNumber int64
	SourceRowHash   []byte

	// Hospital metadata
	HospitalName     string
	HospitalLocation *string
	HospitalAddress  *string
	LicenseNumber    *string
	LicenseState     *string
	Version          *string
	LastUpdatedOn    *time.Time
	Affirmation      bool

	// Service attrs
	Description  string
	Setting      *string
	BillingClass *string

	// Wide code columns (normalized)
	CPTCode     *string
	HCPCSCode   *string
	MSDRGCode   *string
	NDCCode     *string
	RCCode      *string
	ICDCode     *string
	DRGCode     *string
	CDMCode     *string
	LOCALCode   *string
	APCCode     *string
	EAPGCode    *string
	HIPPSCode   *string
	CDTCode     *string
	RDRGCode    *string
	SDRGCode    *string
	APSDRGCode  *string
	APDRGCode   *string
	APRDRGCode  *string
	TRISDRGCode *string

	// Payer / plan
	PayerName     *string
	PayerNameNorm *string
	PlanName      *string
	PlanNameNorm  *string

	// Charges as fixed-point
	GrossChargeCents        *int64
	DiscountedCashCents     *int64
	NegotiatedDollarCents   *int64
	NegotiatedPercentageBPS *int32
	EstimatedAmountCents    *int64
	MinChargeCents          *int64
	MaxChargeCents          *int64

	Methodology         *string
	NegotiatedAlgorithm *string

	DrugUnit     *float64
	DrugUnitType *string

	Modifiers              *string
	AdditionalGenericNotes *string
	AdditionalPayerNotes   *string
}

// StagingColumns returns the ordered column names for COPY into ingest.stage_charge_rows.
func StagingColumns() []string {
	return []string{
		"ingest_batch_id",
		"mrf_file_id",
		"source_row_number",
		"source_row_hash",
		"hospital_name",
		"hospital_location",
		"hospital_address",
		"license_number",
		"license_state",
		"version",
		"last_updated_on",
		"affirmation",
		"description",
		"setting",
		"billing_class",
		"cpt_code",
		"hcpcs_code",
		"ms_drg_code",
		"ndc_code",
		"rc_code",
		"icd_code",
		"drg_code",
		"cdm_code",
		"local_code",
		"apc_code",
		"eapg_code",
		"hipps_code",
		"cdt_code",
		"r_drg_code",
		"s_drg_code",
		"aps_drg_code",
		"ap_drg_code",
		"apr_drg_code",
		"tris_drg_code",
		"payer_name",
		"payer_name_norm",
		"plan_name",
		"plan_name_norm",
		"gross_charge_cents",
		"discounted_cash_cents",
		"negotiated_dollar_cents",
		"negotiated_percentage_bps",
		"estimated_amount_cents",
		"min_charge_cents",
		"max_charge_cents",
		"methodology",
		"negotiated_algorithm",
		"drug_unit",
		"drug_unit_type",
		"modifiers",
		"additional_generic_notes",
		"additional_payer_notes",
	}
}

// CopyValues returns the row values in the same order as StagingColumns(),
// suitable for pgx CopyFromSource.
func (r *StagingRow) CopyValues() []any {
	return []any{
		r.IngestBatchID,
		r.MRFFileID,
		r.SourceRowNumber,
		r.SourceRowHash,
		r.HospitalName,
		r.HospitalLocation,
		r.HospitalAddress,
		r.LicenseNumber,
		r.LicenseState,
		r.Version,
		r.LastUpdatedOn,
		r.Affirmation,
		r.Description,
		r.Setting,
		r.BillingClass,
		r.CPTCode,
		r.HCPCSCode,
		r.MSDRGCode,
		r.NDCCode,
		r.RCCode,
		r.ICDCode,
		r.DRGCode,
		r.CDMCode,
		r.LOCALCode,
		r.APCCode,
		r.EAPGCode,
		r.HIPPSCode,
		r.CDTCode,
		r.RDRGCode,
		r.SDRGCode,
		r.APSDRGCode,
		r.APDRGCode,
		r.APRDRGCode,
		r.TRISDRGCode,
		r.PayerName,
		r.PayerNameNorm,
		r.PlanName,
		r.PlanNameNorm,
		r.GrossChargeCents,
		r.DiscountedCashCents,
		r.NegotiatedDollarCents,
		r.NegotiatedPercentageBPS,
		r.EstimatedAmountCents,
		r.MinChargeCents,
		r.MaxChargeCents,
		r.Methodology,
		r.NegotiatedAlgorithm,
		r.DrugUnit,
		r.DrugUnitType,
		r.Modifiers,
		r.AdditionalGenericNotes,
		r.AdditionalPayerNotes,
	}
}
