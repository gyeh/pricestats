package normalize

import (
	"github.com/google/uuid"

	"github.com/gyeh/pricestats/internal/model"
)

// ToStagingRow converts a Parquet-read HospitalChargeRow into a normalized StagingRow.
// When includePayerPrices is false, payer/plan names and negotiated price fields are nulled out.
func ToStagingRow(row *model.HospitalChargeRow, batchID uuid.UUID, mRFFileID int64, rowNum int64, includePayerPrices bool) (*model.StagingRow, error) {
	s := &model.StagingRow{
		IngestBatchID:   batchID,
		MRFFileID:       mRFFileID,
		SourceRowNumber: rowNum,

		HospitalName:     row.HospitalName,
		HospitalLocation: optStr(row.HospitalLocation),
		HospitalAddress:  optStr(row.HospitalAddress),
		LicenseNumber:    row.LicenseNumber,
		LicenseState:     row.LicenseState,
		Version:          optStr(row.Version),
		LastUpdatedOn:    ParseDate(row.LastUpdatedOn),
		Affirmation:      row.Affirmation,

		Description:  row.Description,
		Setting:      optStr(row.Setting),
		BillingClass: row.BillingClass,

		// Normalize code columns
		CPTCode:   NormalizeCode(row.CPTCode),
		HCPCSCode: NormalizeCode(row.HCPCSCode),
		MSDRGCode: NormalizeCode(row.MSDRGCode),
		NDCCode:   NormalizeCode(row.NDCCode),
		CDTCode:   NormalizeCode(row.CDTCode),

		// Hospital-level charges (always included)
		GrossChargeCents:    DollarsToCents(row.GrossCharge),
		DiscountedCashCents: DollarsToCents(row.DiscountedCash),
		MinChargeCents:      DollarsToCents(row.MinCharge),
		MaxChargeCents:      DollarsToCents(row.MaxCharge),

		DrugUnit:     row.DrugUnitOfMeasurement,
		DrugUnitType: row.DrugTypeOfMeasurement,

		Modifiers:              row.Modifiers,
		AdditionalGenericNotes: row.AdditionalGenericNotes,
	}

	// Payer-specific fields: only populated when --include-payer-prices is set
	if includePayerPrices {
		s.PayerName = row.PayerName
		s.PayerNameNorm = NormalizeName(row.PayerName)
		s.PlanName = row.PlanName
		s.PlanNameNorm = NormalizeName(row.PlanName)
		s.NegotiatedDollarCents = DollarsToCents(row.NegotiatedDollar)
		s.NegotiatedPercentageBPS = PercentToBasisPoints(row.NegotiatedPercentage)
		s.EstimatedAmountCents = DollarsToCents(row.EstimatedAmount)
		s.Methodology = row.Methodology
		s.NegotiatedAlgorithm = row.NegotiatedAlgorithm
		s.AdditionalPayerNotes = row.AdditionalPayerNotes
	}

	// Compute row hash from key identifying fields
	s.SourceRowHash = RowHashFromValues(rowNum,
		row.Description,
		row.Setting,
		derefStr(row.PayerName),
		derefStr(row.PlanName),
		derefStr(row.CPTCode),
		derefStr(row.HCPCSCode),
		derefStr(row.MSDRGCode),
		derefStr(row.NDCCode),
	)

	return s, nil
}

func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
