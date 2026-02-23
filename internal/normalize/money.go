package normalize

import "math"

// DollarsToCents converts a nullable float64 dollar amount to nullable int64 cents.
// Uses math.Round to avoid truncation bias.
func DollarsToCents(v *float64) *int64 {
	if v == nil {
		return nil
	}
	c := int64(math.Round(*v * 100))
	return &c
}

// PercentToBasisPoints converts a nullable float64 percentage to nullable int32 basis points.
// e.g. 12.34% → 1234 bps.
func PercentToBasisPoints(v *float64) *int32 {
	if v == nil {
		return nil
	}
	bp := int32(math.Round(*v * 100))
	return &bp
}
