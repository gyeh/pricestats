package model

import "time"

// IngestSummary captures metrics from a single file ingest run.
type IngestSummary struct {
	FilePath            string
	FileSHA256          string
	MRFFileID           int64
	IngestBatchID       string
	RowsRead            int64
	RowsStaged          int64
	RowsRejected        int64
	RowsInsertedServing int64
	RowsExplodedByCode  map[string]int64
	DurationRead        time.Duration
	DurationCopy        time.Duration
	DurationTransform   time.Duration
	DurationFinalize    time.Duration
	DurationTotal       time.Duration
}
