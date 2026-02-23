package ingest

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/gyeh/pricestats/internal/db"
	"github.com/gyeh/pricestats/internal/model"
	"github.com/gyeh/pricestats/internal/normalize"
	"github.com/gyeh/pricestats/internal/parquetread"
)

const readBatchSize = 1024

// StageResult holds metrics from the staging phase.
type StageResult struct {
	RowsRead     int64
	RowsStaged   int64
	RowsRejected int64
	Duration     time.Duration
}

// Stage streams rows from the Parquet file, normalizes them, and COPY-loads
// them into the staging table via a channel-backed CopyFromSource.
func Stage(ctx context.Context, pool *pgxpool.Pool, log zerolog.Logger, pf *PreflightResult, includePayerPrices bool) (*StageResult, error) {
	start := time.Now()

	reader, err := parquetread.Open(pf.FilePath)
	if err != nil {
		return nil, fmt.Errorf("stage open: %w", err)
	}
	defer reader.Close()

	ch := make(chan *model.StagingRow, readBatchSize)
	errCh := make(chan error, 1)

	var rowsRead, rowsRejected int64

	// Producer goroutine: read Parquet → normalize → push to channel
	go func() {
		defer close(ch)
		buf := make([]model.HospitalChargeRow, readBatchSize)
		var rowNum int64

		for {
			n, readErr := reader.Read(buf)
			for i := 0; i < n; i++ {
				rowNum++
				rowsRead++

				staging, normErr := normalize.ToStagingRow(&buf[i], pf.IngestBatchID, pf.MRFFileID, rowNum, includePayerPrices)
				if normErr != nil {
					rowsRejected++
					log.Warn().Err(normErr).Int64("row", rowNum).Msg("row rejected")
					continue
				}

				select {
				case ch <- staging:
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				errCh <- fmt.Errorf("read parquet at row %d: %w", rowNum, readErr)
				return
			}
		}
		errCh <- nil
	}()

	// Consumer: COPY from channel into staging table
	source := db.NewChannelSource(ch)
	rowsStaged, err := pool.CopyFrom(ctx,
		pgx.Identifier{"ingest", "stage_charge_rows"},
		model.StagingColumns(),
		source,
	)

	// Wait for producer to finish
	prodErr := <-errCh
	if prodErr != nil {
		return nil, fmt.Errorf("stage producer: %w", prodErr)
	}
	if err != nil {
		return nil, fmt.Errorf("stage copy: %w", err)
	}

	dur := time.Since(start)
	log.Info().
		Int64("rows_read", rowsRead).
		Int64("rows_staged", rowsStaged).
		Int64("rows_rejected", rowsRejected).
		Str("duration", dur.String()).
		Float64("rows_per_sec", float64(rowsStaged)/dur.Seconds()).
		Msg("staging complete")

	return &StageResult{
		RowsRead:     rowsRead,
		RowsStaged:   rowsStaged,
		RowsRejected: rowsRejected,
		Duration:     dur,
	}, nil
}

// UpdateStatus updates the mrf_file status.
func UpdateStatus(ctx context.Context, pool *pgxpool.Pool, mRFFileID int64, status string) error {
	_, err := pool.Exec(ctx,
		"UPDATE ingest.mrf_files SET status = $2 WHERE mrf_file_id = $1",
		mRFFileID, status,
	)
	return err
}

// DeleteStagingBatch deletes staging rows for a specific batch (cleanup failed runs).
func DeleteStagingBatch(ctx context.Context, pool *pgxpool.Pool, batchID uuid.UUID) error {
	_, err := pool.Exec(ctx,
		"DELETE FROM ingest.stage_charge_rows WHERE ingest_batch_id = $1",
		batchID,
	)
	return err
}
