package parquetread

import (
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"

	"github.com/gyeh/pricestats/internal/model"
)

// Reader wraps a parquet GenericReader for streaming HospitalChargeRow records.
type Reader struct {
	file   *os.File
	reader *parquet.GenericReader[model.HospitalChargeRow]
}

// Open opens a Parquet file and returns a streaming Reader.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat parquet file: %w", err)
	}

	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("open parquet: %w", err)
	}

	r := parquet.NewGenericReader[model.HospitalChargeRow](pf)
	return &Reader{file: f, reader: r}, nil
}

// NumRows returns the total number of rows in the Parquet file.
func (r *Reader) NumRows() int64 {
	return r.reader.NumRows()
}

// Read reads up to len(rows) records into the provided slice.
// Returns the number of rows read and io.EOF when done.
func (r *Reader) Read(rows []model.HospitalChargeRow) (int, error) {
	n, err := r.reader.Read(rows)
	if err != nil && err != io.EOF {
		return n, fmt.Errorf("read parquet rows: %w", err)
	}
	return n, err
}

// Schema returns the Parquet schema for validation.
func (r *Reader) Schema() *parquet.Schema {
	return r.reader.Schema()
}

// Close releases all resources.
func (r *Reader) Close() error {
	if err := r.reader.Close(); err != nil {
		r.file.Close()
		return err
	}
	return r.file.Close()
}
