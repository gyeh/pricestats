package db

import (
	"github.com/gyeh/pricestats/internal/model"
	"github.com/jackc/pgx/v5"
)

// ChannelSource implements pgx.CopyFromSource by reading StagingRows from a channel.
// This provides natural backpressure between the Parquet reader and COPY writer.
type ChannelSource struct {
	ch      <-chan *model.StagingRow
	current *model.StagingRow
	err     error
}

// NewChannelSource creates a CopyFromSource backed by a channel.
func NewChannelSource(ch <-chan *model.StagingRow) *ChannelSource {
	return &ChannelSource{ch: ch}
}

// Next advances to the next row. Returns false when the channel is closed.
func (s *ChannelSource) Next() bool {
	row, ok := <-s.ch
	if !ok {
		return false
	}
	s.current = row
	return true
}

// Values returns the current row's values in COPY column order.
func (s *ChannelSource) Values() ([]any, error) {
	return s.current.CopyValues(), nil
}

// Err returns any error encountered during iteration.
func (s *ChannelSource) Err() error {
	return s.err
}

// Compile-time check that ChannelSource satisfies the interface.
var _ pgx.CopyFromSource = (*ChannelSource)(nil)
