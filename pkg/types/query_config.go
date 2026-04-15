package types

import (
	"fmt"
	"time"
)

const (
	MinBatchSize     = 1
	MaxBatchSize     = 1000000
	DefaultBatchSize = 10000

	MinSeriesPerQuery     = 1
	MaxSeriesPerQuery     = 1000
	DefaultSeriesPerQuery = 100

	MinTimeWindow     = 1 * time.Hour
	MaxTimeWindow     = 30 * 24 * time.Hour
	DefaultTimeWindow = 168 * time.Hour
)

type QueryConfig struct {
	BatchSize  int
	TimeWindow time.Duration
	MaxSeriesPerQuery int  // 0 means use default (100)
}

func (c *QueryConfig) Validate() error {
	if c.BatchSize < MinBatchSize || c.BatchSize > MaxBatchSize {
		return fmt.Errorf("batch_size must be between %d and %d, got %d", MinBatchSize, MaxBatchSize, c.BatchSize)
	}

	if c.TimeWindow < MinTimeWindow || c.TimeWindow > MaxTimeWindow {
		return fmt.Errorf("time_window must be between %v and %v, got %v", MinTimeWindow, MaxTimeWindow, c.TimeWindow)
	}

	if c.MaxSeriesPerQuery != 0 && (c.MaxSeriesPerQuery < MinSeriesPerQuery || c.MaxSeriesPerQuery > MaxSeriesPerQuery) {
		return fmt.Errorf("max_series_per_query must be between %d and %d, got %d", MinSeriesPerQuery, MaxSeriesPerQuery, c.MaxSeriesPerQuery)
	}

	return nil
}

func (c *QueryConfig) WithDefaults() *QueryConfig {
	cfg := *c // shallow copy
	if cfg.BatchSize == 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.TimeWindow == 0 {
		cfg.TimeWindow = DefaultTimeWindow
	}
	if cfg.MaxSeriesPerQuery == 0 {
		cfg.MaxSeriesPerQuery = DefaultSeriesPerQuery
	}
	return &cfg
}

// ApplyDefaults applies defaults to the receiver (mutating) for backwards compatibility.
// Prefer WithDefaults() for immutable patterns.
func (c *QueryConfig) ApplyDefaults() {
	if c.BatchSize == 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.TimeWindow == 0 {
		c.TimeWindow = DefaultTimeWindow
	}
	if c.MaxSeriesPerQuery == 0 {
		c.MaxSeriesPerQuery = DefaultSeriesPerQuery
	}
}
