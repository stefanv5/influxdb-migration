package types

import (
	"fmt"
	"time"
)

const (
	MinBatchSize     = 1
	MaxBatchSize     = 1000000
	DefaultBatchSize = 10000

	MinTimeWindow     = 1 * time.Hour
	MaxTimeWindow     = 30 * 24 * time.Hour
	DefaultTimeWindow = 168 * time.Hour
)

type QueryConfig struct {
	BatchSize  int
	TimeWindow time.Duration
}

func (c *QueryConfig) Validate() error {
	if c.BatchSize < MinBatchSize || c.BatchSize > MaxBatchSize {
		return fmt.Errorf("batch_size must be between %d and %d, got %d", MinBatchSize, MaxBatchSize, c.BatchSize)
	}

	if c.TimeWindow < MinTimeWindow || c.TimeWindow > MaxTimeWindow {
		return fmt.Errorf("time_window must be between %v and %v, got %v", MinTimeWindow, MaxTimeWindow, c.TimeWindow)
	}

	return nil
}

func (c *QueryConfig) ApplyDefaults() {
	if c.BatchSize == 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.TimeWindow == 0 {
		c.TimeWindow = DefaultTimeWindow
	}
}
