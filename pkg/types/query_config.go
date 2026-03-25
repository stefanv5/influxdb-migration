package types

import "time"

type QueryConfig struct {
	BatchSize  int
	TimeWindow time.Duration
}
