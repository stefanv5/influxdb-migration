package engine

import (
	"time"

	"github.com/migration-tools/influx-migrator/internal/adapter"
)

// TimeWindow represents a time range for processing
type TimeWindow struct {
	Start time.Time
	End   time.Time
}

// SplitTimeWindows splits a time range into windows of the given duration
func SplitTimeWindows(start, end time.Time, windowDuration time.Duration) []TimeWindow {
	if windowDuration <= 0 {
		// If window duration is 0 or negative, return single window spanning entire range
		return []TimeWindow{{Start: start, End: end}}
	}

	var windows []TimeWindow
	for windowStart := start; windowStart.Before(end); {
		windowEnd := windowStart.Add(windowDuration)
		if windowEnd.After(end) {
			windowEnd = end
		}
		windows = append(windows, TimeWindow{
			Start: windowStart,
			End:   windowEnd,
		})
		windowStart = windowEnd
	}
	return windows
}

// ShardGroupOverlaps returns true if shard group overlaps with query range
func ShardGroupOverlaps(sg *adapter.ShardGroup, queryStart, queryEnd time.Time) bool {
	return !queryEnd.Before(sg.StartTime) && !queryStart.After(sg.EndTime)
}

// ShardGroupEffectiveTimeRange returns the intersection of shard group and query time range
func ShardGroupEffectiveTimeRange(sg *adapter.ShardGroup, queryStart, queryEnd time.Time) (start, end time.Time) {
	start = queryStart
	end = queryEnd
	if start.Before(sg.StartTime) {
		start = sg.StartTime
	}
	if end.After(sg.EndTime) {
		end = sg.EndTime
	}
	return start, end
}