package engine

import (
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/internal/adapter"
)

func TestSplitTimeWindows(t *testing.T) {
	tests := []struct {
		name            string
		start           time.Time
		end             time.Time
		windowDuration  time.Duration
		expectedWindows int
	}{
		{
			name:            "zero duration returns single window",
			start:           time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:             time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			windowDuration:  0,
			expectedWindows: 1,
		},
		{
			name:            "24h window on 48h range",
			start:           time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:             time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			windowDuration:  24 * time.Hour,
			expectedWindows: 2,
		},
		{
			name:            "window larger than range",
			start:           time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:             time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			windowDuration:  48 * time.Hour,
			expectedWindows: 1,
		},
		{
			name:            "exact fit",
			start:           time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			end:             time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
			windowDuration:  24 * time.Hour,
			expectedWindows: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows := SplitTimeWindows(tt.start, tt.end, tt.windowDuration)
			if len(windows) != tt.expectedWindows {
				t.Errorf("expected %d windows, got %d", tt.expectedWindows, len(windows))
			}
			// Verify continuity
			for i, w := range windows {
				if i > 0 {
					if !w.Start.Equal(windows[i-1].End) {
						t.Errorf("window %d does not start where previous ended", i)
					}
				}
				if !w.End.After(w.Start) {
					t.Errorf("window %d has end before start", i)
				}
			}
		})
	}
}

func TestShardGroupOverlaps(t *testing.T) {
	sg := &adapter.ShardGroup{
		ID:        1,
		StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name       string
		queryStart time.Time
		queryEnd   time.Time
		expected   bool
	}{
		{
			name:       "full overlap",
			queryStart: time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
			queryEnd:   time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			expected:   true,
		},
		{
			name:       "partial overlap",
			queryStart: time.Date(2023, 12, 15, 0, 0, 0, 0, time.UTC),
			queryEnd:   time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
			expected:   true,
		},
		{
			name:       "no overlap before",
			queryStart: time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC),
			queryEnd:   time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC),
			expected:   false,
		},
		{
			name:       "no overlap after",
			queryStart: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			queryEnd:   time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			expected:   false,
		},
		{
			name:       "exact match",
			queryStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			queryEnd:   time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
			expected:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShardGroupOverlaps(sg, tt.queryStart, tt.queryEnd)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestShardGroupEffectiveTimeRange(t *testing.T) {
	sg := &adapter.ShardGroup{
		ID:        1,
		StartTime: time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name            string
		queryStart      time.Time
		queryEnd        time.Time
		expectedStart   time.Time
		expectedEnd     time.Time
	}{
		{
			name:            "query before shard",
			queryStart:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			queryEnd:        time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
			expectedStart:   time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
			expectedEnd:     time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
		},
		{
			name:            "query after shard",
			queryStart:      time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
			queryEnd:        time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC),
			expectedStart:   time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
			expectedEnd:     time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:            "query spans shard",
			queryStart:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			queryEnd:        time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC),
			expectedStart:   time.Date(2024, 1, 5, 0, 0, 0, 0, time.UTC),
			expectedEnd:     time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:            "query within shard",
			queryStart:      time.Date(2024, 1, 7, 0, 0, 0, 0, time.UTC),
			queryEnd:        time.Date(2024, 1, 12, 0, 0, 0, 0, time.UTC),
			expectedStart:   time.Date(2024, 1, 7, 0, 0, 0, 0, time.UTC),
			expectedEnd:     time.Date(2024, 1, 12, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := ShardGroupEffectiveTimeRange(sg, tt.queryStart, tt.queryEnd)
			if !start.Equal(tt.expectedStart) {
				t.Errorf("expected start %v, got %v", tt.expectedStart, start)
			}
			if !end.Equal(tt.expectedEnd) {
				t.Errorf("expected end %v, got %v", tt.expectedEnd, end)
			}
		})
	}
}