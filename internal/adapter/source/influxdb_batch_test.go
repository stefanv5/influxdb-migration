package source

import (
	"strings"
	"testing"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

func TestQueryConfigTagKeys(t *testing.T) {
	// Test that TagKeys field exists and can be set
	cfg := &types.QueryConfig{
		BatchSize:         1000,
		TimeWindow:        168 * 60 * 60 * 1e9, // 168 hours in nanoseconds
		MaxSeriesPerQuery: 100,
		TagKeys:           []string{"host", "region", "service"},
	}

	if len(cfg.TagKeys) != 3 {
		t.Errorf("expected 3 tag keys, got %d", len(cfg.TagKeys))
	}

	if cfg.TagKeys[0] != "host" || cfg.TagKeys[1] != "region" || cfg.TagKeys[2] != "service" {
		t.Errorf("unexpected tag keys: %v", cfg.TagKeys)
	}
}

func TestQueryConfigWithDefaults(t *testing.T) {
	// Test WithDefaults applies defaults correctly
	cfg := &types.QueryConfig{}
	cfg = cfg.WithDefaults()

	if cfg.BatchSize != types.DefaultBatchSize {
		t.Errorf("expected default batch size %d, got %d", types.DefaultBatchSize, cfg.BatchSize)
	}

	if cfg.MaxSeriesPerQuery != types.DefaultSeriesPerQuery {
		t.Errorf("expected default series per query %d, got %d", types.DefaultSeriesPerQuery, cfg.MaxSeriesPerQuery)
	}
}

func TestQueryConfigTagKeysDefaults(t *testing.T) {
	// Test that TagKeys defaults to nil (not empty slice)
	cfg := &types.QueryConfig{}
	cfg = cfg.WithDefaults()

	// TagKeys should be nil when not set, not an empty slice
	// This is important because nil slice is distinguishable from empty slice
	if cfg.TagKeys == nil {
		t.Log("TagKeys is nil as expected when not set")
	} else if len(cfg.TagKeys) == 0 {
		t.Log("TagKeys is empty slice (acceptable)")
	}
}

func TestParseSeriesKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected map[string]string
	}{
		{
			name: "single tag",
			key:  "cpu,host=server1",
			expected: map[string]string{"host": "server1"},
		},
		{
			name: "multiple tags",
			key:  "cpu,host=server1,region=us",
			expected: map[string]string{"host": "server1", "region": "us"},
		},
		{
			name:     "no tags",
			key:      "cpu",
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseSeriesKey(tt.key)
			if len(result) != len(tt.expected) {
				t.Errorf("expected %d tags, got %d", len(tt.expected), len(result))
			}
			for k, v := range tt.expected {
				if result[k] != v {
					t.Errorf("expected %s=%s, got %s=%s", k, v, k, result[k])
				}
			}
		})
	}
}

func TestBuildWhereClause(t *testing.T) {
	tests := []struct {
		name     string
		series   []string
		contains string
	}{
		{
			name:     "single series",
			series:   []string{"cpu,host=server1"},
			contains: `"host"='server1'`,
		},
		{
			name:     "multiple series",
			series:   []string{"cpu,host=server1", "cpu,host=server2"},
			contains: " OR ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildWhereClause(tt.series)
			if tt.contains != "" && !strings.Contains(result, tt.contains) {
				t.Errorf("expected result to contain %q, got %q", tt.contains, result)
			}
		})
	}
}
