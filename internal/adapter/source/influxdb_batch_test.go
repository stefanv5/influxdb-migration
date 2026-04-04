package source

import "testing"

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
			if tt.contains != "" && !containsString(result, tt.contains) {
				t.Errorf("expected result to contain %q, got %q", tt.contains, result)
			}
		})
	}
}

func containsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
