package engine

import (
	"testing"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

func TestMatchTagFiltersUsesParsedTags(t *testing.T) {
	engine := &MigrationEngine{}

	tests := []struct {
		name    string
		series  string
		filters map[string][]string
		want    bool
	}{
		{
			name:    "exact match",
			series:  "cpu,host=server1",
			filters: map[string][]string{"host": {"server1"}},
			want:    true,
		},
		{
			name:    "does not match prefix value",
			series:  "cpu,host=server10",
			filters: map[string][]string{"host": {"server1"}},
			want:    false,
		},
		{
			name:    "escaped comma and equals",
			series:  `cpu,host=server\,1,region=us\=east`,
			filters: map[string][]string{"host": {"server,1"}, "region": {"us=east"}},
			want:    true,
		},
		{
			name:    "escaped tag key",
			series:  `cpu,tag\,key=value`,
			filters: map[string][]string{"tag,key": {"value"}},
			want:    true,
		},
		{
			name:    "missing tag",
			series:  "cpu,host=server1",
			filters: map[string][]string{"region": {"us-east"}},
			want:    false,
		},
		{
			name:    "empty allowed list ignored",
			series:  "cpu,host=server1",
			filters: map[string][]string{"host": {}},
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := engine.matchTagFilters(tt.series, tt.filters); got != tt.want {
				t.Fatalf("matchTagFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplySubtablePatternUsesParsedTags(t *testing.T) {
	engine := &MigrationEngine{}

	got := engine.applySubtablePattern("cpu", `cpu,host=server\,1,region=us\=east`, "{{table}}_{{tag1}}_{{value1}}_{{tag2}}_{{value2}}")
	want := "cpu_host_server,1_region_us=east"
	if got != want {
		t.Fatalf("applySubtablePattern() = %q, want %q", got, want)
	}
}

func TestSourceConfigToMapPassesInfluxDBDirectCredentials(t *testing.T) {
	engine := &MigrationEngine{}

	cfg := engine.sourceConfigToMap(types.SourceConfig{
		Type: "influxdb",
		InfluxDB: types.InfluxDBConfig{
			URL:      "http://localhost:8086",
			Bucket:   "db",
			Username: "direct-user",
			Password: "direct-pass",
		},
	})

	influxCfg, ok := cfg["influxdb"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected influxdb config map, got %#v", cfg["influxdb"])
	}
	if influxCfg["username"] != "direct-user" || influxCfg["password"] != "direct-pass" {
		t.Fatalf("expected direct credentials, got username=%#v password=%#v", influxCfg["username"], influxCfg["password"])
	}
}
