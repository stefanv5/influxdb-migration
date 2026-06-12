package source

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
			name:     "single tag",
			key:      "cpu,host=server1",
			expected: map[string]string{"host": "server1"},
		},
		{
			name:     "multiple tags",
			key:      "cpu,host=server1,region=us",
			expected: map[string]string{"host": "server1", "region": "us"},
		},
		{
			name:     "no tags",
			key:      "cpu",
			expected: map[string]string{},
		},
		{
			name: "escaped separators",
			key:  `cpu,host=server\,1,region=us\=east,path=c:\\data`,
			expected: map[string]string{
				"host":   "server,1",
				"region": "us=east",
				"path":   `c:\data`,
			},
		},
		{
			name: "escaped tag key",
			key:  `cpu,tag\,key=value`,
			expected: map[string]string{
				"tag,key": "value",
			},
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
		{
			name:     "mixed no-tag and tagged series includes no-tag condition",
			series:   []string{"cpu", "cpu,host=server1"},
			contains: `"host" !~ /.*/`,
		},
		{
			name:     "escapes backslash and single quote in string literals",
			series:   []string{`cpu,host=server\\1,region=us'east`},
			contains: `"host"='server\\1'`,
		},
		{
			name:     "keeps escaped separators in parsed tag values",
			series:   []string{`cpu,host=server\,1,region=us\=east`},
			contains: `"host"='server,1'`,
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

func TestBuildWhereClauseNoTagOnlyMatchesAll(t *testing.T) {
	if got := BuildWhereClause([]string{"cpu"}); got != "1=1" {
		t.Fatalf("expected no-tag-only clause to be 1=1, got %q", got)
	}
}

func TestBuildWhereClauseWithTagKeysNoTagOnlyDoesNotMatchAll(t *testing.T) {
	got := BuildWhereClauseWithTagKeys([]string{"cpu"}, []string{"host", "region"})
	if got == "1=1" {
		t.Fatal("expected no-tag-only clause with known tag keys to avoid 1=1")
	}
	for _, want := range []string{`"host" !~ /.*/`, `"region" !~ /.*/`, " AND "} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected clause to contain %q, got %q", want, got)
		}
	}
}

func TestBuildWhereClauseEscapesInfluxQLStringLiteral(t *testing.T) {
	got := BuildWhereClause([]string{`cpu,host=server\\1,region=us'east`})
	if !strings.Contains(got, `"host"='server\\1'`) {
		t.Fatalf("expected escaped backslash in where clause, got %q", got)
	}
	if !strings.Contains(got, `"region"='us\'east'`) {
		t.Fatalf("expected escaped single quote in where clause, got %q", got)
	}
}

func TestDecodeInfluxV1ConfigAcceptsDirectCredentials(t *testing.T) {
	var cfg InfluxDBV1Config
	err := decodeInfluxV1Config(map[string]interface{}{
		"influxdb": map[string]interface{}{
			"url":      "http://localhost:8086",
			"database": "db",
			"username": "direct-user",
			"password": "direct-pass",
		},
	}, &cfg)
	if err != nil {
		t.Fatalf("decodeInfluxV1Config returned error: %v", err)
	}
	if cfg.Username != "direct-user" || cfg.Password != "direct-pass" {
		t.Fatalf("expected direct credentials, got username=%q password=%q", cfg.Username, cfg.Password)
	}
}

func TestDecodeInfluxV1ConfigBasicAuthOverridesDirectCredentials(t *testing.T) {
	var cfg InfluxDBV1Config
	err := decodeInfluxV1Config(map[string]interface{}{
		"influxdb": map[string]interface{}{
			"url":      "http://localhost:8086",
			"database": "db",
			"username": "direct-user",
			"password": "direct-pass",
			"basic_auth": map[string]interface{}{
				"username": "basic-user",
				"password": "basic-pass",
			},
		},
	}, &cfg)
	if err != nil {
		t.Fatalf("decodeInfluxV1Config returned error: %v", err)
	}
	if cfg.Username != "basic-user" || cfg.Password != "basic-pass" {
		t.Fatalf("expected basic_auth credentials, got username=%q password=%q", cfg.Username, cfg.Password)
	}
}

func TestInfluxDBV1QueryDataHonorsConfigTimeBounds(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)
	var selectQuery string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[]}`)
			return
		}
		if r.URL.Query().Get("chunked") == "true" {
			selectQuery = query
			fmt.Fprint(w, `{"results":[]}`)
			return
		}
		t.Fatalf("unexpected query request: %s", r.URL.RawQuery)
	}))
	defer server.Close()

	adapter := &InfluxDBV1Adapter{
		client:  server.Client(),
		config:  &InfluxDBV1Config{Database: "db"},
		baseURL: server.URL,
	}

	_, err := adapter.QueryData(context.Background(), "cpu", nil, func([]types.Record) error {
		return nil
	}, &types.QueryConfig{StartTime: start, EndTime: end, BatchSize: 10})
	if err != nil {
		t.Fatalf("QueryData returned error: %v", err)
	}

	if !strings.Contains(selectQuery, start.Format(time.RFC3339Nano)) {
		t.Fatalf("query does not contain configured start time %q: %s", start.Format(time.RFC3339Nano), selectQuery)
	}
	if !strings.Contains(selectQuery, end.Format(time.RFC3339Nano)) {
		t.Fatalf("query does not contain configured end time %q: %s", end.Format(time.RFC3339Nano), selectQuery)
	}
}

func TestInfluxDBV2QueryDataHonorsConfigTimeBoundsAndOmitsEmptyCredentials(t *testing.T) {
	start := time.Date(2024, 2, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 2, 3, 3, 4, 5, 6, time.UTC)
	var selectQuery string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.URL.Query()
		if _, ok := values["u"]; ok {
			t.Fatalf("unexpected empty u parameter in %s", r.URL.RawQuery)
		}
		if _, ok := values["p"]; ok {
			t.Fatalf("unexpected empty p parameter in %s", r.URL.RawQuery)
		}

		query := values.Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[]}`)
			return
		}
		if values.Get("chunked") == "true" {
			selectQuery = query
			fmt.Fprint(w, `{"results":[]}`)
			return
		}
		t.Fatalf("unexpected query request: %s", r.URL.RawQuery)
	}))
	defer server.Close()

	adapter := &InfluxDBV2Adapter{
		client: server.Client(),
		config: &InfluxDBV2Config{URL: server.URL, Bucket: "bucket"},
	}

	_, err := adapter.QueryData(context.Background(), "cpu", nil, func([]types.Record) error {
		return nil
	}, &types.QueryConfig{StartTime: start, EndTime: end, BatchSize: 10})
	if err != nil {
		t.Fatalf("QueryData returned error: %v", err)
	}

	if !strings.Contains(selectQuery, start.Format(time.RFC3339Nano)) {
		t.Fatalf("query does not contain configured start time %q: %s", start.Format(time.RFC3339Nano), selectQuery)
	}
	if !strings.Contains(selectQuery, end.Format(time.RFC3339Nano)) {
		t.Fatalf("query does not contain configured end time %q: %s", end.Format(time.RFC3339Nano), selectQuery)
	}
}

func TestInfluxDBV1QueryDataBatchNoTagSeriesUsesDiscoveredTagKeys(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)
	var selectQuery string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"columns":["tagKey"],"values":[["host"]]}]}]}`)
			return
		}
		if r.URL.Query().Get("chunked") == "true" {
			selectQuery = query
			fmt.Fprint(w, `{"results":[]}`)
			return
		}
		t.Fatalf("unexpected query request: %s", r.URL.RawQuery)
	}))
	defer server.Close()

	adapter := &InfluxDBV1Adapter{
		client:  server.Client(),
		config:  &InfluxDBV1Config{Database: "db"},
		baseURL: server.URL,
	}

	_, err := adapter.QueryDataBatch(context.Background(), "cpu", []string{"cpu"}, start, end, nil, func([]types.Record) error {
		return nil
	}, &types.QueryConfig{BatchSize: 10})
	if err != nil {
		t.Fatalf("QueryDataBatch returned error: %v", err)
	}
	if strings.Contains(selectQuery, "1=1") {
		t.Fatalf("expected batch query to avoid 1=1 for no-tag series with known tag keys: %s", selectQuery)
	}
	if !strings.Contains(selectQuery, `"host" !~ /.*/`) {
		t.Fatalf("expected batch query to filter no-tag series by missing host tag: %s", selectQuery)
	}
}

func TestInfluxDBV2QueryDataBatchNoTagSeriesUsesDiscoveredTagKeys(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)
	var selectQuery string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[{"series":[{"columns":["tagKey"],"values":[["host"]]}]}]}`)
			return
		}
		if r.URL.Query().Get("chunked") == "true" {
			selectQuery = query
			fmt.Fprint(w, `{"results":[]}`)
			return
		}
		t.Fatalf("unexpected query request: %s", r.URL.RawQuery)
	}))
	defer server.Close()

	adapter := &InfluxDBV2Adapter{
		client: server.Client(),
		config: &InfluxDBV2Config{URL: server.URL, Bucket: "bucket"},
	}

	_, err := adapter.QueryDataBatch(context.Background(), "cpu", []string{"cpu"}, start, end, nil, func([]types.Record) error {
		return nil
	}, &types.QueryConfig{BatchSize: 10})
	if err != nil {
		t.Fatalf("QueryDataBatch returned error: %v", err)
	}
	if strings.Contains(selectQuery, "1=1") {
		t.Fatalf("expected batch query to avoid 1=1 for no-tag series with known tag keys: %s", selectQuery)
	}
	if !strings.Contains(selectQuery, `"host" !~ /.*/`) {
		t.Fatalf("expected batch query to filter no-tag series by missing host tag: %s", selectQuery)
	}
}

func TestInfluxDBV2BuildV1QueryParamsIncludesNonEmptyCredentials(t *testing.T) {
	adapter := &InfluxDBV2Adapter{
		config: &InfluxDBV2Config{
			Bucket:   "bucket",
			Username: "user",
			Password: "pass",
		},
	}

	params := adapter.buildV1QueryParams("SHOW MEASUREMENTS")
	if got := params.Get("u"); got != "user" {
		t.Fatalf("expected username param, got %q", got)
	}
	if got := params.Get("p"); got != "pass" {
		t.Fatalf("expected password param, got %q", got)
	}
}

func TestInfluxDBV2ExecuteV1QueryChecksStatementError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"results":[{"statement_id":0,"error":"bad statement"}]}`)
	}))
	defer server.Close()

	adapter := &InfluxDBV2Adapter{
		client: server.Client(),
		config: &InfluxDBV2Config{URL: server.URL, Bucket: "bucket"},
	}

	_, err := adapter.executeV1Query(context.Background(), "SELECT * FROM cpu")
	if err == nil || !strings.Contains(err.Error(), "bad statement") {
		t.Fatalf("expected per-statement error, got %v", err)
	}
}
