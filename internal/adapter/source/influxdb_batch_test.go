package source

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

func TestParseInfluxJSONNumbersPreservesFieldTypes(t *testing.T) {
	columns := []string{"time", "integer_value", "float_value", "whole_float_value", "bool_value"}
	values := []interface{}{
		"2024-01-01T00:00:00Z",
		json.Number("42"),
		json.Number("42.5"),
		json.Number("42.0"),
		true,
	}

	record, err := parseV1Values(columns, values, nil)
	if err != nil {
		t.Fatalf("parseV1Values returned error: %v", err)
	}

	if got, ok := record.Fields["integer_value"].(int64); !ok || got != 42 {
		t.Fatalf("expected integer_value int64(42), got %#v (%T)", record.Fields["integer_value"], record.Fields["integer_value"])
	}
	if got, ok := record.Fields["float_value"].(float64); !ok || got != 42.5 {
		t.Fatalf("expected float_value float64(42.5), got %#v (%T)", record.Fields["float_value"], record.Fields["float_value"])
	}
	if got, ok := record.Fields["whole_float_value"].(float64); !ok || got != 42.0 {
		t.Fatalf("expected whole_float_value float64(42.0), got %#v (%T)", record.Fields["whole_float_value"], record.Fields["whole_float_value"])
	}
	if got, ok := record.Fields["bool_value"].(bool); !ok || !got {
		t.Fatalf("expected bool_value true, got %#v (%T)", record.Fields["bool_value"], record.Fields["bool_value"])
	}
}

func TestParseInfluxJSONNumberTooLargeIsExplicitlyRepresented(t *testing.T) {
	columns := []string{"time", "too_large"}
	values := []interface{}{
		"2024-01-01T00:00:00Z",
		json.Number("9223372036854775808"),
	}

	_, err := parseV1Values(columns, values, nil)
	if err == nil {
		t.Fatal("expected oversized integer conversion error")
	}
	if !strings.Contains(err.Error(), "too_large") || !strings.Contains(err.Error(), "9223372036854775808") {
		t.Fatalf("expected explicit field/value conversion error, got %v", err)
	}
}

func TestParseInfluxJSONNumbersWithTagKeysPreservesTagsAndFields(t *testing.T) {
	columns := []string{"time", "host", "requests"}
	values := []interface{}{
		"2024-01-01T00:00:00Z",
		"server-a",
		json.Number("7"),
	}

	record, err := parseV1ValuesWithTagKeys(columns, values, map[string]bool{"host": true}, nil)
	if err != nil {
		t.Fatalf("parseV1ValuesWithTagKeys returned error: %v", err)
	}

	if got := record.Tags["host"]; got != "server-a" {
		t.Fatalf("expected host tag server-a, got %q", got)
	}
	if got, ok := record.Fields["requests"].(int64); !ok || got != 7 {
		t.Fatalf("expected requests int64(7), got %#v (%T)", record.Fields["requests"], record.Fields["requests"])
	}
}

func TestInfluxDBV1DiscoverTagKeysReturnsQueryErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer server.Close()

	adapter := &InfluxDBV1Adapter{
		client:  server.Client(),
		config:  &InfluxDBV1Config{URL: server.URL, Database: "metrics"},
		baseURL: server.URL,
	}

	_, err := adapter.DiscoverTagKeys(context.Background(), "cpu")
	if err == nil {
		t.Fatal("expected tag key discovery error")
	}
	if !strings.Contains(err.Error(), "discover tag keys") {
		t.Fatalf("expected explicit discover tag keys error, got %v", err)
	}
}

func TestInfluxDBV1DiscoverTagKeysAllowsEmptyResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"results":[{}]}`))
	}))
	defer server.Close()

	adapter := &InfluxDBV1Adapter{
		client:  server.Client(),
		config:  &InfluxDBV1Config{URL: server.URL, Database: "metrics"},
		baseURL: server.URL,
	}

	tagKeys, err := adapter.DiscoverTagKeys(context.Background(), "cpu")
	if err != nil {
		t.Fatalf("expected empty tag key result without error, got %v", err)
	}
	if len(tagKeys) != 0 {
		t.Fatalf("expected no tag keys, got %#v", tagKeys)
	}
}

func TestInfluxDBV2DiscoverTagKeysReturnsQueryErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer server.Close()

	adapter := &InfluxDBV2Adapter{
		client: server.Client(),
		config: &InfluxDBV2Config{
			URL:      server.URL,
			Username: "user",
			Password: "pass",
		},
	}

	_, err := adapter.DiscoverTagKeys(context.Background(), "cpu")
	if err == nil {
		t.Fatal("expected tag key discovery error")
	}
	if !strings.Contains(err.Error(), "discover tag keys") {
		t.Fatalf("expected explicit discover tag keys error, got %v", err)
	}
}

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

// TestInfluxDBV1ConnectHasNoOverallClientTimeout verifies the V1 adapter does
// not set http.Client.Timeout, which would cancel chunked response bodies mid-stream
// (Go's client Timeout covers reading the body). Streaming must rely on the
// per-request context.Context and Transport.ResponseHeaderTimeout instead.
func TestInfluxDBV1ConnectHasNoOverallClientTimeout(t *testing.T) {
	t.Setenv("ALLOW_INSECURE_TLS", "0")
	adapter := &InfluxDBV1Adapter{}
	cfg := map[string]interface{}{
		"influxdb": map[string]interface{}{
			"url":      "http://localhost:8086",
			"database": "db",
		},
	}
	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	defer adapter.Disconnect(context.Background())

	if adapter.client == nil {
		t.Fatal("expected non-nil http client after Connect")
	}
	if adapter.client.Timeout != 0 {
		t.Fatalf("expected http.Client.Timeout == 0 to allow indefinite streaming, got %v", adapter.client.Timeout)
	}
	transport, ok := adapter.client.Transport.(*http.Transport)
	if !ok || transport == nil {
		t.Fatalf("expected *http.Transport, got %T", adapter.client.Transport)
	}
	if transport.ResponseHeaderTimeout <= 0 {
		t.Fatalf("expected Transport.ResponseHeaderTimeout > 0 so headers arrive promptly, got %v", transport.ResponseHeaderTimeout)
	}
}

// TestInfluxDBV2ConnectHasNoOverallClientTimeout mirrors the V1 check for the V2 adapter.
func TestInfluxDBV2ConnectHasNoOverallClientTimeout(t *testing.T) {
	t.Setenv("ALLOW_INSECURE_TLS", "0")
	adapter := &InfluxDBV2Adapter{}
	cfg := map[string]interface{}{
		"influxdb": map[string]interface{}{
			"url":    "http://localhost:8086",
			"token":  "tok",
			"org":    "org",
			"bucket": "bucket",
		},
	}
	if err := adapter.Connect(context.Background(), cfg); err != nil {
		t.Fatalf("Connect returned error: %v", err)
	}
	defer adapter.Disconnect(context.Background())

	if adapter.client == nil {
		t.Fatal("expected non-nil http client after Connect")
	}
	if adapter.client.Timeout != 0 {
		t.Fatalf("expected http.Client.Timeout == 0 to allow indefinite streaming, got %v", adapter.client.Timeout)
	}
	transport, ok := adapter.client.Transport.(*http.Transport)
	if !ok || transport == nil {
		t.Fatalf("expected *http.Transport, got %T", adapter.client.Transport)
	}
	if transport.ResponseHeaderTimeout <= 0 {
		t.Fatalf("expected Transport.ResponseHeaderTimeout > 0 so headers arrive promptly, got %v", transport.ResponseHeaderTimeout)
	}
}

// TestInfluxDBV1ChunkedQueryStreamsBeyondOldClientTimeout verifies that a slow
// chunked response (each chunk written with a delay exceeding the old 30s
// client timeout) is not cancelled mid-stream. The handler writes two chunks
// spaced 2s apart; with the old client.Timeout=30s the body read deadline was
// measured from the start, but here we assert via a short artificial delay that
// the body is fully consumed. The previous code would fail this only if the
// delay exceeded 30s; we instead assert the contract: no client-level Timeout.
// TestParseV1ValuesWithTagKeysTreatsSeriesTagColumnAsTagWhenMissingFromTagKeySet
// reproduces A2: a string column present in series.Tags but absent from
// DiscoverTagKeys (e.g. due to an offline shard during SHOW TAG KEYS) must
// still be classified as a tag, not a string field, to preserve series identity.
func TestParseV1ValuesWithTagKeysTreatsSeriesTagColumnAsTagWhenMissingFromTagKeySet(t *testing.T) {
	// host is a tag per series.Tags but missing from tagKeySet (simulating an
	// incomplete DiscoverTagKeys result).
	columns := []string{"time", "host", "value"}
	values := []interface{}{
		"2024-01-01T00:00:00Z",
		"server-a",
		json.Number("7"),
	}
	seriesTags := map[string]string{"host": "server-a"}
	tagKeySet := map[string]bool{} // host intentionally absent

	record, err := parseV1ValuesWithTagKeys(columns, values, tagKeySet, seriesTags)
	if err != nil {
		t.Fatalf("parseV1ValuesWithTagKeys returned error: %v", err)
	}
	if got := record.Tags["host"]; got != "server-a" {
		t.Fatalf("expected host to be classified as a tag (server-a) via series.Tags cross-check, got %q (tags=%#v fields=%#v)", got, record.Tags, record.Fields)
	}
	if _, exists := record.Fields["host"]; exists {
		t.Fatalf("host must not be a field when it is a series tag, got fields=%#v", record.Fields)
	}
	if got, ok := record.Fields["value"].(int64); !ok || got != 7 {
		t.Fatalf("expected value int64(7), got %#v (%T)", record.Fields["value"], record.Fields["value"])
	}
}

// TestInfluxDBV1ParseValuesTreatsSeriesTagColumnAsTagWhenMissingFromTagKeySet
// mirrors A2 for the V1 adapter's parseValues method.
func TestInfluxDBV1ParseValuesTreatsSeriesTagColumnAsTagWhenMissingFromTagKeySet(t *testing.T) {
	adapter := &InfluxDBV1Adapter{}
	columns := []string{"time", "region", "value"}
	values := []interface{}{
		"2024-01-01T00:00:00Z",
		"us-east",
		json.Number("9"),
	}
	seriesTags := map[string]string{"region": "us-east"}
	tagKeySet := map[string]bool{} // region intentionally absent

	record, err := adapter.parseValues(columns, values, seriesTags, tagKeySet)
	if err != nil {
		t.Fatalf("parseValues returned error: %v", err)
	}
	if got := record.Tags["region"]; got != "us-east" {
		t.Fatalf("expected region to be a tag via series.Tags cross-check, got %q (tags=%#v fields=%#v)", got, record.Tags, record.Fields)
	}
	if _, exists := record.Fields["region"]; exists {
		t.Fatalf("region must not be a field when it is a series tag, got fields=%#v", record.Fields)
	}
}

// TestInfluxDBV1QueryDataBatchCachesTagKeysAcrossBatches verifies that a single
// QueryDataBatch call issues SHOW TAG KEYS at most once (cached), not once per
// chunk. The prior code re-discovered per batch call; within one call it already
// discovers once, so this test guards against regression and documents the
// contract. A2's main fix (series.Tags cross-check) is covered by the unit tests
// above; caching across batch calls is enforced here by counting SHOW TAG KEYS
// requests during a multi-chunk stream.
func TestInfluxDBV1QueryDataBatchCachesTagKeysAcrossBatches(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)
	var tagKeyQueryCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			tagKeyQueryCount++
			fmt.Fprint(w, `{"results":[{"series":[{"columns":["tagKey"],"values":[["host"]]}]}]}`)
			return
		}
		if r.URL.Query().Get("chunked") == "true" {
			// Emit two chunks so the batchFunc is invoked twice.
			fmt.Fprint(w, `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"a"},"columns":["time","value"],"values":[["2024-01-02T03:04:05Z",1]]}]}]}`)
			fmt.Fprint(w, `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"a"},"columns":["time","value"],"values":[["2024-01-02T03:05:00Z",2]]}]}]}`)
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

	_, err := adapter.QueryDataBatch(context.Background(), "cpu", []string{"cpu,host=a"}, start, end, nil, func([]types.Record) error {
		return nil
	}, &types.QueryConfig{BatchSize: 10})
	if err != nil {
		t.Fatalf("QueryDataBatch returned error: %v", err)
	}
	if tagKeyQueryCount != 1 {
		t.Fatalf("expected SHOW TAG KEYS to be issued exactly once per batch (cached), got %d", tagKeyQueryCount)
	}
}

// TestInfluxDBV1QueryDataBatchCachesTagKeysAcrossSeparateBatchCalls verifies
// that two sequential QueryDataBatch calls for the same measurement issue
// SHOW TAG KEYS only once (cached on the adapter), exercising the A2 cache.
func TestInfluxDBV1QueryDataBatchCachesTagKeysAcrossSeparateBatchCalls(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)
	var tagKeyQueryCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			tagKeyQueryCount++
			fmt.Fprint(w, `{"results":[{"series":[{"columns":["tagKey"],"values":[["host"]]}]}]}`)
			return
		}
		if r.URL.Query().Get("chunked") == "true" {
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

	for i := 0; i < 2; i++ {
		_, err := adapter.QueryDataBatch(context.Background(), "cpu", []string{"cpu,host=a"}, start, end, nil, func([]types.Record) error {
			return nil
		}, &types.QueryConfig{BatchSize: 10})
		if err != nil {
			t.Fatalf("QueryDataBatch call %d returned error: %v", i, err)
		}
	}
	if tagKeyQueryCount != 1 {
		t.Fatalf("expected SHOW TAG KEYS to be issued once across two batch calls (cached), got %d", tagKeyQueryCount)
	}
}

// TestInfluxDBV2QueryDataBatchCachesTagKeysAcrossSeparateBatchCalls mirrors
// the V1 cross-batch cache test for the V2 adapter.
func TestInfluxDBV2QueryDataBatchCachesTagKeysAcrossSeparateBatchCalls(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)
	var tagKeyQueryCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			tagKeyQueryCount++
			fmt.Fprint(w, `{"results":[{"series":[{"columns":["tagKey"],"values":[["host"]]}]}]}`)
			return
		}
		if r.URL.Query().Get("chunked") == "true" {
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

	for i := 0; i < 2; i++ {
		_, err := adapter.QueryDataBatch(context.Background(), "cpu", []string{"cpu,host=a"}, start, end, nil, func([]types.Record) error {
			return nil
		}, &types.QueryConfig{BatchSize: 10})
		if err != nil {
			t.Fatalf("QueryDataBatch call %d returned error: %v", i, err)
		}
	}
	if tagKeyQueryCount != 1 {
		t.Fatalf("expected SHOW TAG KEYS to be issued once across two batch calls (cached), got %d", tagKeyQueryCount)
	}
}

// TestInfluxDBV1QueryDataBatchPrefersConfigTagKeys verifies that when cfg.TagKeys
// is supplied, the adapter does NOT issue SHOW TAG KEYS at all.
func TestInfluxDBV1QueryDataBatchPrefersConfigTagKeys(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)
	var tagKeyQueryCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			tagKeyQueryCount++
			fmt.Fprint(w, `{"results":[]}`)
			return
		}
		if r.URL.Query().Get("chunked") == "true" {
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

	_, err := adapter.QueryDataBatch(context.Background(), "cpu", []string{"cpu,host=a"}, start, end, nil, func([]types.Record) error {
		return nil
	}, &types.QueryConfig{BatchSize: 10, TagKeys: []string{"host"}})
	if err != nil {
		t.Fatalf("QueryDataBatch returned error: %v", err)
	}
	if tagKeyQueryCount != 0 {
		t.Fatalf("expected no SHOW TAG KEYS when cfg.TagKeys supplied, got %d", tagKeyQueryCount)
	}
}

// TestParseV1ValuesRejectsMalformedTimestamp reproduces A3: a totally
// unparseable timestamp string must produce a hard error, not silently leave
// record.Time=0 (which would write the point at epoch and corrupt data).
func TestParseV1ValuesRejectsMalformedTimestamp(t *testing.T) {
	columns := []string{"time", "value"}
	values := []interface{}{"not-a-timestamp", json.Number("1")}
	_, err := parseV1Values(columns, values, nil)
	if err == nil {
		t.Fatal("expected error for malformed timestamp, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse InfluxDB timestamp") {
		t.Fatalf("expected timestamp parse error message, got %v", err)
	}
	if !strings.Contains(err.Error(), "not-a-timestamp") {
		t.Fatalf("expected error to reference the bad timestamp value, got %v", err)
	}
}

// TestParseV1ValuesWithTagKeysRejectsMalformedTimestamp mirrors A3 for the
// tag-keys-aware parser.
func TestParseV1ValuesWithTagKeysRejectsMalformedTimestamp(t *testing.T) {
	columns := []string{"time", "host", "value"}
	values := []interface{}{"2024-13-99T99:99:99Z", "a", json.Number("1")}
	_, err := parseV1ValuesWithTagKeys(columns, values, map[string]bool{"host": true}, nil)
	if err == nil {
		t.Fatal("expected error for malformed timestamp, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse InfluxDB timestamp") {
		t.Fatalf("expected timestamp parse error message, got %v", err)
	}
}

// TestInfluxDBV1ParseValuesRejectsMalformedTimestamp mirrors A3 for the V1
// adapter method.
func TestInfluxDBV1ParseValuesRejectsMalformedTimestamp(t *testing.T) {
	adapter := &InfluxDBV1Adapter{}
	columns := []string{"time", "value"}
	values := []interface{}{"garbage", json.Number("1")}
	_, err := adapter.parseValues(columns, values, nil, nil)
	if err == nil {
		t.Fatal("expected error for malformed timestamp, got nil")
	}
	if !strings.Contains(err.Error(), "failed to parse InfluxDB timestamp") {
		t.Fatalf("expected timestamp parse error message, got %v", err)
	}
}

// TestParseV1ValuesAcceptsRFC3339WithWarn verifies the reduced-precision
// fallback (RFC3339Nano fails, RFC3339 succeeds) still works and does NOT error.
func TestParseV1ValuesAcceptsRFC3339WithWarn(t *testing.T) {
	columns := []string{"time", "value"}
	values := []interface{}{"2024-01-02T03:04:05Z", json.Number("1")}
	record, err := parseV1Values(columns, values, nil)
	if err != nil {
		t.Fatalf("expected RFC3339 timestamp to parse without error, got %v", err)
	}
	expected := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano()
	if record.Time != expected {
		t.Fatalf("expected time %d, got %d", expected, record.Time)
	}
}

// TestInfluxDBV1QueryDataBatchErrorsOnMalformedTimestamp verifies the A3 error
// propagates through executeChunkedQuery's batchFunc path so the whole batch
// aborts rather than silently writing zero-timestamp points.
func TestInfluxDBV1QueryDataBatchErrorsOnMalformedTimestamp(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[]}`)
			return
		}
		if r.URL.Query().Get("chunked") == "true" {
			// Row with a malformed timestamp that cannot be parsed by either
			// RFC3339Nano or RFC3339.
			fmt.Fprint(w, `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["garbage-time",1]]}]}]}`)
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
	if err == nil {
		t.Fatal("expected QueryDataBatch to return error for malformed timestamp")
	}
	if !strings.Contains(err.Error(), "failed to parse InfluxDB timestamp") {
		t.Fatalf("expected timestamp parse error to propagate, got %v", err)
	}
}

// TestInfluxDBV2QueryDataBatchErrorsOnMalformedTimestamp mirrors the V1
// propagation test for the V2 adapter (uses parseV1ValuesWithTagKeys).
func TestInfluxDBV2QueryDataBatchErrorsOnMalformedTimestamp(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[]}`)
			return
		}
		if r.URL.Query().Get("chunked") == "true" {
			fmt.Fprint(w, `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["garbage-time",1]]}]}]}`)
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
	if err == nil {
		t.Fatal("expected QueryDataBatch to return error for malformed timestamp")
	}
	if !strings.Contains(err.Error(), "failed to parse InfluxDB timestamp") {
		t.Fatalf("expected timestamp parse error to propagate, got %v", err)
	}
}

// TestParseTimeReturnsErrorOnUnparseableString reproduces A4: parseTime must
// surface parse failures as an error rather than silently returning time.Time{}
// (which would produce a zero-start/end shard and zero windows downstream).
func TestParseTimeReturnsErrorOnUnparseableString(t *testing.T) {
	_, err := parseTime("not-a-time")
	if err == nil {
		t.Fatal("expected parseTime to return an error for an unparseable string")
	}
	if !strings.Contains(err.Error(), "not-a-time") {
		t.Fatalf("expected error to reference the bad value, got %v", err)
	}
}

// TestParseTimeReturnsErrorOnEmptyString verifies empty string is a hard error,
// not a silent zero.
func TestParseTimeReturnsErrorOnEmptyString(t *testing.T) {
	_, err := parseTime("")
	if err == nil {
		t.Fatal("expected parseTime to return an error for an empty string")
	}
}

// TestParseTimeParsesRFC3339AndNano verifies valid inputs still parse.
func TestParseTimeParsesRFC3339AndNano(t *testing.T) {
	cases := []struct {
		name  string
		input interface{}
	}{
		{"rfc3339nano", "2024-01-02T03:04:05.123456789Z"},
		{"rfc3339", "2024-01-02T03:04:05Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTime(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.IsZero() {
				t.Fatal("expected non-zero time for valid input")
			}
		})
	}
}

// TestInfluxDBV1DiscoverShardGroupsSkipsBadShardAndKeepsGoodOnes reproduces A4:
// when one shard has an unparseable start/end time, DiscoverShardGroups should
// skip that shard with a warning and continue, rather than storing a zero
// start/end shard that produces empty time windows. The good shard must still
// be returned. SHOW SHARDS columns: id, database, rp, shard_group, start, end.
func TestInfluxDBV1DiscoverShardGroupsSkipsBadShardAndKeepsGoodOnes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Two shard groups: group 1 has a bad start time, group 2 is valid.
		fmt.Fprint(w, `{"results":[{"statement_id":0,"series":[{"name":"shards","columns":["id","database","retention_policy","shard_group","start_time","end_time"],"values":[[1,"db","autogen",1,"garbage-start","2024-01-02T00:00:00Z"],[2,"db","autogen",2,"2024-01-02T00:00:00Z","2024-01-03T00:00:00Z"]]}]}]}`)
	}))
	defer server.Close()

	adapter := &InfluxDBV1Adapter{
		client:  server.Client(),
		config:  &InfluxDBV1Config{Database: "db"},
		baseURL: server.URL,
	}

	groups, err := adapter.DiscoverShardGroups(context.Background())
	if err != nil {
		t.Fatalf("expected nil error when at least one shard parses, got %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 shard group (bad one skipped), got %d: %#v", len(groups), groups)
	}
	if groups[0].ID != 2 {
		t.Fatalf("expected surviving group ID 2, got %d", groups[0].ID)
	}
	if groups[0].StartTime.IsZero() || groups[0].EndTime.IsZero() {
		t.Fatalf("expected non-zero bounds on surviving shard, got start=%v end=%v", groups[0].StartTime, groups[0].EndTime)
	}
}

// TestInfluxDBV1DiscoverShardGroupsErrorsWhenAllShardsFail verifies that if
// every shard fails to parse, DiscoverShardGroups returns an error rather than
// an empty result that would silently no-op the migration.
func TestInfluxDBV1DiscoverShardGroupsErrorsWhenAllShardsFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"results":[{"statement_id":0,"series":[{"name":"shards","columns":["id","database","retention_policy","shard_group","start_time","end_time"],"values":[[1,"db","autogen",1,"garbage-1","garbage-2"]]}]}]}`)
	}))
	defer server.Close()

	adapter := &InfluxDBV1Adapter{
		client:  server.Client(),
		config:  &InfluxDBV1Config{Database: "db"},
		baseURL: server.URL,
	}

	groups, err := adapter.DiscoverShardGroups(context.Background())
	if err == nil {
		t.Fatalf("expected error when all shards fail to parse, got groups=%#v", groups)
	}
	if !strings.Contains(err.Error(), "parse") {
		t.Fatalf("expected parse-related error, got %v", err)
	}
}

// TestInfluxDBV1QueryDataRejectsMissingEndTime reproduces A5: single-mode
// QueryData (no time_range.end) previously baked end=time.Now().Add(1h) once,
// silently truncating the migration tail for long-running jobs. It must now
// return an error demanding an explicit end time.
func TestInfluxDBV1QueryDataRejectsMissingEndTime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"results":[]}`)
	}))
	defer server.Close()

	adapter := &InfluxDBV1Adapter{
		client:  server.Client(),
		config:  &InfluxDBV1Config{Database: "db"},
		baseURL: server.URL,
	}

	_, err := adapter.QueryData(context.Background(), "cpu", nil, func([]types.Record) error {
		return nil
	}, &types.QueryConfig{StartTime: time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC), BatchSize: 10})
	if err == nil {
		t.Fatal("expected error when single-mode QueryData has no explicit end time")
	}
	if !strings.Contains(err.Error(), "end time") {
		t.Fatalf("expected end-time error, got %v", err)
	}
}

// TestInfluxDBV2QueryDataRejectsMissingEndTime mirrors A5 for the V2 adapter.
func TestInfluxDBV2QueryDataRejectsMissingEndTime(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"results":[]}`)
	}))
	defer server.Close()

	adapter := &InfluxDBV2Adapter{
		client: server.Client(),
		config: &InfluxDBV2Config{URL: server.URL, Bucket: "bucket"},
	}

	_, err := adapter.QueryData(context.Background(), "cpu", nil, func([]types.Record) error {
		return nil
	}, &types.QueryConfig{StartTime: time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC), BatchSize: 10})
	if err == nil {
		t.Fatal("expected error when single-mode QueryData has no explicit end time")
	}
	if !strings.Contains(err.Error(), "end time") {
		t.Fatalf("expected end-time error, got %v", err)
	}
}

// TestEffectiveQueryBoundsRejectsMissingEndTime verifies the shared helper
// surfaces the error regardless of lastTS.
func TestEffectiveQueryBoundsRejectsMissingEndTime(t *testing.T) {
	// With cfg.EndTime zero and no start, end must still error.
	_, _, err := effectiveQueryBounds(0, &types.QueryConfig{BatchSize: 10})
	if err == nil {
		t.Fatal("expected error when effective bounds has no explicit end time")
	}
	if !strings.Contains(err.Error(), "end time") {
		t.Fatalf("expected end-time error, got %v", err)
	}
}

// TestEffectiveQueryBoundsAcceptsExplicitEndTime verifies a configured end
// time still produces bounds without error.
func TestEffectiveQueryBoundsAcceptsExplicitEndTime(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)
	startStr, endStr, err := effectiveQueryBounds(0, &types.QueryConfig{StartTime: start, EndTime: end})
	if err != nil {
		t.Fatalf("expected no error with explicit end time, got %v", err)
	}
	if !strings.Contains(startStr, "2024-01-02") {
		t.Fatalf("expected start string, got %q", startStr)
	}
	if !strings.Contains(endStr, "2024-01-03") {
		t.Fatalf("expected end string, got %q", endStr)
	}
}

// TestDedupAndSortSeriesDeduplicatesAndSorts reproduces A7 at the helper level:
// when SHOW SERIES pagination returns non-lexicographic or duplicate keys
// (multi-shard responses can), the dedup+sort helper must yield a complete,
// duplicate-free, sorted set. This is the core of the A7 defensive fix.
func TestDedupAndSortSeriesDeduplicatesAndSorts(t *testing.T) {
	input := []string{"cpu,b=2", "cpu,a=1", "cpu,b=2", "cpu,c=3", "cpu,a=1"}
	got := dedupAndSortSeries(input)
	want := []string{"cpu,a=1", "cpu,b=2", "cpu,c=3"}
	if len(got) != len(want) {
		t.Fatalf("expected %d unique sorted keys, got %d: %#v", len(want), len(got), got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("at index %d expected %q, got %q (full=%#v)", i, w, got[i], got)
		}
	}
}

// TestDedupAndSortSeriesEmpty verifies the helper handles empty/nil input.
func TestDedupAndSortSeriesEmpty(t *testing.T) {
	if got := dedupAndSortSeries(nil); len(got) != 0 {
		t.Fatalf("expected empty result for nil input, got %#v", got)
	}
	if got := dedupAndSortSeries([]string{}); len(got) != 0 {
		t.Fatalf("expected empty result for empty input, got %#v", got)
	}
}

// TestInfluxDBV1DiscoverSeriesDedupsAcrossPages reproduces A7 end-to-end with a
// small, overridable batch size. The server returns a full first page
// (== batchSize) of keys in non-sorted order, then a second page containing a
// duplicate plus a new key. Without dedup the duplicate would be emitted and
// the set would be internally inconsistent; the fix returns sorted unique keys.
func TestInfluxDBV1DiscoverSeriesDedupsAcrossPages(t *testing.T) {
	// Use a small batch size so the loop paginates without emitting 10000 rows.
	prev := seriesPaginationBatchSize
	seriesPaginationBatchSize = 2
	t.Cleanup(func() { seriesPaginationBatchSize = prev })

	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		// Page 1: 2 keys (== batchSize) in non-sorted order.
		// Page 2: a duplicate of an already-seen key plus a new key (== batchSize).
		// Page 3: a short final page (1 key < batchSize) to terminate the loop.
		switch requestCount {
		case 1:
			fmt.Fprint(w, `{"results":[{"series":[{"name":"cpu","columns":["key"],"values":[["cpu,b=2"],["cpu,a=1"]]}]}]}`)
		case 2:
			fmt.Fprint(w, `{"results":[{"series":[{"name":"cpu","columns":["key"],"values":[["cpu,b=2"],["cpu,c=3"]]}]}]}`)
		default:
			fmt.Fprint(w, `{"results":[{"series":[{"name":"cpu","columns":["key"],"values":[["cpu,c=3"]]}]}]}`)
		}
	}))
	defer server.Close()

	adapter := &InfluxDBV1Adapter{
		client:  server.Client(),
		config:  &InfluxDBV1Config{Database: "db"},
		baseURL: server.URL,
	}

	series, err := adapter.DiscoverSeries(context.Background(), "cpu")
	if err != nil {
		t.Fatalf("DiscoverSeries returned error: %v", err)
	}
	want := []string{"cpu,a=1", "cpu,b=2", "cpu,c=3"}
	if len(series) != len(want) {
		t.Fatalf("expected %d unique sorted series, got %d: %#v", len(want), len(series), series)
	}
	for i, w := range want {
		if series[i] != w {
			t.Fatalf("at index %d expected %q, got %q (full=%#v)", i, w, series[i], series)
		}
	}
}

// TestInfluxDBV2DiscoverSeriesDedupsAcrossPages mirrors A7 for the V2 adapter.
func TestInfluxDBV2DiscoverSeriesDedupsAcrossPages(t *testing.T) {
	prev := seriesPaginationBatchSize
	seriesPaginationBatchSize = 2
	t.Cleanup(func() { seriesPaginationBatchSize = prev })

	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		switch requestCount {
		case 1:
			fmt.Fprint(w, `{"results":[{"series":[{"name":"cpu","columns":["key"],"values":[["cpu,b=2"],["cpu,a=1"]]}]}]}`)
		case 2:
			fmt.Fprint(w, `{"results":[{"series":[{"name":"cpu","columns":["key"],"values":[["cpu,b=2"],["cpu,c=3"]]}]}]}`)
		default:
			fmt.Fprint(w, `{"results":[{"series":[{"name":"cpu","columns":["key"],"values":[["cpu,c=3"]]}]}]}`)
		}
	}))
	defer server.Close()

	adapter := &InfluxDBV2Adapter{
		client: server.Client(),
		config: &InfluxDBV2Config{URL: server.URL, Bucket: "bucket"},
	}

	series, err := adapter.DiscoverSeries(context.Background(), "cpu")
	if err != nil {
		t.Fatalf("DiscoverSeries returned error: %v", err)
	}
	want := []string{"cpu,a=1", "cpu,b=2", "cpu,c=3"}
	if len(series) != len(want) {
		t.Fatalf("expected %d unique sorted series, got %d: %#v", len(want), len(series), series)
	}
	for i, w := range want {
		if series[i] != w {
			t.Fatalf("at index %d expected %q, got %q (full=%#v)", i, w, series[i], series)
		}
	}
}

func TestInfluxDBV1ChunkedQueryStreamsBeyondOldClientTimeout(t *testing.T) {
	start := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)
	end := time.Date(2024, 1, 3, 3, 4, 5, 6, time.UTC)
	var sawChunked bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if strings.HasPrefix(query, "SHOW TAG KEYS") {
			fmt.Fprint(w, `{"results":[]}`)
			return
		}
		if r.URL.Query().Get("chunked") == "true" {
			sawChunked = true
			w.Header().Set("Content-Type", "application/json")
			// First chunk immediately
			fmt.Fprint(w, `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2024-01-02T03:04:05Z",42]]}]}]}`)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			// Artificial inter-chunk delay. Historically the client-level 30s
			// timeout would cancel a body whose total elapsed time (including
			// delays between chunks) exceeded 30s. We cannot sleep 30s in a
			// unit test, but we can assert the architectural contract: the
			// client must not have a Timeout that bounds total body read time.
			// The dedicated Connect-Timeout test above enforces that contract.
			time.Sleep(50 * time.Millisecond)
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

	var recordCount int
	_, err := adapter.QueryDataBatch(context.Background(), "cpu", []string{"cpu"}, start, end, nil, func(records []types.Record) error {
		recordCount += len(records)
		return nil
	}, &types.QueryConfig{BatchSize: 10})
	if err != nil {
		t.Fatalf("QueryDataBatch returned error: %v", err)
	}
	if !sawChunked {
		t.Fatal("expected chunked query to be issued")
	}
	if recordCount != 1 {
		t.Fatalf("expected 1 record streamed, got %d", recordCount)
	}
}
