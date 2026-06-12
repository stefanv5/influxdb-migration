package target

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

func TestInfluxDBV1FormatInfluxLineEscapesMeasurementAndPreservesZeroTimestamp(t *testing.T) {
	adapter := &InfluxDBV1TargetAdapter{}
	record := types.Record{
		Fields: map[string]any{"value": int64(1)},
		Tags:   map[string]string{},
		Time:   0,
	}

	got := adapter.formatInfluxLine("cpu load,main", record)
	want := `cpu\ load\,main value=1i 0`
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestInfluxDBV2WriteBatchSendsOrgBucketAndEscapedLineProtocol(t *testing.T) {
	var requestPath string
	var requestQuery string
	var requestBody string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath = r.URL.Path
		requestQuery = r.URL.RawQuery
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("failed to read request body: %v", err)
		}
		requestBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	adapter := &InfluxDBV2TargetAdapter{
		client: server.Client(),
		config: &InfluxDBV2TargetConfig{
			URL:    server.URL,
			Token:  "token",
			Org:    "org1",
			Bucket: "bucket1",
		},
	}
	record := types.Record{
		Fields: map[string]any{"value": int64(1)},
		Tags:   map[string]string{},
		Time:   0,
	}

	if err := adapter.WriteBatch(context.Background(), "cpu load,main", []types.Record{record}); err != nil {
		t.Fatalf("WriteBatch returned error: %v", err)
	}

	if requestPath != "/api/v2/write" {
		t.Fatalf("expected write path, got %q", requestPath)
	}
	if !strings.Contains(requestQuery, "org=org1") || !strings.Contains(requestQuery, "bucket=bucket1") {
		t.Fatalf("expected org and bucket query parameters, got %q", requestQuery)
	}

	wantBody := `cpu\ load\,main value=1i 0`
	if requestBody != wantBody {
		t.Fatalf("expected request body %q, got %q", wantBody, requestBody)
	}
}
