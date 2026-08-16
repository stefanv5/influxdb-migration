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

func TestInfluxDBFormatFieldValuePreservesIntegerAndFloatTypes(t *testing.T) {
	if got := formatFieldValue(int64(42)); got != "42i" {
		t.Fatalf("expected int64 field to use integer suffix, got %q", got)
	}
	if got := formatFieldValue(42.0); got != "42" {
		t.Fatalf("expected whole-number float to remain float line protocol, got %q", got)
	}
}

func TestInfluxDBV1TargetConnectRequiresDatabase(t *testing.T) {
	adapter := &InfluxDBV1TargetAdapter{}
	err := adapter.Connect(context.Background(), map[string]interface{}{
		"influxdb": map[string]interface{}{
			"url": "http://localhost:8086",
		},
	})
	if err == nil {
		t.Fatal("expected missing database error")
	}
	if !strings.Contains(err.Error(), "database is required") {
		t.Fatalf("expected database error, got %v", err)
	}
}

func TestInfluxDBV1TargetConnectCompleteConfig(t *testing.T) {
	adapter := &InfluxDBV1TargetAdapter{}
	err := adapter.Connect(context.Background(), map[string]interface{}{
		"database": "metrics",
		"influxdb": map[string]interface{}{
			"url": "http://localhost:8086",
		},
	})
	if err != nil {
		t.Fatalf("expected complete V1 config to connect, got %v", err)
	}
}

func TestInfluxDBV2TargetConnectRequiresTokenOrgAndBucket(t *testing.T) {
	adapter := &InfluxDBV2TargetAdapter{}
	err := adapter.Connect(context.Background(), map[string]interface{}{
		"influxdb": map[string]interface{}{
			"url": "http://localhost:8086",
		},
	})
	if err == nil {
		t.Fatal("expected missing V2 target fields error")
	}
	if !strings.Contains(err.Error(), "token is required") {
		t.Fatalf("expected token error first, got %v", err)
	}
}

func TestInfluxDBV2TargetConnectCompleteConfig(t *testing.T) {
	adapter := &InfluxDBV2TargetAdapter{}
	err := adapter.Connect(context.Background(), map[string]interface{}{
		"influxdb": map[string]interface{}{
			"url":    "http://localhost:8086",
			"token":  "token",
			"org":    "org",
			"bucket": "bucket",
		},
	})
	if err != nil {
		t.Fatalf("expected complete V2 config to connect, got %v", err)
	}
}

// TestInfluxDBFormatTagsDropsEmptyTagValue is a defensive safety-net test that
// documents the B1 reachability finding: in the influx->influx migration path,
// empty tag values never reach formatTags because pkg/types.Record.AddTag
// (pkg/types/record.go) drops any tag whose value is "" at construction time,
// and internal/engine/transform.FilterNulls also drops empty tags. As a result,
// an empty tag value is ALREADY gone before the target adapter sees the Record,
// so formatTags' own `v == ""` drop (internal/adapter/target/influxdb.go) is a
// redundant safety net, not the root cause of series-identity changes.
//
// Known upstream limitation: if a future source adapter populates Record.Tags
// directly (bypassing AddTag) with an empty value, that tag will be dropped and
// the target series identity will differ from the source. Fixing that requires
// changing pkg/types.Record.AddTag (shared, out of scope here) OR making
// formatTags preserve empty values by emitting `key=` — but InfluxDB server-side
// drops empty tag values on write anyway, so preservation is moot. See B1 in the
// task spec.
func TestInfluxDBFormatTagsDropsEmptyTagValue(t *testing.T) {
	// formatTags keeps a defensive drop of empty values. This test pins that
	// behavior so a future change to formatTags does not silently start
	// emitting `key=` (empty value) tags without an explicit decision.
	got := formatTags(map[string]string{"host": "server-a", "env": ""})
	want := "host=server-a"
	if got != want {
		t.Fatalf("formatTags should drop empty-value tags (safety net); expected %q, got %q", want, got)
	}
}

// TestInfluxDBRecordAddTagDropsEmptyValueUpstream documents the root cause of
// the B1 "empty tag dropped" behavior: it lives in pkg/types.Record.AddTag, not
// in the target adapter. This test reproduces the upstream drop so the link is
// visible from the target-side test file. Do NOT fix here (shared file, out of
// scope); this is a documentation/characterization test.
func TestInfluxDBRecordAddTagDropsEmptyValueUpstream(t *testing.T) {
	r := types.NewRecord()
	r.AddTag("host", "server-a")
	r.AddTag("env", "") // dropped by AddTag upstream

	if _, present := r.Tags["env"]; present {
		t.Fatalf("AddTag should drop empty-value tags upstream; env tag was unexpectedly retained")
	}
	if got := len(r.Tags); got != 1 {
		t.Fatalf("expected exactly 1 tag after AddTag drops empty, got %d (%v)", got, r.Tags)
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

// TestInfluxDBEscapeMeasurementEscapesBackslashFirst verifies that a literal
// backslash in a measurement name is escaped as `\\` BEFORE other characters,
// so a measurement like `cpu\load,main` is not mis-parsed as containing an
// escaped comma. Without backslash-first escaping, `\,` would be produced for
// the comma and the leading backslash would be ambiguous to the server parser.
func TestInfluxDBEscapeMeasurementEscapesBackslashFirst(t *testing.T) {
	got := escapeMeasurement(`cpu\load,main`)
	want := `cpu\\load\,main`
	if got != want {
		t.Fatalf("escapeMeasurement should escape backslash first: expected %q, got %q", want, got)
	}
}

// TestInfluxDBEscapeTagValueEscapesBackslashFirst verifies backslash escaping
// in tag values (and tag keys, since escapeTagValue is applied to both).
func TestInfluxDBEscapeTagValueEscapesBackslashFirst(t *testing.T) {
	// tag value with backslash + special chars
	got := escapeTagValue(`server\01,eu`)
	want := `server\\01\,eu`
	if got != want {
		t.Fatalf("escapeTagValue should escape backslash first: expected %q, got %q", want, got)
	}
	// tag key with backslash (escapeTagValue is reused for keys)
	gotKey := escapeTagValue(`host\name`)
	wantKey := `host\\name`
	if gotKey != wantKey {
		t.Fatalf("escapeTagValue should escape backslash in tag keys too: expected %q, got %q", wantKey, gotKey)
	}
}

// TestInfluxDBEscapeFieldKeyEscapesBackslashFirst verifies backslash escaping
// in field keys.
func TestInfluxDBEscapeFieldKeyEscapesBackslashFirst(t *testing.T) {
	got := escapeFieldKey(`val\ue`)
	want := `val\\ue`
	if got != want {
		t.Fatalf("escapeFieldKey should escape backslash first: expected %q, got %q", want, got)
	}
}

// TestInfluxDBFormatInfluxLineEscapesBackslashInMeasurementTagAndField is an
// end-to-end check via formatInfluxLine that a backslash in the measurement, a
// tag value, a tag key, and a field key all become `\\` in the emitted line.
func TestInfluxDBFormatInfluxLineEscapesBackslashInMeasurementTagAndField(t *testing.T) {
	adapter := &InfluxDBV1TargetAdapter{}
	record := types.Record{
		Fields: map[string]any{`val\ue`: int64(1)},
		Tags:   map[string]string{`host\name`: `server\01`},
		Time:   0,
	}
	got := adapter.formatInfluxLine(`cpu\load`, record)
	// measurement: cpu\load -> cpu\\load
	// tag key: host\name -> host\\name ; tag value: server\01 -> server\\01
	// field key: val\ue -> val\\ue
	want := `cpu\\load,host\\name=server\\01 val\\ue=1i 0`
	if got != want {
		t.Fatalf("formatInfluxLine should escape backslashes everywhere: expected %q, got %q", want, got)
	}
}

// TestInfluxDBEscapeStringFieldEscapesNewlineAndCR verifies that literal
// newlines and carriage returns inside string field values are escaped as the
// two-character sequences `\n` and `\r` (backslash + letter), per the InfluxDB
// line protocol spec. Without this, a string field containing a newline splits
// one logical point into two physical lines, producing a 400 that fails the
// whole batch. Verified against the official influxdata/line-protocol library's
// stringFieldEscaper, which escapes \t \n \f \r " and \.
func TestInfluxDBEscapeStringFieldEscapesNewlineAndCR(t *testing.T) {
	// "line1\nline2" (literal newline) must become the single physical token
	// `line1\nline2` (backslash + n), NOT two physical lines.
	got := escapeStringValue("line1\nline2\r")
	want := `line1\nline2\r`
	if got != want {
		t.Fatalf("escapeStringValue should escape newline/CR as backslash-n/backslash-r: expected %q, got %q", want, got)
	}
}

// TestInfluxDBEscapeStringFieldDoesNotDoubleEscapeBackslash verifies the escape
// ordering: backslash is escaped FIRST, so a backslash-n already present in the
// input is NOT re-escaped into `\\n` (which would decode to a literal backslash
// + n instead of a newline). The official library uses strings.NewReplacer
// (single pass, no double-escape); our sequential ReplaceAll must match by
// doing backslash first.
func TestInfluxDBEscapeStringFieldDoesNotDoubleEscapeBackslash(t *testing.T) {
	// input: literal backslash followed by 'n' (two chars: \ n)
	got := escapeStringValue(`line1\nline2`)
	want := `line1\\nline2` // backslash -> \\ , then 'n' stays; NOT \\n
	if got != want {
		t.Fatalf("escapeStringValue must escape backslash first and not double-escape: expected %q, got %q", want, got)
	}
}

// TestInfluxDBFormatInfluxLineWithStringFieldContainingNewlineIsOneLine is the
// end-to-end guarantee: a record whose string field contains a real newline
// produces exactly one physical line (no embedded newline in the output), with
// the newline encoded as the two-char sequence `\n`.
func TestInfluxDBFormatInfluxLineWithStringFieldContainingNewlineIsOneLine(t *testing.T) {
	adapter := &InfluxDBV1TargetAdapter{}
	record := types.Record{
		Fields: map[string]any{"msg": "line1\nline2"},
		Tags:   map[string]string{"host": "a"},
		Time:   0,
	}
	got := adapter.formatInfluxLine("cpu", record)
	want := `cpu,host=a msg="line1\nline2" 0`
	if got != want {
		t.Fatalf("expected single-line escaped output %q, got %q", want, got)
	}
	if strings.Contains(got, "\n") {
		t.Fatalf("output must not contain a literal newline; got %q", got)
	}
}

// TestInfluxDBV1WriteBatchSurfacesErrorBodyOn2xxDefenseInDepth exercises the
// 2xx-with-error-body branch of writeLines. NOTE: this is NOT real InfluxDB V1
// behavior. InfluxDB V1 /write returns HTTP 400 (not 2xx) for partial-write
// rejections such as field-type conflict or points beyond retention policy;
// that real path is covered by TestInfluxDBV1WriteBatchSurfacesPartialWriteErrorOn400.
//
// The 2xx body-parse path kept here is a DEFENSE-IN-DEPTH guard: a
// non-conforming proxy, middleware, or a future InfluxDB build might return 2xx
// with an error body. If that happens, WriteBatch must still surface the error
// instead of silently treating 2xx as full success. Without the guard, such a
// response would be invisible and the engine would count every source row as
// written.
func TestInfluxDBV1WriteBatchSurfacesErrorBodyOn2xxDefenseInDepth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Drain the request body so the server can respond cleanly.
		io.Copy(io.Discard, r.Body)
		// Synthetic case: 2xx WITH a JSON error body. Real InfluxDB V1 does NOT
		// do this (it returns 400); this mocks a non-conforming proxy/middleware
		// that wraps the upstream 400 as a 200 while preserving the error body.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"results":[],"error":"partial write error: field type conflict: input field \"value\" on measurement \"cpu\" is type int64, already exists as type string"}`))
	}))
	defer server.Close()

	adapter := &InfluxDBV1TargetAdapter{
		client:  server.Client(),
		baseURL: server.URL,
		config: &InfluxDBV1TargetConfig{
			URL:      server.URL,
			Database: "metrics",
		},
	}
	record := types.Record{
		Fields: map[string]any{"value": int64(1)},
		Tags:   map[string]string{},
		Time:   0,
	}

	err := adapter.WriteBatch(context.Background(), "cpu", []types.Record{record})
	if err == nil {
		t.Fatal("expected WriteBatch to surface the partial-write error body, got nil")
	}
	if !strings.Contains(err.Error(), "partial write error") {
		t.Fatalf("expected error to contain the partial-write message, got %v", err)
	}
	if !strings.Contains(err.Error(), "field type conflict") {
		t.Fatalf("expected error to contain the underlying conflict detail, got %v", err)
	}
}

// TestInfluxDBV1WriteBatchSurfacesPartialWriteErrorOn400 covers the REAL
// InfluxDB V1 partial-write path. InfluxDB V1 /write returns HTTP 400 with a
// JSON body of the form {"error":"partial write: ..."} for partial-write
// rejections (field-type conflict, points beyond retention policy, etc.); a
// successful write returns 204 No Content with an empty body.
//
// The 4xx branch of writeLines reads the body and returns
// "write failed with status 400: <body>". The engine's isRetryableWriteError
// then classifies 400 (a 4xx, non-429) as permanent/non-retryable, so
// writeWithRetry returns the error immediately rather than retrying. This test
// pins that end-to-end visibility: the 400 status AND the error message must
// both survive into the returned error so the operator can see why points were
// rejected.
func TestInfluxDBV1WriteBatchSurfacesPartialWriteErrorOn400(t *testing.T) {
	// Body is intentionally short (<200 chars) so the 4xx branch's 200-char
	// truncation cannot mask the message under test.
	const errorBody = `{"error":"partial write: points beyond retention policy dropped=3"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(errorBody))
	}))
	defer server.Close()

	adapter := &InfluxDBV1TargetAdapter{
		client:  server.Client(),
		baseURL: server.URL,
		config: &InfluxDBV1TargetConfig{
			URL:      server.URL,
			Database: "metrics",
		},
	}
	record := types.Record{
		Fields: map[string]any{"value": int64(1)},
		Tags:   map[string]string{},
		Time:   0,
	}

	err := adapter.WriteBatch(context.Background(), "cpu", []types.Record{record})
	if err == nil {
		t.Fatal("expected WriteBatch to surface the 400 partial-write error, got nil")
	}
	if !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("expected error to contain the HTTP status (status 400), got %v", err)
	}
	if !strings.Contains(err.Error(), "partial write") {
		t.Fatalf("expected error to contain the partial-write message, got %v", err)
	}
	if !strings.Contains(err.Error(), "retention policy") {
		t.Fatalf("expected error to contain the retention-policy detail, got %v", err)
	}
}

// TestInfluxDBV1WriteBatchReturnsNilOn204WithEmptyBody verifies the normal V1
// success path is unaffected: 204 with an empty body returns nil.
func TestInfluxDBV1WriteBatchReturnsNilOn204WithEmptyBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	adapter := &InfluxDBV1TargetAdapter{
		client:  server.Client(),
		baseURL: server.URL,
		config: &InfluxDBV1TargetConfig{
			URL:      server.URL,
			Database: "metrics",
		},
	}
	record := types.Record{
		Fields: map[string]any{"value": int64(1)},
		Tags:   map[string]string{},
		Time:   0,
	}

	if err := adapter.WriteBatch(context.Background(), "cpu", []types.Record{record}); err != nil {
		t.Fatalf("expected nil error on 204 with empty body, got %v", err)
	}
}

// TestInfluxDBV1WriteBatchReturnsNilOn200WithEmptyBody verifies that a 200
// response with an EMPTY body (no error JSON) is treated as full success — the
// partial-write parsing path must only trigger when the body actually contains
// an error.
func TestInfluxDBV1WriteBatchReturnsNilOn200WithEmptyBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	adapter := &InfluxDBV1TargetAdapter{
		client:  server.Client(),
		baseURL: server.URL,
		config: &InfluxDBV1TargetConfig{
			URL:      server.URL,
			Database: "metrics",
		},
	}
	record := types.Record{
		Fields: map[string]any{"value": int64(1)},
		Tags:   map[string]string{},
		Time:   0,
	}

	if err := adapter.WriteBatch(context.Background(), "cpu", []types.Record{record}); err != nil {
		t.Fatalf("expected nil error on 200 with empty body, got %v", err)
	}
}

// TestInfluxDBV2WriteBatchReturnsNilOn204WithEmptyBody documents the B4 V2
// limitation: InfluxDB 2.x returns 204 with NO body even when it silently drops
// points (field-type conflict, limits). The HTTP layer cannot reveal per-point
// drops, so 204 + empty body must return nil (accepted). Full per-point
// reconciliation is an engine-side concern, out of scope for this target fix.
func TestInfluxDBV2WriteBatchReturnsNilOn204WithEmptyBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		// V2 returns 204 with empty body on accepted-but-partially-dropped.
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

	if err := adapter.WriteBatch(context.Background(), "cpu", []types.Record{record}); err != nil {
		t.Fatalf("V2 204 with empty body must return nil (accepted); partial drops are not visible at the HTTP layer. got %v", err)
	}
}
