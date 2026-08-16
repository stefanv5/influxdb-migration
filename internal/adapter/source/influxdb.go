package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/migration-tools/influx-migrator/internal/adapter"
	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/migration-tools/influx-migrator/pkg/types"
	"go.uber.org/zap"
)

type InfluxDBV1Adapter struct {
	client  *http.Client
	config  *InfluxDBV1Config
	baseURL string

	// tagKeyCache memoizes DiscoverTagKeys results per measurement so that
	// repeated QueryDataBatch calls (one per time window/series batch) do not
	// re-issue SHOW TAG KEYS on every batch. SHOW TAG KEYS returns
	// whole-measurement metadata, so the result is stable for a measurement.
	tagKeyCache map[string][]string
}

type InfluxDBV1Config struct {
	URL      string
	Username string
	Password string
	Database string
	SSL      types.SSLConfig
}

type influxV1Result struct {
	Results []influxV1ResultSeries `json:"results"`
	Error   string                 `json:"error"` // V1 API error message
}

type influxV1ResultSeries struct {
	StatementID int              `json:"statement_id"`
	Series      []influxV1Series `json:"series"`
	Error       string           `json:"error"`
}

type influxV1Series struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags"`
	Columns []string          `json:"columns"`
	Values  [][]interface{}   `json:"values"`
}

func decodeInfluxV1Result(body []byte, result *influxV1Result) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	return decoder.Decode(result)
}

func influxNumberValue(n json.Number) (any, error) {
	text := n.String()
	if strings.ContainsAny(text, ".eE") {
		v, err := n.Float64()
		if err != nil {
			return nil, err
		}
		return v, nil
	}
	v, err := n.Int64()
	if err != nil {
		return nil, err
	}
	return v, nil
}

func addInfluxField(record *types.Record, field string, value any) error {
	switch v := value.(type) {
	case json.Number:
		converted, err := influxNumberValue(v)
		if err != nil {
			return fmt.Errorf("failed to preserve InfluxDB numeric field %q value %q: %w", field, v.String(), err)
		}
		record.AddField(field, converted)
	case float64:
		record.AddField(field, v)
	case string:
		record.AddField(field, v)
	case bool:
		record.AddField(field, v)
	case int64:
		record.AddField(field, v)
	case int:
		record.AddField(field, int64(v))
	}
	return nil
}

func init() {
	adapter.RegisterSourceAdapter("influxdb-v1", func() adapter.SourceAdapter {
		return &InfluxDBV1Adapter{}
	})
}

// sourceHTTPResponseHeaderTimeout bounds the time the HTTP client waits for
// response headers. Unlike http.Client.Timeout, it does NOT cover reading the
// response body, so chunked query streams can take arbitrarily long while a
// slow batchFunc writes to the target. Overall request lifetime is governed by
// the per-request context.Context threaded through every query method.
const sourceHTTPResponseHeaderTimeout = 30 * time.Second

// seriesPaginationBatchSize is the SHOW SERIES page size used by
// DiscoverSeries. It is a package-level variable (rather than a const) so
// tests can override it to exercise multi-page pagination without emitting
// 10000 rows. Production code uses the default of 10000.
var seriesPaginationBatchSize = 10000

// dedupAndSortSeries returns the sorted unique keys from the input slice.
// It is the defensive core of DiscoverSeries (A7): SHOW SERIES pagination
// assumes lexicographic ordering, but a multi-shard response can return
// non-lexicographic or duplicate keys. Deduplicating into a set and returning
// sorted unique keys makes the result internally consistent and prevents
// duplicate series from being emitted to the target.
func dedupAndSortSeries(keys []string) []string {
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	out := keysFromSet(set)
	sort.Strings(out)
	return out
}

// keysFromSet returns the keys of the set as a slice. The order is
// non-deterministic; callers that need ordering should sort the result.
func keysFromSet(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

func (a *InfluxDBV1Adapter) Name() string {
	return "influxdb-v1"
}

func (a *InfluxDBV1Adapter) SupportedVersions() []string {
	return []string{"1.x"}
}

func (a *InfluxDBV1Adapter) Connect(ctx context.Context, config map[string]interface{}) error {
	cfg := &InfluxDBV1Config{}
	if err := decodeInfluxV1Config(config, cfg); err != nil {
		return err
	}
	a.config = cfg
	a.baseURL = cfg.URL

	transport := &http.Transport{
		// ResponseHeaderTimeout bounds only the time waiting for response
		// headers. The body is allowed to stream indefinitely so chunked
		// query responses (which may feed a slow batchFunc) are not
		// cancelled mid-stream. Overall cancellation is driven by the
		// per-request context.Context threaded through every query method.
		// Do NOT set http.Client.Timeout here: Go's client Timeout covers
		// reading the response body and would abort long migrations.
		ResponseHeaderTimeout: sourceHTTPResponseHeaderTimeout,
	}
	if cfg.SSL.Enabled && cfg.SSL.SkipVerify {
		// Require explicit opt-in via environment variable for insecure TLS
		if os.Getenv("ALLOW_INSECURE_TLS") != "1" {
			logger.Error("TLS certificate verification is disabled - set ALLOW_INSECURE_TLS=1 environment variable to allow",
				zap.String("url", cfg.URL))
			return fmt.Errorf("insecure TLS requires ALLOW_INSECURE_TLS=1 environment variable")
		}
		logger.Warn("TLS certificate verification is disabled - this is insecure and not recommended for production use",
			zap.String("url", cfg.URL))
		transport.TLSClientConfig.InsecureSkipVerify = true
	}
	a.client = &http.Client{
		Transport: transport,
		// Timeout intentionally left zero (indefinite) so chunked response
		// bodies can stream beyond 30s while a slow batchFunc writes to the
		// target. Per-request context.Context governs cancellation.
	}

	return nil
}

func (a *InfluxDBV1Adapter) Disconnect(ctx context.Context) error {
	if a.client != nil {
		a.client.CloseIdleConnections()
	}
	return nil
}

func (a *InfluxDBV1Adapter) Ping(ctx context.Context) error {
	u, err := url.Parse(a.baseURL)
	if err != nil {
		return fmt.Errorf("invalid base URL: %w", err)
	}
	u.Path = "/ping"

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return err
	}

	if a.config.Username != "" {
		req.SetBasicAuth(a.config.Username, a.config.Password)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("ping failed with status: %d", resp.StatusCode)
	}

	return nil
}

func (a *InfluxDBV1Adapter) DiscoverTables(ctx context.Context) ([]string, error) {
	query := "SHOW MEASUREMENTS"
	results, err := a.executeQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	var measurements []string
	for _, result := range results {
		for _, values := range result.Values {
			if len(values) > 0 {
				if name, ok := values[0].(string); ok {
					measurements = append(measurements, name)
				}
			}
		}
	}

	return measurements, nil
}

func (a *InfluxDBV1Adapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
	// Collect all series keys into a dedup set. SHOW SERIES pagination uses
	// `series_key > lastKey`, which assumes lexicographic ordering. A
	// multi-shard response can return non-lexicographic order, causing the
	// predicate to skip legitimately unseen keys or re-emit already-seen keys.
	// Deduplicating into a set and returning sorted unique keys makes the
	// result internally consistent regardless of the engine's ordering, and
	// prevents duplicate series from being emitted (which would cause
	// redundant work and duplicate writes on the target).
	seriesSet := make(map[string]struct{})
	var lastKey string
	batchSize := seriesPaginationBatchSize

	for {
		var query string
		if lastKey == "" {
			query = fmt.Sprintf("SHOW SERIES FROM %s LIMIT %d",
				influxQuoteIdentifier(measurement), batchSize)
		} else {
			// InfluxDB 1.7+ supports WHERE series_key > for pagination
			query = fmt.Sprintf("SHOW SERIES FROM %s WHERE series_key > '%s' LIMIT %d",
				influxQuoteIdentifier(measurement), lastKey, batchSize)
		}

		results, err := a.executeQuery(ctx, query)
		if err != nil {
			// If pagination query fails (older InfluxDB), fall back to a single
			// non-paginated query for ALL series. Dedup handles any overlap with
			// keys already collected, so no `key <= lastKey` skip is needed.
			if lastKey != "" {
				logger.Warn("series_key pagination not supported, falling back to non-paginated SHOW SERIES",
					zap.Error(err))
				fallbackQuery := fmt.Sprintf("SHOW SERIES FROM %s", influxQuoteIdentifier(measurement))
				fallbackResults, fallbackErr := a.executeQuery(ctx, fallbackQuery)
				if fallbackErr != nil {
					return nil, fallbackErr
				}
				for _, result := range fallbackResults {
					for _, values := range result.Values {
						if len(values) > 0 {
							if key, ok := values[0].(string); ok {
								seriesSet[key] = struct{}{}
							}
						}
					}
				}
				return dedupAndSortSeries(keysFromSet(seriesSet)), nil
			}
			return nil, err
		}

		batchCount := 0
		for _, result := range results {
			for _, values := range result.Values {
				if len(values) > 0 {
					if key, ok := values[0].(string); ok {
						seriesSet[key] = struct{}{}
						lastKey = key
						batchCount++
					}
				}
			}
		}

		// If returned fewer than batch size, we're done
		if batchCount < batchSize {
			break
		}

		// Check context before continuing
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	return dedupAndSortSeries(keysFromSet(seriesSet)), nil
}

func (a *InfluxDBV1Adapter) DiscoverShardGroups(ctx context.Context) ([]*adapter.ShardGroup, error) {
	query := "SHOW SHARDS"
	results, err := a.executeQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	// Group by shard_group ID, extract time range.
	// A shard whose start or end time cannot be parsed is skipped with a
	// warning so one malformed shard does not abort discovery; storing a
	// zero-start/end shard would instead produce empty time windows and
	// silently no-op the migration. If every shard fails to parse, return an
	// error so the caller does not mistake an empty result for "no shards".
	shardGroups := make(map[int]*adapter.ShardGroup)
	totalShards := 0
	skippedShards := 0
	for _, result := range results {
		for _, values := range result.Values {
			if len(values) < 6 {
				continue
			}
			totalShards++
			// Parse: id, database, retention_policy, shard_group, start_time, end_time
			shardGroupID := parseInt(values[3])
			startTime, startErr := parseTime(values[4])
			endTime, endErr := parseTime(values[5])
			if startErr != nil || endErr != nil {
				skippedShards++
				logger.Warn("skipping shard with unparseable time bounds",
					zap.Int("shard_group_id", shardGroupID),
					zap.Any("start_value", values[4]),
					zap.Any("end_value", values[5]),
					zap.NamedError("start_error", startErr),
					zap.NamedError("end_error", endErr))
				continue
			}

			if _, exists := shardGroups[shardGroupID]; !exists {
				shardGroups[shardGroupID] = &adapter.ShardGroup{
					ID:        shardGroupID,
					StartTime: startTime,
					EndTime:   endTime,
				}
			}
		}
	}

	if totalShards > 0 && skippedShards == totalShards {
		return nil, fmt.Errorf("all %d shard(s) failed to parse time bounds during SHOW SHARDS", totalShards)
	}

	var result []*adapter.ShardGroup
	for _, sg := range shardGroups {
		result = append(result, sg)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].StartTime.Before(result[j].StartTime)
	})
	return result, nil
}

func (a *InfluxDBV1Adapter) DiscoverSeriesInTimeWindow(ctx context.Context, measurement string, startTime, endTime time.Time) ([]string, error) {
	// Note: InfluxDB SHOW SERIES does not support time-based WHERE filtering.
	// The time parameters are accepted for interface compatibility but ignored.
	// All series for the measurement are returned.
	logger.Debug("DiscoverSeriesInTimeWindow using all-series fallback; point queries remain time-bounded",
		zap.String("measurement", measurement),
		zap.Time("window_start", startTime),
		zap.Time("window_end", endTime))
	query := fmt.Sprintf(`SHOW SERIES FROM %s`, influxQuoteIdentifier(measurement))

	series, err := a.executeShowSeries(ctx, query)
	if err != nil {
		return nil, err
	}

	return series, nil
}

// DiscoverTagKeys returns tag keys for V1 adapter using SHOW TAG KEYS.
// This allows proper distinction between tags and fields when parsing query results.
// Results are cached per measurement for the lifetime of the adapter because
// SHOW TAG KEYS returns whole-measurement metadata that is stable across
// batches; re-issuing it per QueryDataBatch call wastes a round-trip and can
// return an incomplete set if a shard is transiently offline.
func (a *InfluxDBV1Adapter) DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error) {
	if cached, ok := a.lookupCachedTagKeys(measurement); ok {
		return cached, nil
	}
	query := fmt.Sprintf("SHOW TAG KEYS FROM %s", influxQuoteIdentifier(measurement))
	results, err := a.executeQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("discover tag keys for measurement %q failed: %w", measurement, err)
	}

	var tagKeys []string
	for _, result := range results {
		for _, values := range result.Values {
			if len(values) > 0 {
				if key, ok := values[0].(string); ok {
					tagKeys = append(tagKeys, key)
				}
			}
		}
	}
	a.storeCachedTagKeys(measurement, tagKeys)
	return tagKeys, nil
}

// resolveTagKeySet returns the tag-key set to use for a QueryData(Batch) call.
// It prefers caller-supplied cfg.TagKeys (authoritative, avoids any network
// round-trip), then falls back to DiscoverTagKeys (cached on the adapter).
// Returning a non-nil set keeps the tag/field classification path consistent.
func (a *InfluxDBV1Adapter) resolveTagKeySet(ctx context.Context, measurement string, cfg *types.QueryConfig) (map[string]bool, []string, error) {
	if cfg != nil && len(cfg.TagKeys) > 0 {
		set := make(map[string]bool, len(cfg.TagKeys))
		for _, k := range cfg.TagKeys {
			set[k] = true
		}
		return set, cfg.TagKeys, nil
	}
	tagKeys, err := a.DiscoverTagKeys(ctx, measurement)
	if err != nil {
		return nil, nil, err
	}
	set := make(map[string]bool, len(tagKeys))
	for _, k := range tagKeys {
		set[k] = true
	}
	return set, tagKeys, nil
}

// lookupCachedTagKeys returns cached tag keys for a measurement, ok=false if absent.
func (a *InfluxDBV1Adapter) lookupCachedTagKeys(measurement string) ([]string, bool) {
	if a.tagKeyCache == nil {
		return nil, false
	}
	cached, ok := a.tagKeyCache[measurement]
	if !ok {
		return nil, false
	}
	// Return a copy so callers cannot mutate the cached slice.
	out := make([]string, len(cached))
	copy(out, cached)
	return out, true
}

// storeCachedTagKeys memoizes tag keys for a measurement (immutable copy).
func (a *InfluxDBV1Adapter) storeCachedTagKeys(measurement string, tagKeys []string) {
	if a.tagKeyCache == nil {
		a.tagKeyCache = make(map[string][]string)
	}
	stored := make([]string, len(tagKeys))
	copy(stored, tagKeys)
	a.tagKeyCache[measurement] = stored
}

func (a *InfluxDBV1Adapter) executeShowSeries(ctx context.Context, query string) ([]string, error) {
	results, err := a.executeQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	var series []string
	for _, result := range results {
		for _, values := range result.Values {
			if len(values) > 0 {
				if key, ok := values[0].(string); ok {
					series = append(series, key)
				}
			}
		}
	}

	return series, nil
}

// parseInt parses an interface{} to int
func parseInt(v interface{}) int {
	switch val := v.(type) {
	case json.Number:
		// Prefer exact integer parsing; fall back to float if the value is
		// numeric but not integral (shard_group IDs are integral, but be
		// defensive against any encoded-as-float value).
		if i, err := val.Int64(); err == nil {
			return int(i)
		}
		if f, err := val.Float64(); err == nil {
			return int(f)
		}
	case float64:
		return int(val)
	case int64:
		return int(val)
	case int:
		return val
	case string:
		var i int
		fmt.Sscanf(val, "%d", &i)
		return i
	}
	return 0
}

// parseTime parses an interface{} to time.Time. It returns an error when the
// value cannot be parsed so callers can skip the shard (rather than silently
// storing a zero start/end that would produce empty time windows downstream
// and no-op a migration). Numeric inputs (seconds or nanoseconds) are always
// parseable and never return an error.
func parseTime(v interface{}) (time.Time, error) {
	switch val := v.(type) {
	case string:
		// Try RFC3339Nano first
		if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
			return t, nil
		}
		// Try RFC3339
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t, nil
		}
		// Both parses failed. Use the RFC3339 error as the wrapped cause.
		_, cause := time.Parse(time.RFC3339, val)
		return time.Time{}, fmt.Errorf("failed to parse shard time %q: %w", val, cause)
	case float64:
		// Unix timestamp in seconds or nanoseconds
		if val > 1e12 {
			// Likely nanoseconds
			return time.Unix(0, int64(val)), nil
		}
		// Likely seconds
		return time.Unix(int64(val), 0), nil
	case int64:
		if val > 1e12 {
			return time.Unix(0, val), nil
		}
		return time.Unix(val, 0), nil
	case int:
		if int64(val) > 1e12 {
			return time.Unix(0, int64(val)), nil
		}
		return time.Unix(int64(val), 0), nil
	}
	return time.Time{}, fmt.Errorf("failed to parse shard time: unsupported type %T", v)
}

// effectiveQueryBounds computes the start/end time bounds for a single-mode
// (non-batch) QueryData call. Unlike QueryDataBatch, single-mode does not
// receive an explicit endTime argument, so the end bound must come from
// cfg.EndTime. If no end time is configured, this returns an error rather than
// silently baking end=now+1h, which would truncate the migration tail for any
// migration running longer than one hour after start (silent data loss).
//
// lastTS (from the checkpoint) seeds the start bound when cfg.StartTime is
// unset, supporting incremental single-mode resumption.
func effectiveQueryBounds(lastTS int64, cfg *types.QueryConfig) (string, string, error) {
	var start time.Time
	if cfg != nil && !cfg.StartTime.IsZero() {
		start = cfg.StartTime
	} else if lastTS != 0 {
		start = time.Unix(0, lastTS)
	}

	startTime := "1970-01-01T00:00:00Z"
	if !start.IsZero() {
		startTime = start.Format(time.RFC3339Nano)
	}

	if cfg == nil || cfg.EndTime.IsZero() {
		return "", "", fmt.Errorf("single-mode migration requires an explicit end time (time_range.end) to avoid tail data loss")
	}
	end := cfg.EndTime

	return startTime, end.Format(time.RFC3339Nano), nil
}

func checkInfluxV1ResultError(result influxV1Result) error {
	if result.Error != "" {
		return fmt.Errorf("V1 query error: %s", result.Error)
	}
	for _, statement := range result.Results {
		if statement.Error != "" {
			return fmt.Errorf("V1 query error: %s", statement.Error)
		}
	}
	return nil
}

func (a *InfluxDBV1Adapter) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
	// InfluxDB is a time-series database with schemaless writes.
	// Return a minimal schema with just the measurement name.
	// Actual field/tag discovery is done through queries.
	return &types.TableSchema{
		TableName: table,
		Columns:   []types.Column{},
	}, nil
}

func (a *InfluxDBV1Adapter) QueryData(ctx context.Context, measurement string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	var lastTS int64
	var totalProcessed int64

	if lastCheckpoint != nil {
		lastTS = lastCheckpoint.LastTimestamp
		totalProcessed = lastCheckpoint.ProcessedRows
	}

	startTime, endTime, err := effectiveQueryBounds(lastTS, cfg)
	if err != nil {
		return nil, err
	}

	chunkSize := 10000
	if cfg != nil && cfg.BatchSize > 0 {
		chunkSize = cfg.BatchSize
	}

	// Build query without LIMIT - InfluxDB will return data in chunks
	query := fmt.Sprintf(`SELECT * FROM %s WHERE time >= '%s' AND time < '%s'`,
		influxQuoteIdentifier(measurement), startTime, endTime)

	logger.Debug("executing chunked query for InfluxDB V1",
		zap.String("measurement", measurement),
		zap.Int("chunk_size", chunkSize),
		zap.String("query_start", startTime),
		zap.String("query_end", endTime))

	var totalRecords int
	var maxTS int64

	// Resolve tag keys: prefer caller-supplied cfg.TagKeys, else DiscoverTagKeys
	// (cached per measurement on the adapter).
	tagKeySet, _, tagErr := a.resolveTagKeySet(ctx, measurement, cfg)
	if tagErr != nil {
		return nil, tagErr
	}

	// Use chunked query - InfluxDB automatically splits response into chunks
	err = a.executeChunkedQuery(ctx, query, chunkSize, func(records []types.Record) error {
		totalRecords += len(records)
		totalProcessed += int64(len(records))
		for _, record := range records {
			if record.Time > maxTS {
				maxTS = record.Time
			}
		}
		return batchFunc(records)
	}, tagKeySet)

	if err != nil {
		return nil, fmt.Errorf("chunked query failed: %w", err)
	}

	logger.Info("completed chunked query for InfluxDB V1",
		zap.String("measurement", measurement),
		zap.Int("total_records", totalRecords))

	return &types.Checkpoint{
		LastTimestamp: maxTS,
		ProcessedRows: totalProcessed,
	}, nil
}

func (a *InfluxDBV1Adapter) QueryDataBatch(ctx context.Context, measurement string,
	series []string, startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
	batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

	// Determine effective start time - always use the original startTime in batch mode
	// The lastCheckpoint.LastTimestamp is for progress tracking only, not for
	// modifying query parameters. Each batch queries its full assigned time range.
	queryStart := startTime

	chunkSize := getBatchSize(cfg)

	// Resolve tag keys: prefer caller-supplied cfg.TagKeys, else DiscoverTagKeys
	// (cached per measurement on the adapter).
	tagKeySet, tagKeys, tagErr := a.resolveTagKeySet(ctx, measurement, cfg)
	if tagErr != nil {
		return nil, tagErr
	}
	whereClause := BuildWhereClauseWithTagKeys(series, tagKeys)

	// Build query without LIMIT - InfluxDB will return data in chunks
	query := fmt.Sprintf("SELECT * FROM %s WHERE (%s) AND time >= '%s' AND time < '%s'",
		influxQuoteIdentifier(measurement),
		whereClause,
		queryStart.Format(time.RFC3339Nano),
		endTime.Format(time.RFC3339Nano))

	logger.Debug("executing chunked query for InfluxDB V1",
		zap.String("measurement", measurement),
		zap.Int("series_count", len(series)),
		zap.Int("chunk_size", chunkSize),
		zap.String("query_start", queryStart.Format(time.RFC3339Nano)),
		zap.String("query_end", endTime.Format(time.RFC3339Nano)))

	var totalRecords int
	var maxTS int64

	// Use chunked query - InfluxDB automatically splits response into chunks
	err := a.executeChunkedQuery(ctx, query, chunkSize, func(records []types.Record) error {
		totalRecords += len(records)
		for _, record := range records {
			if record.Time > maxTS {
				maxTS = record.Time
			}
		}
		return batchFunc(records)
	}, tagKeySet)

	if err != nil {
		return nil, fmt.Errorf("chunked query failed: %w", err)
	}

	logger.Info("completed chunked query for InfluxDB V1",
		zap.String("measurement", measurement),
		zap.Int("series_count", len(series)),
		zap.Int("total_records", totalRecords),
		zap.Int64("max_timestamp", maxTS))

	return &types.Checkpoint{
		LastTimestamp: maxTS,
		ProcessedRows: int64(totalRecords),
	}, nil
}

func (a *InfluxDBV1Adapter) executeSelectQuery(ctx context.Context, query string) ([]types.Record, error) {
	params := url.Values{}
	params.Set("q", query)
	params.Set("db", a.config.Database)

	if a.config.Username != "" {
		params.Set("u", a.config.Username)
		params.Set("p", a.config.Password)
	}

	u, err := url.Parse(a.baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}
	u.Path = "/query"
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.client.Do(req)
	if err != nil {
		// resp may be non-nil even when err is set (e.g., redirect error)
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return nil, fmt.Errorf("V1 select query request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read V1 response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("V1 query failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result influxV1Result
	if err := decodeInfluxV1Result(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	// Check for V1 API-level errors
	if err := checkInfluxV1ResultError(result); err != nil {
		return nil, err
	}

	var records []types.Record
	for _, r := range result.Results {
		for _, series := range r.Series {
			for _, values := range series.Values {
				record, err := a.parseValues(series.Columns, values, series.Tags, nil)
				if err != nil {
					return nil, err
				}
				records = append(records, *record)
			}
		}
	}

	return records, nil
}

// executeChunkedQuery 执行chunked查询，流式处理每个chunk
// 适用于 SELECT * 查询，让InfluxDB自动按chunk_size分块返回
// 注意：InfluxDB的chunked响应是多个JSON数组，每个数组是一个chunk
func (a *InfluxDBV1Adapter) executeChunkedQuery(
	ctx context.Context,
	query string,
	chunkSize int,
	batchFunc func([]types.Record) error,
	tagKeySet map[string]bool,
) error {
	params := url.Values{}
	params.Set("q", query)
	params.Set("db", a.config.Database)
	params.Set("chunked", "true")
	params.Set("chunk_size", strconv.Itoa(chunkSize))

	if a.config.Username != "" {
		params.Set("u", a.config.Username)
		params.Set("p", a.config.Password)
	}

	u, err := url.Parse(a.baseURL)
	if err != nil {
		return fmt.Errorf("invalid base URL: %w", err)
	}
	u.Path = "/query"
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	logger.Debug("executing chunked query for InfluxDB V1",
		zap.String("url", redactURL(u)))

	resp, err := a.client.Do(req)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return fmt.Errorf("chunked query request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("query failed with status %d", resp.StatusCode)
		}
		return fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	// 使用json.Decoder流式读取chunked响应
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	totalRecords := 0
	chunkIndex := 0

	for {
		chunkIndex++
		// 读取一个chunk - InfluxDB返回的是序列数组格式
		// 每个chunk是一个influxV1Result对象
		var result influxV1Result
		if err := decoder.Decode(&result); err != nil {
			if err == io.EOF {
				logger.Debug("chunked query EOF",
					zap.Int("chunk_index", chunkIndex),
					zap.Int("total_records", totalRecords))
				break
			}
			return fmt.Errorf("decode chunk failed: %w", err)
		}

		// 检查V1 API错误
		if err := checkInfluxV1ResultError(result); err != nil {
			return err
		}

		// 解析chunk为records
		records := make([]types.Record, 0)
		seriesCount := 0
		for _, r := range result.Results {
			seriesCount += len(r.Series)
			for _, series := range r.Series {
				for _, values := range series.Values {
					record, err := a.parseValues(series.Columns, values, series.Tags, tagKeySet)
					if err != nil {
						return err
					}
					records = append(records, *record)
				}
			}
		}

		logger.Debug("chunked query chunk processed",
			zap.Int("chunk_index", chunkIndex),
			zap.Int("series_count", seriesCount),
			zap.Int("records_in_chunk", len(records)),
			zap.Int("total_records_so_far", totalRecords))

		// 处理records
		if len(records) > 0 {
			if err := batchFunc(records); err != nil {
				return fmt.Errorf("batch func failed: %w", err)
			}
			totalRecords += len(records)
		}
	}

	logger.Debug("completed chunked query for InfluxDB V1",
		zap.Int("total_records", totalRecords))

	return nil
}

// isTagColumn decides whether a non-time column holds a tag value.
// A column is a tag when it appears in the measurement-wide tagKeySet
// (populated from DiscoverTagKeys / SHOW TAG KEYS), OR when it is a key in
// seriesTags (the authoritative per-series tag set carried by the InfluxDB
// series object). The seriesTags cross-check defends against an incomplete
// DiscoverTagKeys result (e.g. an offline shard during SHOW TAG KEYS), which
// would otherwise misclassify a tag column as a string field and corrupt
// series identity on the target.
func isTagColumn(col string, tagKeySet map[string]bool, seriesTags map[string]string) bool {
	if tagKeySet != nil && tagKeySet[col] {
		return true
	}
	if seriesTags != nil {
		if _, ok := seriesTags[col]; ok {
			return true
		}
	}
	return false
}

// parseInfluxTimestamp parses an InfluxDB timestamp string into nanoseconds.
// It tries RFC3339Nano first (full precision), then falls back to RFC3339
// (reduced precision, logged as a warning). A total parse failure is a hard
// error: returning it would otherwise leave record.Time=0 and the point would
// be written at epoch, silently corrupting data and stalling the maxTS
// watermark. The error is wrapped so callers can propagate it through the
// chunked-query batchFunc path and abort the batch.
func parseInfluxTimestamp(ts string) (int64, error) {
	if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
		return t.UnixNano(), nil
	}
	if t, err := time.Parse(time.RFC3339, ts); err == nil {
		logger.Warn("timestamp parsed with reduced precision",
			zap.String("timestamp", ts))
		return t.UnixNano(), nil
	}
	// Both parses failed. Use the RFC3339 error (the more general format) as
	// the wrapped cause for a clear diagnostic.
	_, cause := time.Parse(time.RFC3339, ts)
	return 0, fmt.Errorf("failed to parse InfluxDB timestamp %q: %w", ts, cause)
}

func (a *InfluxDBV1Adapter) parseValues(columns []string, values []interface{}, seriesTags map[string]string, tagKeySet map[string]bool) (*types.Record, error) {
	record := types.NewRecord()

	// First, copy series-level tags (e.g., host, region, env from InfluxDB series tags)
	// These are the tags that define the series identity but aren't repeated in each row
	for k, v := range seriesTags {
		record.AddTag(k, v)
	}

	for i, col := range columns {
		if i >= len(values) {
			continue
		}

		val := values[i]
		if val == nil {
			continue
		}

		switch col {
		case "time":
			if ts, ok := val.(string); ok {
				nanos, err := parseInfluxTimestamp(ts)
				if err != nil {
					return nil, err
				}
				record.Time = nanos
			}
		default:
			switch v := val.(type) {
			case string:
				if isTagColumn(col, tagKeySet, seriesTags) {
					record.AddTag(col, v)
				} else {
					record.AddField(col, v)
				}
			default:
				if err := addInfluxField(record, col, v); err != nil {
					return nil, err
				}
			}
		}
	}

	return record, nil
}

func (a *InfluxDBV1Adapter) executeQuery(ctx context.Context, query string) ([]influxV1Series, error) {
	params := url.Values{}
	params.Set("q", query)
	params.Set("db", a.config.Database)

	if a.config.Username != "" {
		params.Set("u", a.config.Username)
		params.Set("p", a.config.Password)
	}

	u, err := url.Parse(a.baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}
	u.Path = "/query"
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.client.Do(req)
	if err != nil {
		// resp may be non-nil even when err is set (e.g., redirect error)
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return nil, fmt.Errorf("V1 query request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read V1 response body: %w", err)
	}

	var result influxV1Result
	if err := decodeInfluxV1Result(body, &result); err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	// Check for V1 API-level errors
	if err := checkInfluxV1ResultError(result); err != nil {
		return nil, err
	}

	var series []influxV1Series
	for _, r := range result.Results {
		series = append(series, r.Series...)
	}

	return series, nil
}

func decodeInfluxV1Config(config map[string]interface{}, cfg interface{}) error {
	// Database can be at top level or inside influxdb block - check both
	if v, ok := config["database"].(string); ok {
		cfg.(*InfluxDBV1Config).Database = v
	}

	cfgMap, ok := config["influxdb"].(map[string]interface{})
	if !ok {
		// If no influxdb block, at least database should have been set above
		if cfg.(*InfluxDBV1Config).Database == "" {
			return fmt.Errorf("influxdb config not found and database not specified at top level")
		}
		return nil
	}

	if v, ok := cfgMap["url"].(string); ok {
		cfg.(*InfluxDBV1Config).URL = v
	}
	// Database inside influxdb block takes precedence if present
	if v, ok := cfgMap["database"].(string); ok {
		cfg.(*InfluxDBV1Config).Database = v
	}
	if v, ok := cfgMap["username"].(string); ok {
		cfg.(*InfluxDBV1Config).Username = v
	}
	if v, ok := cfgMap["password"].(string); ok {
		cfg.(*InfluxDBV1Config).Password = v
	}
	if basicAuth, ok := cfgMap["basic_auth"].(map[string]interface{}); ok {
		if u, ok := basicAuth["username"].(string); ok {
			cfg.(*InfluxDBV1Config).Username = u
		}
		if p, ok := basicAuth["password"].(string); ok {
			cfg.(*InfluxDBV1Config).Password = p
		}
	}

	return nil
}

type InfluxDBV2Adapter struct {
	client *http.Client
	config *InfluxDBV2Config

	// tagKeyCache memoizes DiscoverTagKeys results per measurement so that
	// repeated QueryDataBatch calls do not re-issue SHOW TAG KEYS per batch.
	// SHOW TAG KEYS returns whole-measurement metadata, stable across batches.
	tagKeyCache map[string][]string
}

type InfluxDBV2Config struct {
	URL             string
	Token           string
	Org             string
	Bucket          string
	Username        string // V1 compatibility API credentials
	Password        string
	RetentionPolicy string // V1 compatibility RP
	SSL             types.SSLConfig
}

func init() {
	adapter.RegisterSourceAdapter("influxdb-v2", func() adapter.SourceAdapter {
		return &InfluxDBV2Adapter{}
	})
}

func (a *InfluxDBV2Adapter) Name() string {
	return "influxdb-v2"
}

func (a *InfluxDBV2Adapter) SupportedVersions() []string {
	return []string{"2.x"}
}

func (a *InfluxDBV2Adapter) Connect(ctx context.Context, config map[string]interface{}) error {
	cfg := &InfluxDBV2Config{}
	if err := decodeInfluxV2Config(config, cfg); err != nil {
		return err
	}
	a.config = cfg

	transport := &http.Transport{
		// ResponseHeaderTimeout bounds only the time waiting for response
		// headers. The body streams indefinitely so chunked query responses
		// are not cancelled mid-stream. Per-request context.Context governs
		// overall cancellation. Do NOT set http.Client.Timeout: Go's client
		// Timeout covers reading the body and would abort long migrations.
		ResponseHeaderTimeout: sourceHTTPResponseHeaderTimeout,
	}
	if cfg.SSL.Enabled && cfg.SSL.SkipVerify {
		// Require explicit opt-in via environment variable for insecure TLS
		if os.Getenv("ALLOW_INSECURE_TLS") != "1" {
			logger.Error("TLS certificate verification is disabled - set ALLOW_INSECURE_TLS=1 environment variable to allow",
				zap.String("url", cfg.URL))
			return fmt.Errorf("insecure TLS requires ALLOW_INSECURE_TLS=1 environment variable")
		}
		logger.Warn("TLS certificate verification is disabled - this is insecure and not recommended for production use",
			zap.String("url", cfg.URL))
		transport.TLSClientConfig.InsecureSkipVerify = true
	}
	a.client = &http.Client{
		Transport: transport,
		// Timeout intentionally left zero (indefinite) so chunked response
		// bodies can stream beyond 30s while a slow batchFunc writes to the
		// target. Per-request context.Context governs cancellation.
	}

	return nil
}

func (a *InfluxDBV2Adapter) Disconnect(ctx context.Context) error {
	if a.client != nil {
		a.client.CloseIdleConnections()
	}
	return nil
}

// buildV1URL returns the V1 compatibility API URL
func (a *InfluxDBV2Adapter) buildV1URL() string {
	baseURL := a.config.URL
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	return baseURL + "query"
}

// buildV1QueryParams builds query parameters for V1 compatibility API
func (a *InfluxDBV2Adapter) buildV1QueryParams(query string) url.Values {
	params := url.Values{}
	params.Set("q", query)
	params.Set("db", a.config.Bucket)
	if a.config.RetentionPolicy != "" {
		params.Set("rp", a.config.RetentionPolicy)
	}
	if a.config.Username != "" {
		params.Set("u", a.config.Username)
	}
	if a.config.Password != "" {
		params.Set("p", a.config.Password)
	}
	return params
}

// executeV1Query executes a query using the V1 compatibility API
func (a *InfluxDBV2Adapter) executeV1Query(ctx context.Context, query string) (*influxV1Result, error) {
	params := a.buildV1QueryParams(query)
	u, err := url.Parse(a.buildV1URL())
	if err != nil {
		return nil, fmt.Errorf("invalid V1 API URL: %w", err)
	}
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	logger.Debug("executing V1 compatibility query",
		zap.String("url", redactURL(u)),
		zap.String("query", query))

	resp, err := a.client.Do(req)
	if err != nil {
		// resp may be non-nil even when err is set (e.g., redirect error)
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return nil, fmt.Errorf("V1 query request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read V1 response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("V1 query failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result influxV1Result
	if err := decodeInfluxV1Result(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse V1 response: %w", err)
	}
	if err := checkInfluxV1ResultError(result); err != nil {
		return nil, err
	}

	return &result, nil
}

// executeV1SelectQuery executes a SELECT query and returns records
// tagKeySet is used to distinguish tags from fields (nil means treat all as fields)
func (a *InfluxDBV2Adapter) executeV1SelectQuery(ctx context.Context, query string, tagKeySet map[string]bool) ([]types.Record, error) {
	result, err := a.executeV1Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var records []types.Record
	for _, r := range result.Results {
		for _, series := range r.Series {
			for _, values := range series.Values {
				var record *types.Record
				if tagKeySet != nil {
					record, err = parseV1ValuesWithTagKeys(series.Columns, values, tagKeySet, series.Tags)
				} else {
					record, err = parseV1Values(series.Columns, values, series.Tags)
				}
				if err != nil {
					return nil, err
				}
				records = append(records, *record)
			}
		}
	}

	return records, nil
}

// executeV1ChunkedQuery executes chunked query using V1 compatibility API for V2 adapter
// This allows V2 adapter to use the native InfluxDB chunked response format
func (a *InfluxDBV2Adapter) executeV1ChunkedQuery(
	ctx context.Context,
	query string,
	chunkSize int,
	tagKeySet map[string]bool,
	batchFunc func([]types.Record) error,
) error {
	params := a.buildV1QueryParams(query)
	params.Set("chunked", "true")
	params.Set("chunk_size", strconv.Itoa(chunkSize))

	u, err := url.Parse(a.buildV1URL())
	if err != nil {
		return fmt.Errorf("invalid V1 API URL: %w", err)
	}
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	logger.Debug("executing chunked query for InfluxDB V2 (V1 API)",
		zap.String("url", redactURL(u)))

	resp, err := a.client.Do(req)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return fmt.Errorf("chunked query request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("query failed with status %d", resp.StatusCode)
		}
		return fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Use json.Decoder to stream through chunked response
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	totalRecords := 0

	for {
		// Read one chunk - InfluxDB returns series arrays
		var result influxV1Result
		if err := decoder.Decode(&result); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("decode chunk failed: %w", err)
		}

		// Check for V1 API errors
		if err := checkInfluxV1ResultError(result); err != nil {
			return err
		}

		// Parse chunk into records
		records := make([]types.Record, 0)
		for _, r := range result.Results {
			for _, series := range r.Series {
				for _, values := range series.Values {
					var record *types.Record
					var parseErr error
					if tagKeySet != nil {
						record, parseErr = parseV1ValuesWithTagKeys(series.Columns, values, tagKeySet, series.Tags)
					} else {
						record, parseErr = parseV1Values(series.Columns, values, series.Tags)
					}
					if parseErr != nil {
						return parseErr
					}
					records = append(records, *record)
				}
			}
		}

		// Process records
		if len(records) > 0 {
			if err := batchFunc(records); err != nil {
				return fmt.Errorf("batch func failed: %w", err)
			}
			totalRecords += len(records)
		}
	}

	logger.Debug("completed chunked query for InfluxDB V2 (V1 API)",
		zap.Int("total_records", totalRecords))

	return nil
}

// parseV1Values parses V1 query result values into a Record
// This treats all non-time string values as fields (for V1 adapter or when tagKeys is unavailable)
func parseV1Values(columns []string, values []interface{}, seriesTags map[string]string) (*types.Record, error) {
	record := types.NewRecord()

	// First, copy series-level tags
	for k, v := range seriesTags {
		record.AddTag(k, v)
	}

	for i, col := range columns {
		if i >= len(values) {
			continue
		}

		val := values[i]
		if val == nil {
			continue
		}

		switch col {
		case "time":
			if ts, ok := val.(string); ok {
				nanos, err := parseInfluxTimestamp(ts)
				if err != nil {
					return nil, err
				}
				record.Time = nanos
			}
		default:
			if err := addInfluxField(record, col, val); err != nil {
				return nil, err
			}
		}
	}

	return record, nil
}

// parseV1ValuesWithTagKeys parses V1 query result values into a Record
// It uses tagKeySet to distinguish tags from fields - strings in tagKeySet are tags
func parseV1ValuesWithTagKeys(columns []string, values []interface{}, tagKeySet map[string]bool, seriesTags map[string]string) (*types.Record, error) {
	record := types.NewRecord()

	// First, copy series-level tags
	for k, v := range seriesTags {
		record.AddTag(k, v)
	}

	for i, col := range columns {
		if i >= len(values) {
			continue
		}

		val := values[i]
		if val == nil {
			continue
		}

		switch col {
		case "time":
			if ts, ok := val.(string); ok {
				nanos, err := parseInfluxTimestamp(ts)
				if err != nil {
					return nil, err
				}
				record.Time = nanos
			}
		default:
			// If this column is a known tag key, store as tag.
			// Also treat as a tag when the column is a key in series.Tags:
			// DiscoverTagKeys (SHOW TAG KEYS) returns whole-measurement
			// metadata and may miss a key if a shard is offline or the index
			// is not materialized. The series object's Tags are the
			// authoritative tag set for that series, so cross-checking
			// prevents misclassifying a tag column as a string field and
			// corrupting series identity on the target.
			if isTagColumn(col, tagKeySet, seriesTags) {
				record.AddTag(col, fmt.Sprintf("%v", val))
			} else {
				if err := addInfluxField(record, col, val); err != nil {
					return nil, err
				}
			}
		}
	}

	return record, nil
}

func (a *InfluxDBV2Adapter) Ping(ctx context.Context) error {
	// Use V1 compatibility API for ping - "SHOW MEASUREMENTS" is a lightweight query
	result, err := a.executeV1Query(ctx, "SHOW MEASUREMENTS LIMIT 1")
	if err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("V1 ping failed: empty response")
	}
	return nil
}

func (a *InfluxDBV2Adapter) DiscoverTables(ctx context.Context) ([]string, error) {
	// Use V1 compatibility API: SHOW MEASUREMENTS
	result, err := a.executeV1Query(ctx, "SHOW MEASUREMENTS")
	if err != nil {
		return nil, err
	}

	var measurements []string
	for _, r := range result.Results {
		for _, series := range r.Series {
			for _, values := range series.Values {
				if len(values) > 0 {
					if name, ok := values[0].(string); ok {
						measurements = append(measurements, name)
					}
				}
			}
		}
	}

	return measurements, nil
}

// DiscoverTagKeys returns all tag key names for a measurement.
// Used to distinguish tags from fields in Flux query results.
// Results are cached per measurement for the lifetime of the adapter because
// SHOW TAG KEYS returns whole-measurement metadata that is stable across
// batches; re-issuing it per QueryDataBatch call wastes a round-trip and can
// return an incomplete set if a shard is transiently offline.
func (a *InfluxDBV2Adapter) DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error) {
	if cached, ok := a.lookupCachedTagKeys(measurement); ok {
		return cached, nil
	}
	// Use V1 compatibility API: SHOW TAG KEYS
	// This returns tag keys without values, which is sufficient for distinguishing tags from fields
	query := fmt.Sprintf("SHOW TAG KEYS FROM %s", influxQuoteIdentifier(measurement))
	result, err := a.executeV1Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("discover tag keys for measurement %q failed: %w", measurement, err)
	}

	var tagKeys []string
	for _, r := range result.Results {
		for _, seriesData := range r.Series {
			for _, values := range seriesData.Values {
				if len(values) > 0 {
					if key, ok := values[0].(string); ok {
						tagKeys = append(tagKeys, key)
					}
				}
			}
		}
	}

	a.storeCachedTagKeys(measurement, tagKeys)
	return tagKeys, nil
}

// resolveTagKeySet returns the tag-key set to use for a QueryData(Batch) call.
// It prefers caller-supplied cfg.TagKeys (authoritative, no network round-trip),
// then falls back to DiscoverTagKeys (cached on the adapter).
func (a *InfluxDBV2Adapter) resolveTagKeySet(ctx context.Context, measurement string, cfg *types.QueryConfig) (map[string]bool, []string, error) {
	if cfg != nil && len(cfg.TagKeys) > 0 {
		set := make(map[string]bool, len(cfg.TagKeys))
		for _, k := range cfg.TagKeys {
			set[k] = true
		}
		return set, cfg.TagKeys, nil
	}
	tagKeys, err := a.DiscoverTagKeys(ctx, measurement)
	if err != nil {
		return nil, nil, err
	}
	set := make(map[string]bool, len(tagKeys))
	for _, k := range tagKeys {
		set[k] = true
	}
	return set, tagKeys, nil
}

// lookupCachedTagKeys returns cached tag keys for a measurement, ok=false if absent.
func (a *InfluxDBV2Adapter) lookupCachedTagKeys(measurement string) ([]string, bool) {
	if a.tagKeyCache == nil {
		return nil, false
	}
	cached, ok := a.tagKeyCache[measurement]
	if !ok {
		return nil, false
	}
	out := make([]string, len(cached))
	copy(out, cached)
	return out, true
}

// storeCachedTagKeys memoizes tag keys for a measurement (immutable copy).
func (a *InfluxDBV2Adapter) storeCachedTagKeys(measurement string, tagKeys []string) {
	if a.tagKeyCache == nil {
		a.tagKeyCache = make(map[string][]string)
	}
	stored := make([]string, len(tagKeys))
	copy(stored, tagKeys)
	a.tagKeyCache[measurement] = stored
}

func (a *InfluxDBV2Adapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
	// Use V1 compatibility API: SHOW SERIES with pagination.
	// Collect all series keys into a dedup set. SHOW SERIES pagination uses
	// `series_key > lastKey`, which assumes lexicographic ordering. A
	// multi-shard response can return non-lexicographic or duplicate keys,
	// causing the predicate to skip legitimately unseen keys or re-emit
	// already-seen keys. Deduplicating into a set and returning sorted unique
	// keys makes the result internally consistent and prevents duplicate
	// series from being emitted to the target (A7).
	seriesSet := make(map[string]struct{})
	var lastKey string
	batchSize := seriesPaginationBatchSize

	for {
		var query string
		if lastKey == "" {
			query = fmt.Sprintf("SHOW SERIES FROM %s LIMIT %d",
				influxQuoteIdentifier(measurement), batchSize)
		} else {
			// InfluxDB 1.7+ supports WHERE series_key > for pagination
			query = fmt.Sprintf("SHOW SERIES FROM %s WHERE series_key > '%s' LIMIT %d",
				influxQuoteIdentifier(measurement), lastKey, batchSize)
		}

		result, err := a.executeV1Query(ctx, query)
		if err != nil {
			// If pagination query fails (older InfluxDB), fall back to a single
			// non-paginated query for ALL series. Dedup handles any overlap with
			// keys already collected, so no `key <= lastKey` skip is needed.
			if lastKey != "" {
				logger.Warn("series_key pagination not supported, falling back to non-paginated SHOW SERIES",
					zap.Error(err))
				fallbackQuery := fmt.Sprintf("SHOW SERIES FROM %s", influxQuoteIdentifier(measurement))
				fallbackResult, fallbackErr := a.executeV1Query(ctx, fallbackQuery)
				if fallbackErr != nil {
					return nil, fallbackErr
				}
				for _, r := range fallbackResult.Results {
					for _, seriesData := range r.Series {
						for _, values := range seriesData.Values {
							if len(values) > 0 {
								if key, ok := values[0].(string); ok {
									seriesSet[key] = struct{}{}
								}
							}
						}
					}
				}
				return dedupAndSortSeries(keysFromSet(seriesSet)), nil
			}
			return nil, err
		}

		batchCount := 0
		for _, r := range result.Results {
			for _, seriesData := range r.Series {
				for _, values := range seriesData.Values {
					if len(values) > 0 {
						if key, ok := values[0].(string); ok {
							seriesSet[key] = struct{}{}
							lastKey = key
							batchCount++
						}
					}
				}
			}
		}

		// If returned fewer than batch size, we're done
		if batchCount < batchSize {
			break
		}

		// Check context before continuing
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	return dedupAndSortSeries(keysFromSet(seriesSet)), nil
}

func (a *InfluxDBV2Adapter) DiscoverShardGroups(ctx context.Context) ([]*adapter.ShardGroup, error) {
	// Use the shards API endpoint
	req, err := http.NewRequestWithContext(ctx, "GET", a.config.URL+"/api/v2/shards", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Token "+a.config.Token)

	resp, err := a.client.Do(req)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("shard query failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Shards []struct {
			ID        int   `json:"id"`
			StartTime int64 `json:"startTime"`
			EndTime   int64 `json:"endTime"`
		} `json:"shards"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	var shardGroups []*adapter.ShardGroup
	for _, s := range result.Shards {
		shardGroups = append(shardGroups, &adapter.ShardGroup{
			ID:        s.ID,
			StartTime: time.Unix(0, s.StartTime),
			EndTime:   time.Unix(0, s.EndTime),
		})
	}
	return shardGroups, nil
}

func (a *InfluxDBV2Adapter) DiscoverSeriesInTimeWindow(ctx context.Context, measurement string, startTime, endTime time.Time) ([]string, error) {
	// Note: InfluxDB SHOW SERIES does not support time-based WHERE filtering.
	// The time parameters are accepted for interface compatibility but ignored.
	// All series for the measurement are returned.
	logger.Debug("DiscoverSeriesInTimeWindow using all-series fallback; point queries remain time-bounded",
		zap.String("measurement", measurement),
		zap.Time("window_start", startTime),
		zap.Time("window_end", endTime))
	query := fmt.Sprintf("SHOW SERIES FROM %s", influxQuoteIdentifier(measurement))

	result, err := a.executeV1Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var series []string
	for _, r := range result.Results {
		for _, seriesData := range r.Series {
			for _, values := range seriesData.Values {
				if len(values) > 0 {
					if key, ok := values[0].(string); ok {
						series = append(series, key)
					}
				}
			}
		}
	}

	return series, nil
}

func (a *InfluxDBV2Adapter) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
	// InfluxDB 2.x uses Flux and has schemaless writes.
	// Return a minimal schema with just the measurement name.
	// Actual field/tag discovery is done through queries.
	return &types.TableSchema{
		TableName: table,
		Columns:   []types.Column{},
	}, nil
}

func (a *InfluxDBV2Adapter) QueryData(ctx context.Context, measurement string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	var lastTS int64
	var totalProcessed int64

	if lastCheckpoint != nil && lastCheckpoint.LastTimestamp != 0 {
		lastTS = lastCheckpoint.LastTimestamp
	}

	startTime, endTime, err := effectiveQueryBounds(lastTS, cfg)
	if err != nil {
		return nil, err
	}

	chunkSize := 10000
	if cfg != nil && cfg.BatchSize > 0 {
		chunkSize = cfg.BatchSize
	}

	// Resolve tag keys: prefer caller-supplied cfg.TagKeys, else DiscoverTagKeys
	// (cached per measurement on the adapter).
	tagKeySet, _, tagErr := a.resolveTagKeySet(ctx, measurement, cfg)
	if tagErr != nil {
		return nil, tagErr
	}

	// Build query without LIMIT - InfluxDB will return data in chunks
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE time >= '%s' AND time < '%s'`,
		influxQuoteIdentifier(measurement), startTime, endTime)

	logger.Debug("executing chunked query for InfluxDB V2 (V1 API)",
		zap.String("measurement", measurement),
		zap.Int("chunk_size", chunkSize),
		zap.String("query_start", startTime),
		zap.String("query_end", endTime))

	var totalRecords int
	var maxTS int64

	// Use chunked query - InfluxDB automatically splits response into chunks
	err = a.executeV1ChunkedQuery(ctx, query, chunkSize, tagKeySet, func(records []types.Record) error {
		totalRecords += len(records)
		totalProcessed += int64(len(records))
		for _, record := range records {
			if record.Time > maxTS {
				maxTS = record.Time
			}
		}
		return batchFunc(records)
	})

	if err != nil {
		return nil, fmt.Errorf("chunked query failed: %w", err)
	}

	logger.Info("completed chunked query for InfluxDB V2 (V1 API)",
		zap.String("measurement", measurement),
		zap.Int("total_records", totalRecords))

	return &types.Checkpoint{
		LastTimestamp: maxTS,
		ProcessedRows: totalProcessed,
	}, nil
}

func (a *InfluxDBV2Adapter) QueryDataBatch(ctx context.Context, measurement string,
	series []string, startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
	batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

	// Always use the original startTime in batch mode.
	// The lastCheckpoint.LastTimestamp is for progress tracking only, not for
	// modifying query parameters. Each batch queries its full assigned time range.
	queryStart := startTime

	chunkSize := getBatchSize(cfg)

	// Resolve tag keys: prefer caller-supplied cfg.TagKeys, else DiscoverTagKeys
	// (cached per measurement on the adapter).
	tagKeySet, tagKeys, tagErr := a.resolveTagKeySet(ctx, measurement, cfg)
	if tagErr != nil {
		return nil, tagErr
	}
	whereClause := BuildWhereClauseWithTagKeys(series, tagKeys)

	// Build query without LIMIT - InfluxDB will return data in chunks
	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE (%s) AND time >= '%s' AND time < '%s'`,
		influxQuoteIdentifier(measurement),
		whereClause,
		queryStart.Format(time.RFC3339Nano),
		endTime.Format(time.RFC3339Nano))

	logger.Debug("executing chunked query for InfluxDB V2 (V1 API)",
		zap.String("measurement", measurement),
		zap.Int("series_count", len(series)),
		zap.Int("chunk_size", chunkSize),
		zap.String("query_start", queryStart.Format(time.RFC3339Nano)),
		zap.String("query_end", endTime.Format(time.RFC3339Nano)))

	var totalRecords int
	var maxTS int64

	// Use chunked query - InfluxDB automatically splits response into chunks
	err := a.executeV1ChunkedQuery(ctx, query, chunkSize, tagKeySet, func(records []types.Record) error {
		totalRecords += len(records)
		for _, record := range records {
			if record.Time > maxTS {
				maxTS = record.Time
			}
		}
		return batchFunc(records)
	})

	if err != nil {
		return nil, fmt.Errorf("chunked query failed: %w", err)
	}

	logger.Info("completed chunked query for InfluxDB V2 (V1 API)",
		zap.String("measurement", measurement),
		zap.Int("series_count", len(series)),
		zap.Int("total_records", totalRecords),
		zap.Int64("max_timestamp", maxTS))

	return &types.Checkpoint{
		LastTimestamp: maxTS,
		ProcessedRows: int64(totalRecords),
	}, nil
}

func decodeInfluxV2Config(config map[string]interface{}, cfg interface{}) error {
	cfgMap, ok := config["influxdb"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("influxdb config not found")
	}

	if v, ok := cfgMap["url"].(string); ok {
		cfg.(*InfluxDBV2Config).URL = v
	}
	if v, ok := cfgMap["token"].(string); ok {
		cfg.(*InfluxDBV2Config).Token = v
	}
	if v, ok := cfgMap["org"].(string); ok {
		cfg.(*InfluxDBV2Config).Org = v
	}
	if v, ok := cfgMap["bucket"].(string); ok {
		cfg.(*InfluxDBV2Config).Bucket = v
	}
	// V1 compatibility API credentials
	if v, ok := cfgMap["username"].(string); ok {
		cfg.(*InfluxDBV2Config).Username = v
	}
	if v, ok := cfgMap["password"].(string); ok {
		cfg.(*InfluxDBV2Config).Password = v
	}
	if v, ok := cfgMap["retention_policy"].(string); ok {
		cfg.(*InfluxDBV2Config).RetentionPolicy = v
	}
	// SSL configuration
	if sslMap, ok := cfgMap["ssl"].(map[string]interface{}); ok {
		cfg.(*InfluxDBV2Config).SSL.Enabled, _ = sslMap["enabled"].(bool)
		cfg.(*InfluxDBV2Config).SSL.SkipVerify, _ = sslMap["skip_verify"].(bool)
		if v, ok := sslMap["ca_cert"].(string); ok {
			cfg.(*InfluxDBV2Config).SSL.CaCert = v
		}
		if v, ok := sslMap["client_cert"].(string); ok {
			cfg.(*InfluxDBV2Config).SSL.ClientCert = v
		}
		if v, ok := sslMap["client_key"].(string); ok {
			cfg.(*InfluxDBV2Config).SSL.ClientKey = v
		}
	}

	return nil
}

func influxQuoteIdentifier(s string) string {
	if s == "" {
		return `""`
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// redactURL returns URL string with credentials redacted for safe logging
func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	redacted := *u
	redacted.RawQuery = "REDACTED"
	return redacted.String()
}

// ParseSeriesKey parses "measurement,tag1=value1,tag2=value2" into components.
// It follows Influx line protocol escaping for commas, equals signs, and backslashes.
func ParseSeriesKey(key string) (tags map[string]string) {
	return types.ParseSeriesKey(key).Tags
}

func influxQuoteStringLiteral(s string) string {
	escaped := strings.ReplaceAll(s, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}

// BuildWhereClause builds "(tag1='v1' AND tag2='v2') OR (tag1='v3' AND tag2='v4')"
func BuildWhereClause(series []string) string {
	return buildWhereClause(series, nil)
}

// BuildWhereClauseWithTagKeys builds a series filter using the measurement's full tag key set.
func BuildWhereClauseWithTagKeys(series []string, measurementTagKeys []string) string {
	return buildWhereClause(series, measurementTagKeys)
}

func buildWhereClause(series []string, measurementTagKeys []string) string {
	seriesTags := make([]map[string]string, 0, len(series))
	tagKeySet := make(map[string]struct{})

	for _, k := range measurementTagKeys {
		tagKeySet[k] = struct{}{}
	}
	for _, s := range series {
		tags := ParseSeriesKey(s)
		seriesTags = append(seriesTags, tags)
		for k := range tags {
			tagKeySet[k] = struct{}{}
		}
	}

	tagKeys := make([]string, 0, len(tagKeySet))
	for k := range tagKeySet {
		tagKeys = append(tagKeys, k)
	}
	sort.Strings(tagKeys)

	var conditions []string
	for _, tags := range seriesTags {
		var tagConditions []string
		for _, k := range tagKeys {
			if v, ok := tags[k]; ok {
				tagConditions = append(tagConditions, fmt.Sprintf("%s=%s", influxQuoteIdentifier(k), influxQuoteStringLiteral(v)))
			} else {
				tagConditions = append(tagConditions, fmt.Sprintf("%s !~ /.*/", influxQuoteIdentifier(k)))
			}
		}
		if len(tagConditions) > 0 {
			conditions = append(conditions, "("+strings.Join(tagConditions, " AND ")+")")
		}
	}
	if len(conditions) == 0 {
		// No tag conditions means match all series (no filtering)
		// Return "1=1" to produce valid SQL: WHERE 1=1 AND time...
		return "1=1"
	}
	return strings.Join(conditions, " OR ")
}

// BuildFluxFilter builds Flux filter expression: (r.tag1 == "v1" and r.tag2 == "v2") or ...
func BuildFluxFilter(series []string) string {
	var conditions []string
	for _, s := range series {
		tags := ParseSeriesKey(s)
		var tagConditions []string
		for k, v := range tags {
			// Escape backslashes and double quotes in tag values for Flux security
			escaped := strings.ReplaceAll(v, "\\", "\\\\")
			escaped = strings.ReplaceAll(escaped, `"`, `\"`)
			tagConditions = append(tagConditions, fmt.Sprintf(`r.%s == "%s"`, influxQuoteIdentifier(k), escaped))
		}
		if len(tagConditions) > 0 {
			conditions = append(conditions, "("+strings.Join(tagConditions, " and ")+")")
		}
	}
	return strings.Join(conditions, " or ")
}

// getBatchSize returns batch size from config, defaulting to DefaultBatchSize
func getBatchSize(cfg *types.QueryConfig) int {
	batchSize := types.DefaultBatchSize
	if cfg != nil && cfg.BatchSize > 0 {
		batchSize = cfg.BatchSize
	}
	return batchSize
}
