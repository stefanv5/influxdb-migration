package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
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
}

type InfluxDBV1Config struct {
	URL      string
	Username string
	Password string
	Database string
	SSL      types.SSLConfig
}

type influxV1Result struct {
	Results []influxV1Series `json:"results"`
	Error   string           `json:"error"` // V1 API error message
}

type influxV1Series struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags"`
	Columns []string          `json:"columns"`
	Values  [][]interface{}   `json:"values"`
}

func init() {
	adapter.RegisterSourceAdapter("influxdb-v1", func() adapter.SourceAdapter {
		return &InfluxDBV1Adapter{}
	})
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

	transport := &http.Transport{}
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
		Timeout:   30 * time.Second,
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
	query := fmt.Sprintf("SHOW SERIES FROM %s", influxQuoteIdentifier(measurement))
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

func (a *InfluxDBV1Adapter) DiscoverShardGroups(ctx context.Context) ([]*adapter.ShardGroup, error) {
	query := "SHOW SHARDS"
	results, err := a.executeQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	// Group by shard_group ID, extract time range
	shardGroups := make(map[int]*adapter.ShardGroup)
	for _, result := range results {
		for _, values := range result.Values {
			if len(values) < 6 {
				continue
			}
			// Parse: id, database, retention_policy, shard_group, start_time, end_time
			shardGroupID := parseInt(values[3])
			startTime := parseTime(values[4])
			endTime := parseTime(values[5])

			if _, exists := shardGroups[shardGroupID]; !exists {
				shardGroups[shardGroupID] = &adapter.ShardGroup{
					ID:        shardGroupID,
					StartTime: startTime,
					EndTime:   endTime,
				}
			}
		}
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
	query := fmt.Sprintf(`
		SHOW SERIES FROM %s
		WHERE time >= '%s' AND time < '%s'`,
		influxQuoteIdentifier(measurement),
		startTime.Format(time.RFC3339Nano),
		endTime.Format(time.RFC3339Nano))

	series, err := a.executeShowSeries(ctx, query)
	if err != nil {
		return nil, err
	}

	return series, nil
}

// DiscoverTagKeys returns nil for V1 adapter.
// V1 uses column headers from SELECT results to distinguish tags from fields,
// so no prior tag key discovery is needed.
func (a *InfluxDBV1Adapter) DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error) {
	return nil, nil
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

// parseTime parses an interface{} to time.Time
func parseTime(v interface{}) time.Time {
	switch val := v.(type) {
	case string:
		// Try RFC3339Nano first
		if t, err := time.Parse(time.RFC3339Nano, val); err == nil {
			return t
		}
		// Try RFC3339
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t
		}
	case float64:
		// Unix timestamp in seconds or nanoseconds
		if val > 1e12 {
			// Likely nanoseconds
			return time.Unix(0, int64(val))
		}
		// Likely seconds
		return time.Unix(int64(val), 0)
	case int64:
		return time.Unix(val, 0)
	}
	return time.Time{}
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

	var startTime string
	if lastTS == 0 {
		startTime = "1970-01-01T00:00:00Z"
	} else {
		startTime = time.Unix(0, lastTS).Format(time.RFC3339Nano)
	}

	endTime := time.Now().Add(1 * time.Hour).Format(time.RFC3339)

	batchSize := 10000
	if cfg != nil && cfg.BatchSize > 0 {
		batchSize = cfg.BatchSize
	}
	totalRecords := int(totalProcessed)

	for {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE time >= '%s' AND time < '%s' ORDER BY time LIMIT %d`,
			influxQuoteIdentifier(measurement), startTime, endTime, batchSize)

		records, err := a.executeSelectQuery(ctx, query)
		if err != nil {
			return nil, err
		}

		if len(records) == 0 {
			break
		}

		if err := batchFunc(records); err != nil {
			return nil, err
		}

		totalRecords += len(records)
		totalProcessed += int64(len(records))

		maxTS := records[len(records)-1].Time
		lastTS = maxTS
		if maxTS < math.MaxInt64 {
			startTime = fmt.Sprintf("%d", maxTS+1)
		} else {
			startTime = fmt.Sprintf("%d", maxTS)
		}

		logger.Debug("fetched batch from InfluxDB V1",
			zap.String("measurement", measurement),
			zap.Int("batch_size", len(records)),
			zap.String("next_start", startTime))

		if len(records) < batchSize {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	logger.Info("completed InfluxDB V1 query",
		zap.String("measurement", measurement),
		zap.Int("total_records", totalRecords))

	return &types.Checkpoint{
		LastTimestamp: lastTS,
		ProcessedRows: totalProcessed,
	}, nil
}

func (a *InfluxDBV1Adapter) QueryDataBatch(ctx context.Context, measurement string,
	series []string, startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
	batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

	var lastTS int64
	if lastCheckpoint != nil {
		lastTS = lastCheckpoint.LastTimestamp
	}

	// Determine effective start time
	queryStart := startTime
	if lastTS > 0 && lastTS > startTime.UnixNano() {
		queryStart = time.Unix(0, lastTS)
	}

	batchSize := getBatchSize(cfg)
	whereClause := BuildWhereClause(series)

	var totalRecords int
	var maxTS int64

	for {
		// Query with time bounds: startTime (or resume point) AND endTime
		query := fmt.Sprintf(`
			SELECT * FROM %s
			WHERE (%s) AND time >= '%s' AND time < '%s'
			LIMIT %d ORDER BY time`,
			influxQuoteIdentifier(measurement),
			whereClause,
			queryStart.Format(time.RFC3339Nano),
			endTime.Format(time.RFC3339Nano),
			batchSize)

		logger.Debug("executing batch query for InfluxDB V1",
			zap.String("measurement", measurement),
			zap.Int("series_count", len(series)),
			zap.String("query", query))

		records, err := a.executeSelectQuery(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("batch query failed: %w", err)
		}

		if len(records) == 0 {
			break
		}

		if err := batchFunc(records); err != nil {
			return nil, fmt.Errorf("batch func failed: %w", err)
		}

		totalRecords += len(records)

		// Update max timestamp from this batch
		for _, record := range records {
			if record.Time > maxTS {
				maxTS = record.Time
			}
		}

		logger.Debug("fetched batch from InfluxDB V1",
			zap.String("measurement", measurement),
			zap.Int("batch_size", len(records)),
			zap.String("next_start", queryStart.Format(time.RFC3339Nano)))

		// If we got fewer records than batch size, we're done
		if len(records) < batchSize {
			break
		}

		// Update queryStart for next query using the last record's timestamp
		if maxTS < math.MaxInt64 {
			queryStart = time.Unix(0, maxTS+1)
		} else {
			queryStart = time.Unix(0, maxTS)
		}

		// Don't query beyond window end
		if queryStart.UnixNano() >= endTime.UnixNano() {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	logger.Info("completed batch query for InfluxDB V1",
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
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	// Check for V1 API-level errors
	if result.Error != "" {
		return nil, fmt.Errorf("V1 query error: %s", result.Error)
	}

	var records []types.Record
	for _, series := range result.Results {
		for _, values := range series.Values {
			record := a.parseValues(series.Columns, values)
			records = append(records, *record)
		}
	}

	return records, nil
}

func (a *InfluxDBV1Adapter) parseValues(columns []string, values []interface{}) *types.Record {
	record := types.NewRecord()

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
				if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
					record.Time = t.UnixNano()
				} else if t, err := time.Parse(time.RFC3339, ts); err == nil {
					record.Time = t.UnixNano()
					logger.Warn("timestamp parsed with reduced precision",
						zap.String("timestamp", ts))
				} else {
					logger.Warn("failed to parse InfluxDB timestamp",
						zap.String("timestamp_string", ts),
						zap.Error(err))
				}
			}
		default:
			switch v := val.(type) {
			case float64:
				record.AddField(col, v)
			case string:
				// In InfluxDB V1, string values from query results are typically tags
				// (InfluxDB stores strings as tags by default)
				record.AddTag(col, v)
			case bool:
				record.AddField(col, v)
			}
		}
	}

	return record
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
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	// Check for V1 API-level errors
	if result.Error != "" {
		return nil, fmt.Errorf("V1 query error: %s", result.Error)
	}

	var series []influxV1Series
	for _, r := range result.Results {
		series = append(series, r)
	}

	return series, nil
}

func decodeInfluxV1Config(config map[string]interface{}, cfg interface{}) error {
	cfgMap, ok := config["influxdb"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("influxdb config not found")
	}

	if v, ok := cfgMap["url"].(string); ok {
		cfg.(*InfluxDBV1Config).URL = v
	}
	if v, ok := cfgMap["database"].(string); ok {
		cfg.(*InfluxDBV1Config).Database = v
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
}

type InfluxDBV2Config struct {
	URL             string
	Token          string
	Org            string
	Bucket         string
	Username       string // V1 compatibility API credentials
	Password       string
	RetentionPolicy string // V1 compatibility RP
	SSL            types.SSLConfig
}

type fluxRecord struct {
	Time        time.Time   `json:"_time"`
	Measurement string      `json:"_measurement"`
	Field       string      `json:"_field"`
	Value       interface{} `json:"_value"`
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

	transport := &http.Transport{}
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
		Timeout:   30 * time.Second,
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
	params.Set("u", a.config.Username)
	params.Set("p", a.config.Password)
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
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to parse V1 response: %w", err)
	}

	return &result, nil
}

// executeV1SelectQuery executes a SELECT query and returns records
func (a *InfluxDBV2Adapter) executeV1SelectQuery(ctx context.Context, query string) ([]types.Record, error) {
	result, err := a.executeV1Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var records []types.Record
	for _, series := range result.Results {
		for _, values := range series.Values {
			record := parseV1Values(series.Columns, values)
			records = append(records, *record)
		}
	}

	return records, nil
}

// parseV1Values parses V1 query result values into a Record
func parseV1Values(columns []string, values []interface{}) *types.Record {
	record := types.NewRecord()

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
				if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
					record.Time = t.UnixNano()
				} else if t, err := time.Parse(time.RFC3339, ts); err == nil {
					record.Time = t.UnixNano()
					logger.Warn("timestamp parsed with reduced precision",
						zap.String("timestamp", ts))
				} else {
					logger.Warn("failed to parse timestamp, using 0",
						zap.String("timestamp_string", ts),
						zap.Error(err))
				}
			}
		default:
			switch v := val.(type) {
			case float64:
				record.AddField(col, v)
			case string:
				record.AddField(col, v)
			case bool:
				record.AddField(col, v)
			case int64:
				record.AddField(col, v)
			case int:
				record.AddField(col, int64(v))
			}
		}
	}

	return record
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
	for _, series := range result.Results {
		for _, values := range series.Values {
			if len(values) > 0 {
				if name, ok := values[0].(string); ok {
					measurements = append(measurements, name)
				}
			}
		}
	}

	return measurements, nil
}

// DiscoverTagKeys returns all tag key names for a measurement.
// Used to distinguish tags from fields in Flux query results.
func (a *InfluxDBV2Adapter) DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error) {
	// Use V1 compatibility API: SHOW TAG KEYS
	// This returns tag keys without values, which is sufficient for distinguishing tags from fields
	query := fmt.Sprintf("SHOW TAG KEYS FROM %s", influxQuoteIdentifier(measurement))
	result, err := a.executeV1Query(ctx, query)
	if err != nil {
		// V1 API may not support SHOW TAG KEYS in all cases, return empty
		logger.Warn("SHOW TAG KEYS failed, returning empty tag keys", zap.Error(err))
		return nil, nil
	}

	var tagKeys []string
	for _, seriesData := range result.Results {
		for _, values := range seriesData.Values {
			if len(values) > 0 {
				if key, ok := values[0].(string); ok {
					tagKeys = append(tagKeys, key)
				}
			}
		}
	}

	return tagKeys, nil
}

func (a *InfluxDBV2Adapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
	// Use V1 compatibility API: SHOW SERIES
	query := fmt.Sprintf("SHOW SERIES FROM %s", influxQuoteIdentifier(measurement))
	result, err := a.executeV1Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var series []string
	for _, seriesData := range result.Results {
		for _, values := range seriesData.Values {
			if len(values) > 0 {
				if key, ok := values[0].(string); ok {
					series = append(series, key)
				}
			}
		}
	}

	return series, nil
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
		return nil, err
	}
	defer resp.Body.Close()

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
	// Use V1 compatibility API: SHOW SERIES with time filtering
	query := fmt.Sprintf(
		"SHOW SERIES FROM %s WHERE time >= '%s' AND time < '%s'",
		influxQuoteIdentifier(measurement),
		startTime.Format(time.RFC3339Nano),
		endTime.Format(time.RFC3339Nano))

	result, err := a.executeV1Query(ctx, query)
	if err != nil {
		return nil, err
	}

	var series []string
	for _, seriesData := range result.Results {
		for _, values := range seriesData.Values {
			if len(values) > 0 {
				if key, ok := values[0].(string); ok {
					series = append(series, key)
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
	var startTime string
	var totalProcessed int64
	var lastTS int64

	if lastCheckpoint != nil && lastCheckpoint.LastTimestamp != 0 {
		startTime = time.Unix(0, lastCheckpoint.LastTimestamp).Format(time.RFC3339Nano)
		lastTS = lastCheckpoint.LastTimestamp
	} else {
		startTime = "1970-01-01T00:00:00Z"
	}

	endTime := time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano)

	batchSize := 10000
	if cfg != nil && cfg.BatchSize > 0 {
		batchSize = cfg.BatchSize
	}
	totalRecords := 0

	for {
		// Use V1 compatibility API with LIMIT/OFFSET pagination
		query := fmt.Sprintf(
			`SELECT * FROM %s WHERE time >= '%s' AND time < '%s' ORDER BY time LIMIT %d`,
			influxQuoteIdentifier(measurement), startTime, endTime, batchSize)

		records, err := a.executeV1SelectQuery(ctx, query)
		if err != nil {
			return nil, err
		}

		if len(records) == 0 {
			break
		}

		if err := batchFunc(records); err != nil {
			return nil, err
		}

		totalRecords += len(records)
		totalProcessed += int64(len(records))

		maxTS := records[len(records)-1].Time
		lastTS = maxTS
		if maxTS < math.MaxInt64 {
			startTime = time.Unix(0, maxTS+1).Format(time.RFC3339Nano)
		} else {
			startTime = time.Unix(0, maxTS).Format(time.RFC3339Nano)
		}

		logger.Debug("fetched batch from InfluxDB V2 (V1 API)",
			zap.String("measurement", measurement),
			zap.Int("batch_size", len(records)),
			zap.String("next_start", startTime))

		if len(records) < batchSize {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	logger.Info("completed InfluxDB V2 query (V1 API)",
		zap.String("measurement", measurement),
		zap.Int("total_records", totalRecords))

	return &types.Checkpoint{
		LastTimestamp: lastTS,
		ProcessedRows: totalProcessed,
	}, nil
}

func (a *InfluxDBV2Adapter) QueryDataBatch(ctx context.Context, measurement string,
	series []string, startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
	batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

	var lastTS int64
	if lastCheckpoint != nil && lastCheckpoint.LastTimestamp != 0 {
		lastTS = lastCheckpoint.LastTimestamp
	}

	// Determine effective start time
	queryStart := startTime
	if lastTS > 0 && lastTS > startTime.UnixNano() {
		queryStart = time.Unix(0, lastTS)
	}

	batchSize := getBatchSize(cfg)
	whereClause := BuildWhereClause(series)

	var totalRecords int
	var maxTS int64

	for {
		// Use V1 compatibility API with WHERE clause for series filtering
		query := fmt.Sprintf(
			`SELECT * FROM %s WHERE (%s) AND time >= '%s' AND time < '%s' ORDER BY time LIMIT %d`,
			influxQuoteIdentifier(measurement),
			whereClause,
			queryStart.Format(time.RFC3339Nano),
			endTime.Format(time.RFC3339Nano),
			batchSize)

		logger.Debug("executing batch query for InfluxDB V2 (V1 API)",
			zap.String("measurement", measurement),
			zap.Int("series_count", len(series)),
			zap.String("query", query))

		records, err := a.executeV1SelectQuery(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("batch query failed: %w", err)
		}

		if len(records) == 0 {
			break
		}

		if err := batchFunc(records); err != nil {
			return nil, fmt.Errorf("batch func failed: %w", err)
		}

		totalRecords += len(records)

		// Update max timestamp from this batch
		for _, record := range records {
			if record.Time > maxTS {
				maxTS = record.Time
			}
		}

		logger.Debug("fetched batch from InfluxDB V2 (V1 API)",
			zap.String("measurement", measurement),
			zap.Int("batch_size", len(records)),
			zap.String("next_start", queryStart.Format(time.RFC3339Nano)))

		// If we got fewer records than batch size, we're done
		if len(records) < batchSize {
			break
		}

		// Update queryStart for next query using the last record's timestamp
		if maxTS < math.MaxInt64 {
			queryStart = time.Unix(0, maxTS+1)
		} else {
			queryStart = time.Unix(0, maxTS)
		}

		// Don't query beyond window end
		if queryStart.UnixNano() >= endTime.UnixNano() {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	logger.Info("completed batch query for InfluxDB V2 (V1 API)",
		zap.String("measurement", measurement),
		zap.Int("series_count", len(series)),
		zap.Int("total_records", totalRecords),
		zap.Int64("max_timestamp", maxTS))

	return &types.Checkpoint{
		LastTimestamp: maxTS,
		ProcessedRows: int64(totalRecords),
	}, nil
}

func (a *InfluxDBV2Adapter) executeFluxSelect(ctx context.Context, query string, tagKeys []string) ([]types.Record, error) {
	params, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	u, err := url.Parse(a.config.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}
	u.Path = "/api/v2/query"

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(params))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Token "+a.config.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Check HTTP status code before decoding
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("flux query failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Decode into generic map to capture all fields including tags
	var rawResults []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&rawResults); err != nil {
		return nil, fmt.Errorf("failed to decode flux result: %w", err)
	}

	// Build tagKeys set for O(1) lookup
	tagKeySet := make(map[string]bool)
	for _, k := range tagKeys {
		tagKeySet[k] = true
	}

	var records []types.Record
	for _, r := range rawResults {
		record := types.NewRecord()

		// Extract _time
		if t, ok := r["_time"].(string); ok {
			if tm, err := time.Parse(time.RFC3339Nano, t); err == nil {
				record.Time = tm.UnixNano()
			}
		}

		// Extract _measurement as tag
		if m, ok := r["_measurement"].(string); ok {
			record.AddTag("_measurement", m)
		}

		// Extract _field and _value as field
		if f, ok := r["_field"].(string); ok {
			if v, ok := r["_value"]; ok {
				record.AddField(f, v)
			}
		}

		// Distinguish tags from fields
		// System fields: _time, _measurement, _field, _value
		// Tag fields: any field whose name is in tagKeys
		// Other fields: regular fields (float, int, bool)
		for k, v := range r {
			switch k {
			case "_time", "_measurement", "_field", "_value":
				continue
			}
			if tagKeySet[k] {
				// This is a tag - convert value to string
				record.AddTag(k, fmt.Sprintf("%v", v))
			} else if k != "_field" && k != "_value" {
				// This is a regular field
				record.AddField(k, v)
			}
		}

		records = append(records, *record)
	}

	return records, nil
}

func (a *InfluxDBV2Adapter) executeFluxQuery(ctx context.Context, query string) ([][]interface{}, error) {
	params, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	u, err := url.Parse(a.config.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}
	u.Path = "/api/v2/query"

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(params))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Token "+a.config.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result [][]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode flux result: %w", err)
	}

	return result, nil
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

// ParseSeriesKey parses "measurement,tag1=value1,tag2=value2" into components
func ParseSeriesKey(key string) (tags map[string]string) {
	tags = make(map[string]string)
	parts := strings.Split(key, ",")
	// First part is measurement, skip it
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			tags[kv[0]] = kv[1]
		}
	}
	return
}

// BuildWhereClause builds "(tag1='v1' AND tag2='v2') OR (tag1='v3' AND tag2='v4')"
func BuildWhereClause(series []string) string {
	var conditions []string
	for _, s := range series {
		tags := ParseSeriesKey(s)
		var tagConditions []string
		for k, v := range tags {
			// Escape single quotes in tag values for InfluxQL security
			escapedValue := strings.ReplaceAll(v, "'", "''")
			tagConditions = append(tagConditions, fmt.Sprintf("%s='%s'", influxQuoteIdentifier(k), escapedValue))
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
