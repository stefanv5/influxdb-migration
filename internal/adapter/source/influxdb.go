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
	series []string, lastCheckpoint *types.Checkpoint,
	batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

	var lastTS int64
	if lastCheckpoint != nil {
		lastTS = lastCheckpoint.LastTimestamp
	}

	var startTime string
	if lastTS == 0 {
		startTime = "1970-01-01T00:00:00Z"
	} else {
		startTime = time.Unix(0, lastTS).Format(time.RFC3339Nano)
	}

	batchSize := getBatchSize(cfg)
	whereClause := BuildWhereClause(series)

	var totalRecords int
	var maxTS int64

	for {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE (%s) AND time >= '%s' LIMIT %d ORDER BY time`,
			influxQuoteIdentifier(measurement), whereClause, startTime, batchSize)

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
			zap.String("next_start", startTime))

		// If we got fewer records than batch size, we're done
		if len(records) < batchSize {
			break
		}

		// Update startTime for next query using the last record's timestamp
		if maxTS < math.MaxInt64 {
			startTime = fmt.Sprintf("%d", maxTS+1)
		} else {
			startTime = fmt.Sprintf("%d", maxTS)
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
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result influxV1Result
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
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
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result influxV1Result
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
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
	URL    string
	Token  string
	Org    string
	Bucket string
	SSL    types.SSLConfig
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

func (a *InfluxDBV2Adapter) Ping(ctx context.Context) error {
	u, err := url.Parse(a.config.URL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	u.Path = "/api/v2"

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+a.config.Token)

	resp, err := a.client.Do(req)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping failed with status: %d", resp.StatusCode)
	}

	return nil
}

func (a *InfluxDBV2Adapter) DiscoverTables(ctx context.Context) ([]string, error) {
	fluxQuery := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.measurements(bucket: "%s")`, a.config.Bucket)

	results, err := a.executeFluxQuery(ctx, fluxQuery)
	if err != nil {
		return nil, err
	}

	var measurements []string
	for _, r := range results {
		for _, v := range r {
			if arr, ok := v.([]interface{}); ok && len(arr) > 0 {
				if name, ok := arr[0].(string); ok {
					measurements = append(measurements, name)
				}
			}
		}
	}

	return measurements, nil
}

func (a *InfluxDBV2Adapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
	fluxQuery := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.tagKeys(bucket: "%s", measurement: "%s")`, a.config.Bucket, measurement)

	results, err := a.executeFluxQuery(ctx, fluxQuery)
	if err != nil {
		return nil, err
	}

	var series []string
	for _, r := range results {
		for _, v := range r {
			if arr, ok := v.([]interface{}); ok && len(arr) > 0 {
				if key, ok := arr[0].(string); ok {
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
		startTime = fmt.Sprintf("%d", lastCheckpoint.LastTimestamp)
		lastTS = lastCheckpoint.LastTimestamp
	} else {
		startTime = "1970-01-01T00:00:00Z"
	}

	endTime := time.Now().Format(time.RFC3339Nano)

	batchSize := 10000
	if cfg != nil && cfg.BatchSize > 0 {
		batchSize = cfg.BatchSize
	}
	totalRecords := 0

	for {
		fluxQuery := fmt.Sprintf(`
			from(bucket: "%s")
			  |> range(start: %s, stop: %s)
			  |> filter(fn: (r) => r._measurement == "%s")
			  |> limit(n: %d)
		`, a.config.Bucket, startTime, endTime, measurement, batchSize)

		records, err := a.executeFluxSelect(ctx, fluxQuery)
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

		logger.Debug("fetched batch from InfluxDB V2",
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

	logger.Info("completed InfluxDB V2 query",
		zap.String("measurement", measurement),
		zap.Int("total_records", totalRecords))

	return &types.Checkpoint{
		LastTimestamp: lastTS,
		ProcessedRows: totalProcessed,
	}, nil
}

func (a *InfluxDBV2Adapter) QueryDataBatch(ctx context.Context, measurement string,
	series []string, lastCheckpoint *types.Checkpoint,
	batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

	var startTime string
	if lastCheckpoint != nil && lastCheckpoint.LastTimestamp != 0 {
		startTime = fmt.Sprintf("%d", lastCheckpoint.LastTimestamp)
	} else {
		startTime = "1970-01-01T00:00:00Z"
	}

	batchSize := getBatchSize(cfg)
	fluxFilter := BuildFluxFilter(series)

	var totalRecords int
	var maxTS int64

	for {
		fluxQuery := fmt.Sprintf(`
			from(bucket: "%s")
			  |> range(start: %s)
			  |> filter(fn: (r) => r._measurement == "%s" and (%s))
			  |> limit(n: %d)
		`, a.config.Bucket, startTime, measurement, fluxFilter, batchSize)

		logger.Debug("executing batch query for InfluxDB V2",
			zap.String("measurement", measurement),
			zap.Int("series_count", len(series)),
			zap.String("query", fluxQuery))

		records, err := a.executeFluxSelect(ctx, fluxQuery)
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

		logger.Debug("fetched batch from InfluxDB V2",
			zap.String("measurement", measurement),
			zap.Int("batch_size", len(records)),
			zap.String("next_start", startTime))

		// If we got fewer records than batch size, we're done
		if len(records) < batchSize {
			break
		}

		// Update startTime for next query using the last record's timestamp
		if maxTS < math.MaxInt64 {
			startTime = fmt.Sprintf("%d", maxTS+1)
		} else {
			startTime = fmt.Sprintf("%d", maxTS)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}

	logger.Info("completed batch query for InfluxDB V2",
		zap.String("measurement", measurement),
		zap.Int("series_count", len(series)),
		zap.Int("total_records", totalRecords),
		zap.Int64("max_timestamp", maxTS))

	return &types.Checkpoint{
		LastTimestamp: maxTS,
		ProcessedRows: int64(totalRecords),
	}, nil
}

func (a *InfluxDBV2Adapter) executeFluxSelect(ctx context.Context, query string) ([]types.Record, error) {
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

	var result struct {
		Records []fluxRecord `json:"records"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode flux result: %w", err)
	}

	var records []types.Record
	for _, r := range result.Records {
		record := types.NewRecord()
		record.Time = r.Time.UnixNano()
		record.AddField(r.Field, r.Value)
		record.AddTag("_measurement", r.Measurement)
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

	return nil
}

func influxQuoteIdentifier(s string) string {
	if s == "" {
		return `""`
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
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
