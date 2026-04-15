package target

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

type InfluxDBV1TargetAdapter struct {
	client  *http.Client
	config  *InfluxDBV1TargetConfig
	baseURL string
}

type InfluxDBV1TargetConfig struct {
	URL      string
	Username string
	Password string
	Database string
	SSL      types.SSLConfig
}

type influxV1WriteResult struct {
	Results []struct {
		Series []struct {
			Name    string          `json:"name"`
			Columns []string        `json:"columns"`
			Values  [][]interface{} `json:"values"`
		} `json:"series"`
	} `json:"results"`
	Error string `json:"error"`
}

func init() {
	adapter.RegisterTargetAdapter("influxdb-v1", func() adapter.TargetAdapter {
		return &InfluxDBV1TargetAdapter{}
	})
}

func (a *InfluxDBV1TargetAdapter) Name() string {
	return "influxdb-v1-target"
}

func (a *InfluxDBV1TargetAdapter) SupportedVersions() []string {
	return []string{"1.x"}
}

func (a *InfluxDBV1TargetAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	cfg := &InfluxDBV1TargetConfig{}
	if err := decodeInfluxV1TargetConfig(config, cfg); err != nil {
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

func (a *InfluxDBV1TargetAdapter) Disconnect(ctx context.Context) error {
	if a.client != nil {
		a.client.CloseIdleConnections()
	}
	return nil
}

func (a *InfluxDBV1TargetAdapter) Ping(ctx context.Context) error {
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
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("ping failed with status: %d", resp.StatusCode)
	}

	return nil
}

func (a *InfluxDBV1TargetAdapter) WriteBatch(ctx context.Context, measurement string, records []types.Record) error {
	if len(records) == 0 {
		return nil
	}

	var lines []string
	for _, r := range records {
		line := a.formatInfluxLine(measurement, r)
		if line != "" {
			lines = append(lines, line)
		}
	}

	if len(lines) == 0 {
		return nil
	}

	body := strings.Join(lines, "\n")
	return a.writeLines(ctx, body)
}

func (a *InfluxDBV1TargetAdapter) formatInfluxLine(measurement string, r types.Record) string {
	tagsStr := formatTags(r.Tags)
	fieldsStr := formatFields(r.Fields)

	if fieldsStr == "" {
		return ""
	}

	timestampStr := ""
	if r.Time != 0 {
		timestampStr = fmt.Sprintf(" %d", r.Time)
	}

	if tagsStr != "" {
		return fmt.Sprintf("%s,%s %s%s", measurement, tagsStr, fieldsStr, timestampStr)
	}
	return fmt.Sprintf("%s %s%s", measurement, fieldsStr, timestampStr)
}

func formatTags(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}

	var parts []string
	for k, v := range tags {
		parts = append(parts, fmt.Sprintf("%s=%s", escapeTagValue(k), escapeTagValue(v)))
	}
	return strings.Join(parts, ",")
}

func formatFields(fields map[string]interface{}) string {
	if len(fields) == 0 {
		return ""
	}

	var parts []string
	for k, v := range fields {
		if v == nil {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s=%v", escapeFieldKey(k), formatFieldValue(v)))
	}
	return strings.Join(parts, ",")
}

func formatFieldValue(v interface{}) string {
	switch val := v.(type) {
	case float64:
		return fmt.Sprintf("%v", val)
	case float32:
		return fmt.Sprintf("%v", val)
	case int64:
		return fmt.Sprintf("%di", val)
	case int:
		return fmt.Sprintf("%di", val)
	case string:
		return fmt.Sprintf("\"%s\"", escapeStringValue(val))
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}

func escapeTagValue(s string) string {
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "=", "\\=")
	s = strings.ReplaceAll(s, " ", "\\ ")
	return s
}

func escapeStringValue(s string) string {
	// Must escape backslashes first, then quotes
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return s
}

func escapeFieldKey(s string) string {
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "=", "\\=")
	s = strings.ReplaceAll(s, " ", "\\ ")
	return s
}

func (a *InfluxDBV1TargetAdapter) writeLines(ctx context.Context, body string) error {
	u, err := url.Parse(a.baseURL)
	if err != nil {
		return fmt.Errorf("invalid base URL: %w", err)
	}
	u.Path = "/write"
	u.RawQuery = url.Values{
		"db": []string{a.config.Database},
	}.Encode()

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewBufferString(body))
	if err != nil {
		return err
	}

	if a.config.Username != "" {
		req.SetBasicAuth(a.config.Username, a.config.Password)
	}

	req.Header.Set("Content-Type", "text/plain")

	resp, err := a.client.Do(req)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("write failed with status %d, failed to read response: %w", resp.StatusCode, err)
		}
		bodyStr := string(respBody)
		if len(bodyStr) > 200 {
			bodyStr = bodyStr[:200] + "..."
		}
		return fmt.Errorf("write failed with status %d: %s", resp.StatusCode, bodyStr)
	}

	return nil
}

func (a *InfluxDBV1TargetAdapter) MeasurementExists(ctx context.Context, name string) (bool, error) {
	query := fmt.Sprintf("SHOW MEASUREMENTS WHERE \"name\" = '%s'", influxEscape(name))
	resp, err := a.executeQuery(ctx, query)
	if err != nil {
		return false, err
	}

	for _, r := range resp {
		for _, values := range r.Values {
			if len(values) > 0 {
				if n, ok := values[0].(string); ok && n == name {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func influxEscape(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return s
}

func (a *InfluxDBV1TargetAdapter) CreateMeasurement(ctx context.Context, schema *types.Schema) error {
	return nil
}

func (a *InfluxDBV1TargetAdapter) executeQuery(ctx context.Context, query string) ([]influxV1Series, error) {
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

	var result influxV1QueryResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}

	return result.Results, nil
}

type influxV1Series struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags"`
	Columns []string          `json:"columns"`
	Values  [][]interface{}   `json:"values"`
}

type influxV1QueryResult struct {
	Results []influxV1Series `json:"results"`
	Error   string           `json:"error"`
}

func decodeInfluxV1TargetConfig(config map[string]interface{}, cfg interface{}) error {
	cfgMap, ok := config["influxdb"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("influxdb config not found")
	}

	if v, ok := cfgMap["url"].(string); ok {
		cfg.(*InfluxDBV1TargetConfig).URL = v
	}
	if v, ok := cfgMap["database"].(string); ok {
		cfg.(*InfluxDBV1TargetConfig).Database = v
	}
	if basicAuth, ok := cfgMap["basic_auth"].(map[string]interface{}); ok {
		if u, ok := basicAuth["username"].(string); ok {
			cfg.(*InfluxDBV1TargetConfig).Username = u
		}
		if p, ok := basicAuth["password"].(string); ok {
			cfg.(*InfluxDBV1TargetConfig).Password = p
		}
	}

	return nil
}

type InfluxDBV2TargetAdapter struct {
	client *http.Client
	config *InfluxDBV2TargetConfig
}

type InfluxDBV2TargetConfig struct {
	URL    string
	Token  string
	Org    string
	Bucket string
	SSL    types.SSLConfig
}

type fluxWriteRecord struct {
	Measurement string            `json:"_measurement"`
	Field       string            `json:"_field"`
	Value       interface{}       `json:"_value"`
	Time        int64             `json:"_time"`
	Tags        map[string]string `json:"tags,omitempty"`
}

func init() {
	adapter.RegisterTargetAdapter("influxdb-v2", func() adapter.TargetAdapter {
		return &InfluxDBV2TargetAdapter{}
	})
}

func (a *InfluxDBV2TargetAdapter) Name() string {
	return "influxdb-v2-target"
}

func (a *InfluxDBV2TargetAdapter) SupportedVersions() []string {
	return []string{"2.x"}
}

func (a *InfluxDBV2TargetAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	cfg := &InfluxDBV2TargetConfig{}
	if err := decodeInfluxV2TargetConfig(config, cfg); err != nil {
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

func (a *InfluxDBV2TargetAdapter) Disconnect(ctx context.Context) error {
	if a.client != nil {
		a.client.CloseIdleConnections()
	}
	return nil
}

func (a *InfluxDBV2TargetAdapter) Ping(ctx context.Context) error {
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
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping failed with status: %d", resp.StatusCode)
	}

	return nil
}

func (a *InfluxDBV2TargetAdapter) WriteBatch(ctx context.Context, measurement string, records []types.Record) error {
	if len(records) == 0 {
		return nil
	}

	lines, err := a.formatFluxRecords(measurement, records)
	if err != nil {
		return err
	}

	return a.writeLines(ctx, lines)
}

func (a *InfluxDBV2TargetAdapter) formatFluxRecords(measurement string, records []types.Record) ([]string, error) {
	var lines []string

	for _, r := range records {
		if len(r.Fields) == 0 {
			continue
		}

		// Build field part: field1=value1,field2=value2
		var fieldParts []string
		for field, value := range r.Fields {
			if value == nil {
				continue
			}
			fieldParts = append(fieldParts, fmt.Sprintf("%s=%s", escapeFieldKey(field), formatFieldValue(value)))
		}

		if len(fieldParts) == 0 {
			continue
		}

		// InfluxDB v2 write line protocol: measurement,tag1=val1 field1=val1,field2=val2 timestamp
		tagsStr := formatTags(r.Tags)
		if tagsStr != "" {
			lines = append(lines, fmt.Sprintf("%s,%s %s %d", measurement, tagsStr, strings.Join(fieldParts, ","), r.Time))
		} else {
			lines = append(lines, fmt.Sprintf("%s %s %d", measurement, strings.Join(fieldParts, ","), r.Time))
		}
	}

	return lines, nil
}

func (a *InfluxDBV2TargetAdapter) writeLines(ctx context.Context, lines []string) error {
	if len(lines) == 0 {
		return nil
	}

	body := strings.Join(lines, "\n")

	u, err := url.Parse(a.config.URL)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	u.Path = "/api/v2/write"

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewBufferString(body))
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Token "+a.config.Token)
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Accept", "application/json")

	queryParams := url.Values{}
	queryParams.Set("org", a.config.Org)
	queryParams.Set("bucket", a.config.Bucket)
	u.RawQuery = queryParams.Encode()

	resp, err := a.client.Do(req)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("write failed with status %d, failed to read response: %w", resp.StatusCode, err)
		}
		bodyStr := string(respBody)
		if len(bodyStr) > 200 {
			bodyStr = bodyStr[:200] + "..."
		}
		return fmt.Errorf("write failed with status %d: %s", resp.StatusCode, bodyStr)
	}

	return nil
}

func (a *InfluxDBV2TargetAdapter) MeasurementExists(ctx context.Context, name string) (bool, error) {
	fluxQuery := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.measurements(bucket: "%s")`, a.config.Bucket)

	results, err := a.executeFluxQuery(ctx, fluxQuery)
	if err != nil {
		return false, err
	}

	for _, r := range results {
		for _, v := range r {
			if arr, ok := v.([]interface{}); ok && len(arr) > 0 {
				if n, ok := arr[0].(string); ok && n == name {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (a *InfluxDBV2TargetAdapter) CreateMeasurement(ctx context.Context, schema *types.Schema) error {
	return nil
}

func (a *InfluxDBV2TargetAdapter) executeFluxQuery(ctx context.Context, query string) ([][]interface{}, error) {
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

func decodeInfluxV2TargetConfig(config map[string]interface{}, cfg interface{}) error {
	cfgMap, ok := config["influxdb"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("influxdb config not found")
	}

	if v, ok := cfgMap["url"].(string); ok {
		cfg.(*InfluxDBV2TargetConfig).URL = v
	}
	if v, ok := cfgMap["token"].(string); ok {
		cfg.(*InfluxDBV2TargetConfig).Token = v
	}
	if v, ok := cfgMap["org"].(string); ok {
		cfg.(*InfluxDBV2TargetConfig).Org = v
	}
	if v, ok := cfgMap["bucket"].(string); ok {
		cfg.(*InfluxDBV2TargetConfig).Bucket = v
	}

	return nil
}
