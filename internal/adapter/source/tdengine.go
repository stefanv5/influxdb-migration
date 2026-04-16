package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/migration-tools/influx-migrator/internal/adapter"
	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/migration-tools/influx-migrator/pkg/types"
	"go.uber.org/zap"
)

type TDengineAdapter struct {
	client  *http.Client
	config  *TDengineConfig
	baseURL string
}

type TDengineConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Version  string
	SSL      types.SSLConfig
}

type TDengineResponse struct {
	Code    int     `json:"code"`
	Message string  `json:"message"`
	Data    [][]any `json:"data"`
}

func init() {
	adapter.RegisterSourceAdapter("tdengine", func() adapter.SourceAdapter {
		return &TDengineAdapter{}
	})
}

// timestampRegex validates the TDengine timestamp format used in queries
var timestampRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$`)

func (a *TDengineAdapter) Name() string {
	return "tdengine"
}

func (a *TDengineAdapter) SupportedVersions() []string {
	return []string{"2.x", "3.x"}
}

func (a *TDengineAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	cfg := &TDengineConfig{}
	if err := decodeTDConfig(config, cfg); err != nil {
		logger.Error("tdengine config decode failed", zap.Error(err))
		return err
	}
	a.config = cfg

	protocol := "http"
	if cfg.SSL.Enabled {
		protocol = "https"
	}
	a.baseURL = fmt.Sprintf("%s://%s:%d", protocol, cfg.Host, cfg.Port)

	logger.Info("connecting to TDengine",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database),
		zap.String("version", cfg.Version))

	transport := &http.Transport{}
	if cfg.SSL.Enabled && cfg.SSL.SkipVerify {
		// Require explicit opt-in via environment variable for insecure TLS
		if os.Getenv("ALLOW_INSECURE_TLS") != "1" {
			logger.Error("TLS certificate verification is disabled - set ALLOW_INSECURE_TLS=1 environment variable to allow",
				zap.String("url", a.baseURL))
			return fmt.Errorf("insecure TLS requires ALLOW_INSECURE_TLS=1 environment variable")
		}
		logger.Warn("TLS certificate verification is disabled - this is insecure and not recommended for production use",
			zap.String("url", a.baseURL))
		transport.TLSClientConfig.InsecureSkipVerify = true
	}
	a.client = &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	return nil
}

func (a *TDengineAdapter) Disconnect(ctx context.Context) error {
	if a.client != nil {
		a.client.CloseIdleConnections()
	}
	if a.config != nil {
		logger.Info("disconnecting from TDengine", zap.String("database", a.config.Database))
	}
	return nil
}

func (a *TDengineAdapter) Ping(ctx context.Context) error {
	_, err := a.executeQuery(ctx, "SHOW DATABASES")
	return err
}

func (a *TDengineAdapter) DiscoverTables(ctx context.Context) ([]string, error) {
	query := "SHOW TABLES"
	if a.config != nil && a.config.Database != "" {
		query = fmt.Sprintf("SHOW TABLES FROM %s", tdengineQuoteIdentifier(a.config.Database))
	}

	data, err := a.executeQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	var tables []string
	for _, row := range data {
		if len(row) > 0 {
			if name, ok := row[0].(string); ok {
				tables = append(tables, name)
			}
		}
	}

	return tables, nil
}

func (a *TDengineAdapter) DiscoverTablesFromStable(ctx context.Context, stable string) ([]string, error) {
	query := fmt.Sprintf("SHOW TABLES FROM %s", stable)
	data, err := a.executeQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	var tables []string
	for _, row := range data {
		if len(row) > 0 {
			if name, ok := row[0].(string); ok {
				tables = append(tables, name)
			}
		}
	}

	return tables, nil
}

func (a *TDengineAdapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
	return a.DiscoverTablesFromStable(ctx, measurement)
}

// DiscoverShardGroups returns an error since TDengine's sharding is handled internally
func (a *TDengineAdapter) DiscoverShardGroups(ctx context.Context) ([]*adapter.ShardGroup, error) {
	return nil, fmt.Errorf("DiscoverShardGroups is not directly supported for TDengine source")
}

// DiscoverSeriesInTimeWindow returns series for the given time window
func (a *TDengineAdapter) DiscoverSeriesInTimeWindow(ctx context.Context, measurement string, startTime, endTime time.Time) ([]string, error) {
	// For TDengine, we use the regular discovery since it handles time-based queries differently
	return a.DiscoverTablesFromStable(ctx, measurement)
}

// DiscoverTagKeys returns nil for TDengine adapter.
// TDengine uses a different data model and doesn't need tag/field distinction.
func (a *TDengineAdapter) DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error) {
	return nil, nil
}

func (a *TDengineAdapter) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
	// TDengine is a time-series database and does not have a direct equivalent
	// to MySQL's INFORMATION_SCHEMA. TDengine's schema is defined by the STABLE,
	// and columns are fixed for all sub-tables. For now, return a minimal schema.
	return &types.TableSchema{
		TableName: table,
		Columns:   []types.Column{},
	}, nil
}

func (a *TDengineAdapter) QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	var lastTS int64
	if lastCheckpoint != nil {
		lastTS = lastCheckpoint.LastTimestamp
	}

	timeWindow := types.DefaultTimeWindow
	if cfg != nil && cfg.TimeWindow > 0 {
		timeWindow = cfg.TimeWindow
	}

	var startTime string
	var endTime string
	if lastTS == 0 {
		startTime = "1970-01-01 00:00:00.000"
		endTime = time.Unix(0, 0).Add(timeWindow).Format("2006-01-02 15:04:05.000")
	} else {
		startTime = time.Unix(0, lastTS).Format("2006-01-02 15:04:05.000")
		endTime = time.Unix(0, lastTS).Add(timeWindow).Format("2006-01-02 15:04:05.000")
	}

	// Validate timestamps match expected format before query interpolation
	if !timestampRegex.MatchString(startTime) || !timestampRegex.MatchString(endTime) {
		return nil, fmt.Errorf("invalid timestamp format: start=%s, end=%s", startTime, endTime)
	}

	query := fmt.Sprintf(`SELECT *, tbname FROM %s WHERE ts >= '%s' AND ts < '%s' PARTITION BY TBNAME ORDER BY ts`,
		tdengineQuoteIdentifier(table), startTime, endTime)

	data, err := a.executeQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	records := make([]types.Record, 0, len(data))
	var maxTS int64
	totalProcessed := int64(0)

	if lastCheckpoint != nil {
		totalProcessed = lastCheckpoint.ProcessedRows
	}

	for _, row := range data {
		record := types.NewRecord()

		for i, val := range row {
			if i == 0 {
				if ts, ok := val.(string); ok {
					if t, err := time.Parse("2006-01-02 15:04:05.000", ts); err == nil {
						record.Time = t.UnixNano()
						if t.UnixNano() > maxTS {
							maxTS = t.UnixNano()
						}
					} else {
						logger.Warn("failed to parse TDengine timestamp",
							zap.String("timestamp_string", ts),
							zap.Error(err))
					}
				}
			} else if val != nil {
				switch v := val.(type) {
				case string:
					if v != "" {
						record.AddField(fmt.Sprintf("col_%d", i), v)
					}
				case float64:
					record.AddField(fmt.Sprintf("col_%d", i), v)
				case float32:
					record.AddField(fmt.Sprintf("col_%d", i), float64(v))
				case int64:
					record.AddField(fmt.Sprintf("col_%d", i), v)
				case int:
					record.AddField(fmt.Sprintf("col_%d", i), int64(v))
				case int32:
					record.AddField(fmt.Sprintf("col_%d", i), int64(v))
				case int16:
					record.AddField(fmt.Sprintf("col_%d", i), int64(v))
				case int8:
					record.AddField(fmt.Sprintf("col_%d", i), int64(v))
				case uint64:
					record.AddField(fmt.Sprintf("col_%d", i), int64(v))
				case uint:
					record.AddField(fmt.Sprintf("col_%d", i), int64(v))
				case uint32:
					record.AddField(fmt.Sprintf("col_%d", i), int64(v))
				case bool:
					record.AddField(fmt.Sprintf("col_%d", i), v)
				}
			}
		}

		records = append(records, *record)

		internalBatchSize := 1000
		if cfg != nil && cfg.BatchSize > 0 {
			internalBatchSize = cfg.BatchSize
		}
		if len(records) >= internalBatchSize {
			if err := batchFunc(records); err != nil {
				return nil, err
			}
			totalProcessed += int64(len(records))
			records = records[:0]
		}
	}

	if len(records) > 0 {
		if err := batchFunc(records); err != nil {
			return nil, err
		}
		totalProcessed += int64(len(records))
	}

	return &types.Checkpoint{
		LastTimestamp: maxTS,
		ProcessedRows: totalProcessed,
	}, nil
}

// QueryDataBatch is not supported for TDengine adapter.
// Batch series query is only available for InfluxDB sources.
func (a *TDengineAdapter) QueryDataBatch(ctx context.Context, table string, series []string,
	startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
	batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	return nil, fmt.Errorf("QueryDataBatch is not supported for TDengine adapter, use QueryData instead")
}

func (a *TDengineAdapter) executeQuery(ctx context.Context, query string) ([][]any, error) {
	if a.config == nil {
		return nil, fmt.Errorf("TDengine adapter not connected, call Connect first")
	}

	logger.Debug("executing TDengine query",
		zap.String("query", query),
		zap.String("database", a.config.Database))

	reqBody := map[string]string{
		"sql": query,
		"db":  a.config.Database,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", a.baseURL+"/rest/sql", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(a.config.User, a.config.Password)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("TDengine query failed: %w", err)
	}
	defer resp.Body.Close()

	// Check if context was cancelled during/after HTTP call
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var result TDengineResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Code != 0 {
		return nil, fmt.Errorf("TDengine error: %s", result.Message)
	}

	logger.Debug("TDengine query completed",
		zap.Int("rows", len(result.Data)))
	return result.Data, nil
}

func decodeTDConfig(config map[string]interface{}, cfg interface{}) error {
	cfgMap, ok := config["tdengine"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("tdengine config not found")
	}

	if v, ok := cfgMap["host"].(string); ok {
		cfg.(*TDengineConfig).Host = v
	}
	if v, ok := cfgMap["port"].(int); ok {
		cfg.(*TDengineConfig).Port = v
	}
	if v, ok := cfgMap["user"].(string); ok {
		cfg.(*TDengineConfig).User = v
	}
	if v, ok := cfgMap["password"].(string); ok {
		cfg.(*TDengineConfig).Password = v
	}
	if v, ok := cfgMap["database"].(string); ok {
		cfg.(*TDengineConfig).Database = v
	}
	if v, ok := cfgMap["version"].(string); ok {
		cfg.(*TDengineConfig).Version = v
	}

	return nil
}

func tdengineQuoteIdentifier(s string) string {
	if s == "" {
		return "`" + "`"
	}
	result := "`" + strings.ReplaceAll(s, "`", "``") + "`"
	return result
}
