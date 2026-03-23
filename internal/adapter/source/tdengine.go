package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
		transport.TLSClientConfig.InsecureSkipVerify = true
	}
	a.client = &http.Client{Transport: transport}

	return nil
}

func (a *TDengineAdapter) Disconnect(ctx context.Context) error {
	if a.client != nil {
		a.client.CloseIdleConnections()
	}
	logger.Info("disconnecting from TDengine", zap.String("database", a.config.Database))
	return nil
}

func (a *TDengineAdapter) Ping(ctx context.Context) error {
	_, err := a.executeQuery(ctx, "SHOW DATABASES")
	return err
}

func (a *TDengineAdapter) DiscoverTables(ctx context.Context) ([]string, error) {
	query := "SHOW TABLES"
	if a.config != nil && a.config.Database != "" {
		query = fmt.Sprintf("SHOW TABLES FROM %s", a.config.Database)
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

func (a *TDengineAdapter) QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error) (*types.Checkpoint, error) {
	var lastTS time.Time
	if lastCheckpoint != nil {
		lastTS = lastCheckpoint.LastTimestamp
	}

	startTime := lastTS.Format("2006-01-02 15:04:05.000")
	endTime := lastTS.Add(7 * 24 * time.Hour).Format("2006-01-02 15:04:05.000")

	if lastTS.IsZero() {
		startTime = "1970-01-01 00:00:00.000"
	}

	query := fmt.Sprintf(`SELECT *, tbname FROM %s WHERE ts >= '%s' AND ts < '%s' PARTITION BY TBNAME ORDER BY ts`,
		tdengineQuoteIdentifier(table), startTime, endTime)

	data, err := a.executeQuery(ctx, query)
	if err != nil {
		return nil, err
	}

	records := make([]types.Record, 0, len(data))
	var maxTS time.Time
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
						record.Time = t
						if t.After(maxTS) {
							maxTS = t
						}
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
				case int64:
					record.AddField(fmt.Sprintf("col_%d", i), v)
				}
			}
		}

		records = append(records, *record)

		if len(records) >= 1000 {
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
	bodyBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, "POST", a.baseURL+"/rest/sql", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(a.config.User, a.config.Password)

	resp, err := a.client.Do(req)
	if err != nil {
		logger.Error("TDengine query failed", zap.Error(err))
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result TDengineResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Code != 0 {
		logger.Error("TDengine error", zap.String("message", result.Message))
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
		return s
	}
	if s[0] != '`' {
		return "`" + s + "`"
	}
	return s
}
