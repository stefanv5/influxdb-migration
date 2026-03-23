package target

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/migration-tools/influx-migrator/internal/adapter"
	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/migration-tools/influx-migrator/pkg/types"
	"go.uber.org/zap"
)

type TDengineTargetAdapter struct {
	client       *http.Client
	config       *TDengineTargetConfig
	baseURL      string
	stableTables map[string]bool
}

type TDengineTargetConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
	Version  string
	SSL      types.SSLConfig
}

type tdengineWriteResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func init() {
	adapter.RegisterTargetAdapter("tdengine", func() adapter.TargetAdapter {
		return &TDengineTargetAdapter{}
	})
}

func (a *TDengineTargetAdapter) Name() string {
	return "tdengine-target"
}

func (a *TDengineTargetAdapter) SupportedVersions() []string {
	return []string{"2.x", "3.x"}
}

func (a *TDengineTargetAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	cfg := &TDengineTargetConfig{}
	if err := decodeTDengineTargetConfig(config, cfg); err != nil {
		logger.Error("tdengine target config decode failed", zap.Error(err))
		return err
	}
	a.config = cfg

	protocol := "http"
	if cfg.SSL.Enabled {
		protocol = "https"
	}
	a.baseURL = fmt.Sprintf("%s://%s:%d", protocol, cfg.Host, cfg.Port)

	logger.Info("connecting to TDengine target",
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

func (a *TDengineTargetAdapter) Disconnect(ctx context.Context) error {
	if a.client != nil {
		a.client.CloseIdleConnections()
	}
	if a.config != nil {
		logger.Info("disconnecting from TDengine target", zap.String("database", a.config.Database))
	}
	return nil
}

func (a *TDengineTargetAdapter) Ping(ctx context.Context) error {
	if a.client == nil {
		return fmt.Errorf("tdengine target not connected")
	}

	query := "SHOW DATABASES"
	resp, err := a.executeQuery(ctx, query)
	if err != nil {
		logger.Error("tdengine target ping failed", zap.Error(err))
		return err
	}

	if resp.Code != 0 {
		return fmt.Errorf("tdengine ping failed: %s", resp.Message)
	}

	return nil
}

func (a *TDengineTargetAdapter) WriteBatch(ctx context.Context, table string, records []types.Record) error {
	if len(records) == 0 {
		return nil
	}

	if a.client == nil {
		return fmt.Errorf("tdengine target not connected")
	}

	if err := a.writeRecords(ctx, table, records); err != nil {
		logger.Error("tdengine target write batch failed",
			zap.String("table", table),
			zap.Int("records", len(records)),
			zap.Error(err))
		return err
	}

	logger.Debug("tdengine target batch written",
		zap.String("table", table),
		zap.Int("count", len(records)))

	return nil
}

func (a *TDengineTargetAdapter) writeRecords(ctx context.Context, table string, records []types.Record) error {
	if len(records) == 0 {
		return nil
	}

	isSTABLE := a.stableTables != nil && a.stableTables[table]

	if isSTABLE {
		return a.writeSTABLEBatched(ctx, table, records)
	}
	return a.writeTableBatched(ctx, table, records)
}

func (a *TDengineTargetAdapter) writeTableBatched(ctx context.Context, table string, records []types.Record) error {
	var valueStrings []string

	for _, record := range records {
		fieldStr, err := a.buildFieldValues(record)
		if err != nil {
			return err
		}
		timestamp := record.Time.UnixNano()
		valueStrings = append(valueStrings, fmt.Sprintf("('%d', %s)", timestamp, fieldStr))
	}

	if len(valueStrings) == 0 {
		return nil
	}

	query := fmt.Sprintf("INSERT INTO %s.%s VALUES %s",
		a.config.Database, tdengineEscapeIdentifier(table), strings.Join(valueStrings, ","))

	resp, err := a.executeQuery(ctx, query)
	if err != nil {
		return err
	}

	if resp.Code != 0 {
		return fmt.Errorf("tdengine insert failed: %s", resp.Message)
	}

	return nil
}

func (a *TDengineTargetAdapter) writeSTABLEBatched(ctx context.Context, table string, records []types.Record) error {
	var queries []string

	for _, record := range records {
		fieldStr, err := a.buildFieldValues(record)
		if err != nil {
			return err
		}

		var tagValues []string
		for _, v := range record.Tags {
			tagValues = append(tagValues, fmt.Sprintf("'%s'", escapeTDengineTagValue(v)))
		}

		timestamp := record.Time.UnixNano()
		childTable := fmt.Sprintf("%s_%d", table, timestamp)

		if len(tagValues) > 0 {
			queries = append(queries, fmt.Sprintf("INSERT INTO %s.%s USING %s.%s TAGS (%s) VALUES ('%d', %s)",
				a.config.Database, tdengineEscapeIdentifier(childTable), a.config.Database, tdengineEscapeIdentifier(table),
				strings.Join(tagValues, ", "), timestamp, fieldStr))
		} else {
			queries = append(queries, fmt.Sprintf("INSERT INTO %s.%s VALUES ('%d', %s)",
				a.config.Database, tdengineEscapeIdentifier(table), timestamp, fieldStr))
		}
	}

	if len(queries) == 0 {
		return nil
	}

	fullQuery := strings.Join(queries, ";")
	resp, err := a.executeQuery(ctx, fullQuery)
	if err != nil {
		return err
	}

	if resp.Code != 0 {
		return fmt.Errorf("tdengine insert failed: %s", resp.Message)
	}

	return nil
}

func (a *TDengineTargetAdapter) buildFieldValues(record types.Record) (string, error) {
	var fieldParts []string

	for _, field := range record.Fields {
		if field == nil {
			fieldParts = append(fieldParts, "NULL")
			continue
		}
		switch v := field.(type) {
		case float64:
			fieldParts = append(fieldParts, fmt.Sprintf("%v", v))
		case float32:
			fieldParts = append(fieldParts, fmt.Sprintf("%v", v))
		case int64:
			fieldParts = append(fieldParts, fmt.Sprintf("%di", v))
		case int:
			fieldParts = append(fieldParts, fmt.Sprintf("%di", v))
		case int32:
			fieldParts = append(fieldParts, fmt.Sprintf("%di", v))
		case string:
			fieldParts = append(fieldParts, fmt.Sprintf("'%s'", escapeStringValue(v)))
		case bool:
			if v {
				fieldParts = append(fieldParts, "true")
			} else {
				fieldParts = append(fieldParts, "false")
			}
		default:
			fieldParts = append(fieldParts, fmt.Sprintf("'%v'", v))
		}
	}

	return strings.Join(fieldParts, ", "), nil
}

func (a *TDengineTargetAdapter) recordToLineProtocol(table string, record types.Record) (string, error) {
	var tagParts []string
	for k, v := range record.Tags {
		if v == "" {
			continue
		}
		tagParts = append(tagParts, fmt.Sprintf("%s=%s", escapeTDengineValue(k), escapeTDengineValue(v)))
	}

	var fieldParts []string
	for k, v := range record.Fields {
		if v == nil {
			continue
		}
		fieldParts = append(fieldParts, fmt.Sprintf("%s=%s", escapeTDengineValue(k), formatTDengineFieldValue(v)))
	}

	if len(fieldParts) == 0 {
		return "", nil
	}

	tagStr := ""
	if len(tagParts) > 0 {
		tagStr = "," + strings.Join(tagParts, ",")
	}

	timestamp := record.Time.UnixNano()

	return fmt.Sprintf("%s%s %s %d", table, tagStr, strings.Join(fieldParts, ","), timestamp), nil
}

func formatTDengineFieldValue(v interface{}) string {
	switch val := v.(type) {
	case float64:
		return fmt.Sprintf("%v", val)
	case float32:
		return fmt.Sprintf("%v", val)
	case int64:
		return fmt.Sprintf("%di", val)
	case int:
		return fmt.Sprintf("%di", val)
	case int32:
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

func escapeTDengineValue(s string) string {
	s = strings.ReplaceAll(s, "=", "\\=")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, " ", "\\ ")
	return s
}

func tdengineEscapeIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func escapeTDengineTagValue(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return s
}

func (a *TDengineTargetAdapter) MeasurementExists(ctx context.Context, name string) (bool, error) {
	if a.client == nil {
		return false, fmt.Errorf("tdengine target not connected")
	}

	query := fmt.Sprintf("DESCRIBE %s.%s", a.config.Database, name)
	resp, err := a.executeQuery(ctx, query)
	if err != nil {
		return false, err
	}

	if resp.Code != 0 {
		return false, nil
	}
	return true, nil
}

func (a *TDengineTargetAdapter) CreateMeasurement(ctx context.Context, schema *types.Schema) error {
	if schema == nil {
		return nil
	}

	if a.client == nil {
		return fmt.Errorf("tdengine target not connected")
	}

	ddl, isSTABLE, err := a.buildCreateTableDDL(schema)
	if err != nil {
		return err
	}

	if a.stableTables == nil {
		a.stableTables = make(map[string]bool)
	}
	a.stableTables[schema.Measurement] = isSTABLE

	logger.Info("creating tdengine table", zap.String("table", schema.Measurement), zap.String("ddl", ddl), zap.Bool("is_stable", isSTABLE))

	resp, err := a.executeQuery(ctx, ddl)
	if err != nil {
		return err
	}

	if resp.Code != 0 {
		return fmt.Errorf("create table failed: %s", resp.Message)
	}

	return nil
}

func (a *TDengineTargetAdapter) buildCreateTableDDL(schema *types.Schema) (string, bool, error) {
	var fieldDefs []string
	var tagDefs []string

	for _, field := range schema.Fields {
		colName := field.TargetName
		if colName == "" {
			colName = field.SourceName
		}
		colType := a.goTypeToTDengineType(field.DataType)
		fieldDefs = append(fieldDefs, fmt.Sprintf("%s %s", tdengineEscapeIdentifier(colName), colType))
	}

	for _, tag := range schema.Tags {
		colName := tag.TargetName
		if colName == "" {
			colName = tag.SourceName
		}
		tagDefs = append(tagDefs, fmt.Sprintf("%s VARCHAR(255)", tdengineEscapeIdentifier(colName)))
	}

	var ddl string
	tableName := schema.Measurement
	isSTABLE := len(tagDefs) > 0

	if isSTABLE {
		ddl = fmt.Sprintf("CREATE STABLE IF NOT EXISTS %s.%s (%s) TAGS (%s)",
			a.config.Database, tdengineEscapeIdentifier(tableName),
			strings.Join(fieldDefs, ", "),
			strings.Join(tagDefs, ", "))
	} else {
		ddl = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s)",
			a.config.Database, tdengineEscapeIdentifier(tableName),
			strings.Join(fieldDefs, ", "))
	}

	return ddl, isSTABLE, nil
}

func (a *TDengineTargetAdapter) goTypeToTDengineType(dataType string) string {
	switch strings.ToLower(dataType) {
	case "float", "float64":
		return "FLOAT"
	case "float32":
		return "FLOAT"
	case "int", "int32":
		return "INT"
	case "int64", "bigint":
		return "BIGINT"
	case "string", "varchar", "text":
		return "VARCHAR(255)"
	case "bool", "boolean":
		return "BOOL"
	case "datetime", "time":
		return "TIMESTAMP"
	default:
		return "VARCHAR(255)"
	}
}

func (a *TDengineTargetAdapter) executeQuery(ctx context.Context, query string) (*tdengineWriteResponse, error) {
	reqBody := map[string]string{
		"sql": query,
		"db":  a.config.Database,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	u, err := url.Parse(a.baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}
	u.Path = "/rest/sql"

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(a.config.Username, a.config.Password)

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result tdengineWriteResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &result, nil
}

func decodeTDengineTargetConfig(config map[string]interface{}, cfg interface{}) error {
	cfgMap, ok := config["tdengine"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("tdengine config not found")
	}

	if v, ok := cfgMap["host"].(string); ok {
		cfg.(*TDengineTargetConfig).Host = v
	}
	if v, ok := cfgMap["port"].(int); ok {
		cfg.(*TDengineTargetConfig).Port = v
	}
	if v, ok := cfgMap["username"].(string); ok {
		cfg.(*TDengineTargetConfig).Username = v
	}
	if v, ok := cfgMap["password"].(string); ok {
		cfg.(*TDengineTargetConfig).Password = v
	}
	if v, ok := cfgMap["database"].(string); ok {
		cfg.(*TDengineTargetConfig).Database = v
	}
	if v, ok := cfgMap["version"].(string); ok {
		cfg.(*TDengineTargetConfig).Version = v
	}

	return nil
}
