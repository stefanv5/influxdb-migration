package target

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/migration-tools/influx-migrator/internal/adapter"
	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/migration-tools/influx-migrator/pkg/types"
	"go.uber.org/zap"
)

type MySQLTargetAdapter struct {
	db     *sql.DB
	config *MySQLTargetConfig
}

type MySQLTargetConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
	SSL      types.SSLConfig
}

func init() {
	adapter.RegisterTargetAdapter("mysql", func() adapter.TargetAdapter {
		return &MySQLTargetAdapter{}
	})
}

func (a *MySQLTargetAdapter) Name() string {
	return "mysql-target"
}

func (a *MySQLTargetAdapter) SupportedVersions() []string {
	return []string{"5.7", "8.0"}
}

func (a *MySQLTargetAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	cfg := &MySQLTargetConfig{}
	if err := decodeMySQLTargetConfig(config, cfg); err != nil {
		logger.Error("mysql target config decode failed", zap.Error(err))
		return err
	}
	a.config = cfg

	logger.Info("connecting to MySQL target",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database))

	dsn := buildMySQLDSN(cfg)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logger.Error("failed to open mysql connection", zap.Error(err))
		return fmt.Errorf("failed to open mysql: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(1 * time.Hour)

	if err := db.PingContext(ctx); err != nil {
		logger.Error("mysql target ping failed", zap.Error(err))
		return fmt.Errorf("failed to ping mysql: %w", err)
	}

	a.db = db
	logger.Info("mysql target connection established")
	return nil
}

func buildMySQLDSN(cfg *MySQLTargetConfig) string {
	charset := "utf8mb4"
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=UTC",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database, charset)
	return dsn
}

func (a *MySQLTargetAdapter) Disconnect(ctx context.Context) error {
	if a.db != nil {
		logger.Info("disconnecting from MySQL target", zap.String("database", a.config.Database))
		return a.db.Close()
	}
	return nil
}

func (a *MySQLTargetAdapter) Ping(ctx context.Context) error {
	if a.db == nil {
		logger.Error("mysql target ping failed: connection not established")
		return fmt.Errorf("mysql target connection not established")
	}
	return a.db.PingContext(ctx)
}

func (a *MySQLTargetAdapter) WriteBatch(ctx context.Context, table string, records []types.Record) error {
	if len(records) == 0 {
		return nil
	}

	if a.db == nil {
		return fmt.Errorf("mysql target not connected")
	}

	if err := a.writeRecords(ctx, table, records); err != nil {
		logger.Error("mysql target write batch failed",
			zap.String("table", table),
			zap.Int("records", len(records)),
			zap.Error(err))
		return err
	}

	logger.Debug("mysql target batch written",
		zap.String("table", table),
		zap.Int("count", len(records)))

	return nil
}

func (a *MySQLTargetAdapter) writeRecords(ctx context.Context, table string, records []types.Record) error {
	if len(records) == 0 {
		return nil
	}

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	colList, placeholders, updateColumns, err := a.buildInsertSQL(records[0])
	if err != nil {
		tx.Rollback()
		return err
	}

	valueStrings := make([]string, 0, len(records))
	valueArgs := make([]interface{}, 0, len(records)*len(colList))

	for _, record := range records {
		values, err := a.recordToValues(record, colList)
		if err != nil {
			tx.Rollback()
			return err
		}
		valueStrings = append(valueStrings, "("+placeholders+")")
		valueArgs = append(valueArgs, values...)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		quoteIdentifier(table), strings.Join(colList, ", "), strings.Join(valueStrings, ","), updateColumns)

	_, err = tx.ExecContext(ctx, query, valueArgs...)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (a *MySQLTargetAdapter) buildInsertSQL(record types.Record) ([]string, string, string, error) {
	var columns []string
	var placeholders []string
	var updateColumns []string

	columns = append(columns, "timestamp")
	placeholders = append(placeholders, "?")

	for key := range record.Tags {
		columns = append(columns, quoteIdentifier(key))
		placeholders = append(placeholders, "?")
		updateColumns = append(updateColumns, fmt.Sprintf("%s=VALUES(%s)", quoteIdentifier(key), quoteIdentifier(key)))
	}

	for key := range record.Fields {
		if _, exists := record.Tags[key]; exists {
			continue
		}
		columns = append(columns, quoteIdentifier(key))
		placeholders = append(placeholders, "?")
		updateColumns = append(updateColumns, fmt.Sprintf("%s=VALUES(%s)", quoteIdentifier(key), quoteIdentifier(key)))
	}

	return columns,
		strings.Join(placeholders, ", "),
		strings.Join(updateColumns, ", "),
		nil
}

func (a *MySQLTargetAdapter) recordToValues(record types.Record, columns []string) ([]interface{}, error) {
	values := make([]interface{}, 0, len(columns))

	for _, col := range columns {
		if col == "timestamp" {
			values = append(values, record.Time)
			continue
		}

		if v, exists := record.Tags[col]; exists {
			values = append(values, v)
			continue
		}

		if v, exists := record.Fields[col]; exists {
			if v == nil {
				values = append(values, nil)
			} else {
				values = append(values, v)
			}
			continue
		}

		values = append(values, nil)
	}

	return values, nil
}

func (a *MySQLTargetAdapter) MeasurementExists(ctx context.Context, name string) (bool, error) {
	if a.db == nil {
		return false, fmt.Errorf("mysql target not connected")
	}

	query := fmt.Sprintf("SHOW TABLES LIKE '%s'", mysqlEscape(name))
	row := a.db.QueryRowContext(ctx, query)

	var result string
	err := row.Scan(&result)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (a *MySQLTargetAdapter) CreateMeasurement(ctx context.Context, schema *types.Schema) error {
	if schema == nil {
		return nil
	}

	if a.db == nil {
		return fmt.Errorf("mysql target not connected")
	}

	ddl, err := a.buildCreateTableDDL(schema)
	if err != nil {
		return err
	}

	logger.Info("creating mysql table", zap.String("table", schema.Measurement), zap.String("ddl", ddl))

	_, err = a.db.ExecContext(ctx, ddl)
	return err
}

func (a *MySQLTargetAdapter) buildCreateTableDDL(schema *types.Schema) (string, error) {
	var columns []string
	var uniqueKeyCols []string

	columns = append(columns, "id BIGINT AUTO_INCREMENT PRIMARY KEY")

	uniqueKeyCols = append(uniqueKeyCols, "timestamp")

	for _, tag := range schema.Tags {
		colName := tag.TargetName
		if colName == "" {
			colName = tag.SourceName
		}
		columns = append(columns, fmt.Sprintf("%s VARCHAR(255)", quoteIdentifier(colName)))
		uniqueKeyCols = append(uniqueKeyCols, quoteIdentifier(colName))
	}

	for _, field := range schema.Fields {
		colName := field.TargetName
		if colName == "" {
			colName = field.SourceName
		}
		colType := a.goTypeToMySQLType(field.DataType)
		columns = append(columns, fmt.Sprintf("%s %s", quoteIdentifier(colName), colType))
	}

	columns = append(columns, "timestamp DATETIME(3) NOT NULL")
	columns = append(columns, fmt.Sprintf("UNIQUE KEY uk_identity (%s)", strings.Join(uniqueKeyCols, ", ")))

	tableName := quoteIdentifier(schema.Measurement)
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		tableName, strings.Join(columns, ", "))

	return ddl, nil
}

func (a *MySQLTargetAdapter) goTypeToMySQLType(dataType string) string {
	switch strings.ToLower(dataType) {
	case "float", "float64", "double":
		return "DOUBLE"
	case "float32":
		return "FLOAT"
	case "int", "int32", "int64", "bigint", "uint", "uint32", "uint64":
		return "BIGINT"
	case "string", "varchar", "text":
		return "VARCHAR(1024)"
	case "bool", "boolean":
		return "TINYINT(1)"
	case "datetime", "time":
		return "DATETIME(3)"
	default:
		return "VARCHAR(255)"
	}
}

func decodeMySQLTargetConfig(config map[string]interface{}, cfg interface{}) error {
	cfgMap, ok := config["mysql"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("mysql config not found")
	}

	if v, ok := cfgMap["host"].(string); ok {
		cfg.(*MySQLTargetConfig).Host = v
	}
	if v, ok := cfgMap["port"].(int); ok {
		cfg.(*MySQLTargetConfig).Port = v
	}
	if v, ok := cfgMap["username"].(string); ok {
		cfg.(*MySQLTargetConfig).Username = v
	}
	if v, ok := cfgMap["password"].(string); ok {
		cfg.(*MySQLTargetConfig).Password = v
	}
	if v, ok := cfgMap["database"].(string); ok {
		cfg.(*MySQLTargetConfig).Database = v
	}

	return nil
}

func quoteIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func mysqlEscape(s string) string {
	s = strings.ReplaceAll(s, "'", "''")
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "_", "\\_")
	s = strings.ReplaceAll(s, "%", "\\%")
	return s
}
