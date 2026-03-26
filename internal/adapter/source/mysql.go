package source

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

type MySQLAdapter struct {
	db     *sql.DB
	config *MySQLConfig
}

type MySQLConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Charset  string
	SSL      types.SSLConfig
}

func init() {
	adapter.RegisterSourceAdapter("mysql", func() adapter.SourceAdapter {
		return &MySQLAdapter{}
	})
}

func (a *MySQLAdapter) Name() string {
	return "mysql"
}

func (a *MySQLAdapter) SupportedVersions() []string {
	return []string{"5.7", "8.0"}
}

func (a *MySQLAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	cfg := &MySQLConfig{}
	if err := decodeMySQLConfig(config, cfg); err != nil {
		logger.Error("mysql config decode failed", zap.Error(err))
		return err
	}
	a.config = cfg

	logger.Info("connecting to MySQL",
		zap.String("host", cfg.Host),
		zap.Int("port", cfg.Port),
		zap.String("database", cfg.Database))

	dsn := buildMySQLDSN(cfg)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logger.Error("failed to open mysql", zap.Error(err))
		return fmt.Errorf("failed to open mysql: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(1 * time.Hour)

	if err := db.PingContext(ctx); err != nil {
		logger.Error("mysql ping failed", zap.Error(err))
		return fmt.Errorf("failed to ping mysql: %w", err)
	}

	a.db = db
	logger.Info("mysql connection established")
	return nil
}

func buildMySQLDSN(cfg *MySQLConfig) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	if cfg.Charset != "" {
		dsn += "?charset=" + cfg.Charset
	} else {
		dsn += "?charset=utf8mb4"
	}

	if cfg.SSL.Enabled {
		if cfg.SSL.SkipVerify {
			dsn += "&tls=skip-verify"
		}
	}

	return dsn
}

func (a *MySQLAdapter) Disconnect(ctx context.Context) error {
	if a.db != nil {
		logger.Info("disconnecting from MySQL", zap.String("database", a.config.Database))
		return a.db.Close()
	}
	return nil
}

func (a *MySQLAdapter) Ping(ctx context.Context) error {
	if a.db == nil {
		logger.Error("mysql ping failed: connection not established")
		return fmt.Errorf("mysql connection not established")
	}
	return a.db.PingContext(ctx)
}

func (a *MySQLAdapter) DiscoverTables(ctx context.Context) ([]string, error) {
	if a.db == nil {
		return nil, fmt.Errorf("mysql adapter not connected, call Connect first")
	}
	query := "SHOW TABLES"
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to show tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		tables = append(tables, table)
	}

	return tables, rows.Err()
}

func (a *MySQLAdapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
	return []string{measurement}, nil
}

func (a *MySQLAdapter) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
	if a.db == nil {
		return nil, fmt.Errorf("mysql adapter not connected, call Connect first")
	}
	query := `
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`

	rows, err := a.db.QueryContext(ctx, query, a.config.Database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to discover schema for table %s: %w", table, err)
	}
	defer rows.Close()

	schema := &types.TableSchema{
		TableName: table,
		Columns:   make([]types.Column, 0),
	}

	for rows.Next() {
		var col types.Column
		var nullableStr string
		var key string
		if err := rows.Scan(&col.Name, &col.Type, &nullableStr, &key); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		col.Nullable = (nullableStr == "YES")
		if key == "PRI" {
			schema.PrimaryKey = col.Name
		}
		if col.Type == "timestamp" || col.Type == "datetime" {
			schema.TimestampColumn = col.Name
		}
		schema.Columns = append(schema.Columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	return schema, nil
}

func (a *MySQLAdapter) QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	if a.db == nil {
		return nil, fmt.Errorf("mysql adapter not connected, call Connect first")
	}
	var lastID int64
	var lastTS int64
	batchSize := 10000
	if cfg != nil && cfg.BatchSize > 0 {
		batchSize = cfg.BatchSize
	}

	if lastCheckpoint != nil {
		lastID = lastCheckpoint.LastID
		lastTS = lastCheckpoint.LastTimestamp
	}

	totalProcessed := int64(0)
	if lastCheckpoint != nil {
		totalProcessed = lastCheckpoint.ProcessedRows
	}

	for {
		records, err := a.queryBatch(ctx, table, lastTS, lastID, batchSize)
		if err != nil {
			return nil, err
		}

		if len(records) == 0 {
			break
		}

		if err := batchFunc(records); err != nil {
			return nil, fmt.Errorf("batch func failed: %w", err)
		}

		totalProcessed += int64(len(records))

		maxRecord := records[len(records)-1]
		lastID = maxRecord.ID
		lastTS = maxRecord.Time

		if len(records) < batchSize {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		time.Sleep(100 * time.Millisecond)
	}

	return &types.Checkpoint{
		LastID:        lastID,
		LastTimestamp: lastTS,
		ProcessedRows: totalProcessed,
	}, nil
}

type mysqlRecord struct {
	ID     int64
	Time   int64
	Fields map[string]interface{}
	Tags   map[string]string
}

func (a *MySQLAdapter) queryBatch(ctx context.Context, table string, lastTS int64, lastID int64, batchSize int) ([]types.Record, error) {
	query := fmt.Sprintf(`
		SELECT id, timestamp, host_id, cpu_usage, memory_usage
		FROM %s
		WHERE timestamp > ? OR (timestamp = ? AND id > ?)
		ORDER BY timestamp, id
		LIMIT ?`, quoteIdentifier(table))

	lastTSString := ""
	if lastTS > 0 {
		lastTSString = time.Unix(0, lastTS).Format("2006-01-02 15:04:05.000000000")
	}

	rows, err := a.db.QueryContext(ctx, query, lastTSString, lastTSString, lastID, batchSize)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	if rows != nil {
		defer rows.Close()
	}

	var records []types.Record
	for rows.Next() {
		var id int64
		var ts sql.NullString
		var hostID sql.NullString
		var cpuUsage, memoryUsage sql.NullFloat64

		if err := rows.Scan(&id, &ts, &hostID, &cpuUsage, &memoryUsage); err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		record := types.NewRecord()
		record.ID = id
		if ts.Valid {
			if parsed, err := time.Parse("2006-01-02 15:04:05.000000000", ts.String); err == nil {
				record.Time = parsed.UnixNano()
			}
		}

		if hostID.Valid {
			record.AddTag("host_id", hostID.String)
		}
		if cpuUsage.Valid {
			record.AddField("cpu_usage", cpuUsage.Float64)
		}
		if memoryUsage.Valid {
			record.AddField("memory_usage", memoryUsage.Float64)
		}

		records = append(records, *record)
	}

	return records, rows.Err()
}

func decodeMySQLConfig(config map[string]interface{}, cfg interface{}) error {
	cfgMap, ok := config["mysql"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("mysql config not found")
	}

	if v, ok := cfgMap["host"].(string); ok {
		cfg.(*MySQLConfig).Host = v
	}
	if v, ok := cfgMap["port"].(int); ok {
		cfg.(*MySQLConfig).Port = v
	}
	if v, ok := cfgMap["user"].(string); ok {
		cfg.(*MySQLConfig).User = v
	}
	if v, ok := cfgMap["password"].(string); ok {
		cfg.(*MySQLConfig).Password = v
	}
	if v, ok := cfgMap["database"].(string); ok {
		cfg.(*MySQLConfig).Database = v
	}
	if v, ok := cfgMap["charset"].(string); ok {
		cfg.(*MySQLConfig).Charset = v
	}

	return nil
}

func quoteIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
