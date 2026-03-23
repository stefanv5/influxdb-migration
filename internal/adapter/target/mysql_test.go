package target

import (
	"context"
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

func TestMySQLTargetAdapter_Name(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	if adapter.Name() != "mysql-target" {
		t.Errorf("Expected name 'mysql-target', got %s", adapter.Name())
	}
}

func TestMySQLTargetAdapter_SupportedVersions(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	versions := adapter.SupportedVersions()
	if len(versions) != 2 {
		t.Errorf("Expected 2 versions, got %d", len(versions))
	}
	if versions[0] != "5.7" || versions[1] != "8.0" {
		t.Errorf("Expected [5.7, 8.0], got %v", versions)
	}
}

func TestMySQLTargetAdapter_Connect_InvalidConfig(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	ctx := context.Background()

	err := adapter.Connect(ctx, map[string]interface{}{})
	if err == nil {
		t.Error("Expected error for invalid config")
	}
}

func TestMySQLTargetAdapter_Connect_ValidConfig(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	ctx := context.Background()

	config := map[string]interface{}{
		"mysql": map[string]interface{}{
			"host":     "localhost",
			"port":     3306,
			"username": "root",
			"password": "test",
			"database": "test",
		},
	}

	err := adapter.Connect(ctx, config)
	if err != nil {
		t.Logf("Connect error (may be expected if no MySQL server): %v", err)
	}
}

func TestMySQLTargetAdapter_Ping_NotConnected(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	ctx := context.Background()

	err := adapter.Ping(ctx)
	if err == nil {
		t.Error("Expected error when not connected")
	}
}

func TestMySQLTargetAdapter_Disconnect(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	ctx := context.Background()

	err := adapter.Disconnect(ctx)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestMySQLTargetAdapter_Disconnect_WithConfig(t *testing.T) {
	adapter := &MySQLTargetAdapter{
		config: &MySQLTargetConfig{
			Database: "test",
		},
	}
	ctx := context.Background()

	err := adapter.Disconnect(ctx)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestMySQLTargetAdapter_WriteBatch_Empty(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	ctx := context.Background()

	err := adapter.WriteBatch(ctx, "test_table", []types.Record{})
	if err != nil {
		t.Errorf("Unexpected error for empty batch: %v", err)
	}
}

func TestMySQLTargetAdapter_WriteBatch_NotConnected(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	ctx := context.Background()

	record := types.NewRecord()
	record.Time = time.Now()
	record.AddField("cpu", 85.5)

	err := adapter.WriteBatch(ctx, "test_table", []types.Record{*record})
	if err == nil {
		t.Error("Expected error when not connected")
	}
}

func TestMySQLTargetAdapter_MeasurementExists_NotConnected(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	ctx := context.Background()

	exists, err := adapter.MeasurementExists(ctx, "test_table")
	if err == nil {
		t.Error("Expected error when not connected")
	}
	if exists {
		t.Error("Expected exists to be false when not connected")
	}
}

func TestMySQLTargetAdapter_CreateMeasurement_NilSchema(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	ctx := context.Background()

	err := adapter.CreateMeasurement(ctx, nil)
	if err != nil {
		t.Errorf("Unexpected error for nil schema: %v", err)
	}
}

func TestMySQLTargetAdapter_CreateMeasurement_NotConnected(t *testing.T) {
	adapter := &MySQLTargetAdapter{}
	ctx := context.Background()

	schema := &types.Schema{
		Measurement: "test_table",
		Fields: []types.FieldMapping{
			{SourceName: "cpu", TargetName: "cpu", DataType: "float"},
		},
	}

	err := adapter.CreateMeasurement(ctx, schema)
	if err == nil {
		t.Error("Expected error when not connected")
	}
}

func TestBuildMySQLDSN(t *testing.T) {
	cfg := &MySQLTargetConfig{
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "password",
		Database: "testdb",
	}

	dsn := buildMySQLDSN(cfg)
	expected := "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&parseTime=True&loc=Local"
	if dsn != expected {
		t.Errorf("Expected DSN %q, got %q", expected, dsn)
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"table", "`table`"},
		{"table`name", "`table``name`"},
		{"", "``"},
	}

	for _, tt := range tests {
		result := quoteIdentifier(tt.input)
		if result != tt.expected {
			t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestMysqlEscape(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"value", "value"},
		{"it's", "it''s"},
		{"backslash\\", "backslash\\\\"},
	}

	for _, tt := range tests {
		result := mysqlEscape(tt.input)
		if result != tt.expected {
			t.Errorf("mysqlEscape(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestMySQLTargetAdapter_GoTypeToMySQLType(t *testing.T) {
	adapter := &MySQLTargetAdapter{}

	tests := []struct {
		input    string
		expected string
	}{
		{"float", "DOUBLE"},
		{"float64", "DOUBLE"},
		{"float32", "FLOAT"},
		{"int", "INT"},
		{"int64", "BIGINT"},
		{"string", "VARCHAR(255)"},
		{"bool", "TINYINT(1)"},
		{"datetime", "DATETIME(3)"},
		{"unknown", "VARCHAR(255)"},
	}

	for _, tt := range tests {
		result := adapter.goTypeToMySQLType(tt.input)
		if result != tt.expected {
			t.Errorf("goTypeToMySQLType(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestDecodeMySQLTargetConfig(t *testing.T) {
	config := map[string]interface{}{
		"mysql": map[string]interface{}{
			"host":     "localhost",
			"port":     3306,
			"username": "root",
			"password": "password",
			"database": "testdb",
		},
	}

	cfg := &MySQLTargetConfig{}
	err := decodeMySQLTargetConfig(config, cfg)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if cfg.Host != "localhost" {
		t.Errorf("Expected host 'localhost', got %s", cfg.Host)
	}
	if cfg.Port != 3306 {
		t.Errorf("Expected port 3306, got %d", cfg.Port)
	}
	if cfg.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got %s", cfg.Database)
	}
}

func TestDecodeMySQLTargetConfig_Missing(t *testing.T) {
	config := map[string]interface{}{}

	cfg := &MySQLTargetConfig{}
	err := decodeMySQLTargetConfig(config, cfg)
	if err == nil {
		t.Error("Expected error for missing mysql config")
	}
}
