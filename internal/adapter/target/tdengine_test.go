package target

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

func TestTDengineTargetAdapter_Name(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	if adapter.Name() != "tdengine-target" {
		t.Errorf("Expected name 'tdengine-target', got %s", adapter.Name())
	}
}

func TestTDengineTargetAdapter_SupportedVersions(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	versions := adapter.SupportedVersions()
	if len(versions) != 2 {
		t.Errorf("Expected 2 versions, got %d", len(versions))
	}
	if versions[0] != "2.x" || versions[1] != "3.x" {
		t.Errorf("Expected [2.x, 3.x], got %v", versions)
	}
}

func TestTDengineTargetAdapter_Connect_InvalidConfig(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	ctx := context.Background()

	err := adapter.Connect(ctx, map[string]interface{}{})
	if err == nil {
		t.Error("Expected error for invalid config")
	}
}

func TestTDengineTargetAdapter_Connect_ValidConfig(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	ctx := context.Background()

	config := map[string]interface{}{
		"tdengine": map[string]interface{}{
			"host":     "localhost",
			"port":     6030,
			"username": "root",
			"password": "test",
			"database": "test",
			"version":  "3.x",
		},
	}

	err := adapter.Connect(ctx, config)
	if err != nil {
		t.Logf("Connect error (may be expected if no TDengine server): %v", err)
	}
}

func TestTDengineTargetAdapter_Ping_NotConnected(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	ctx := context.Background()

	err := adapter.Ping(ctx)
	if err == nil {
		t.Error("Expected error when not connected")
	}
}

func TestTDengineTargetAdapter_Disconnect(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	ctx := context.Background()

	err := adapter.Disconnect(ctx)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestTDengineTargetAdapter_Disconnect_WithConfig(t *testing.T) {
	adapter := &TDengineTargetAdapter{
		config: &TDengineTargetConfig{
			Database: "test",
		},
	}
	ctx := context.Background()

	err := adapter.Disconnect(ctx)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestTDengineTargetAdapter_WriteBatch_Empty(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	ctx := context.Background()

	err := adapter.WriteBatch(ctx, "test_table", []types.Record{})
	if err != nil {
		t.Errorf("Unexpected error for empty batch: %v", err)
	}
}

func TestTDengineTargetAdapter_WriteBatch_NotConnected(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	ctx := context.Background()

	record := types.NewRecord()
	record.Time = time.Now().UnixNano()
	record.AddField("cpu", 85.5)

	err := adapter.WriteBatch(ctx, "test_table", []types.Record{*record})
	if err == nil {
		t.Error("Expected error when not connected")
	}
}

func TestTDengineTargetAdapter_MeasurementExists_NotConnected(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	ctx := context.Background()

	exists, err := adapter.MeasurementExists(ctx, "test_table")
	if err == nil {
		t.Error("Expected error when not connected")
	}
	if exists {
		t.Error("Expected exists to be false when not connected")
	}
}

func TestTDengineTargetAdapter_CreateMeasurement_NilSchema(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	ctx := context.Background()

	err := adapter.CreateMeasurement(ctx, nil)
	if err != nil {
		t.Errorf("Unexpected error for nil schema: %v", err)
	}
}

func TestTDengineTargetAdapter_CreateMeasurement_NotConnected(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
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

func TestTDengineTargetAdapter_RecordToLineProtocol(t *testing.T) {
	adapter := &TDengineTargetAdapter{}

	record := types.NewRecord()
	record.Time = time.Unix(1709853600, 0).UnixNano()
	record.AddTag("host", "server1")
	record.AddField("cpu", 85.5)
	record.AddField("memory", 70.2)

	line, err := adapter.recordToLineProtocol("metrics", *record)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if !strings.Contains(line, "metrics,host=server1") {
		t.Errorf("Expected line to contain metrics,host=server1, got %q", line)
	}
	if !strings.Contains(line, "cpu=85.5") {
		t.Errorf("Expected line to contain cpu=85.5, got %q", line)
	}
	if !strings.Contains(line, "memory=70.2") {
		t.Errorf("Expected line to contain memory=70.2, got %q", line)
	}
	if !strings.HasSuffix(line, "1709853600000000000") {
		t.Errorf("Expected line to end with timestamp, got %q", line)
	}
}

func TestTDengineTargetAdapter_RecordToLineProtocol_EmptyFields(t *testing.T) {
	adapter := &TDengineTargetAdapter{}

	record := types.NewRecord()
	record.Time = time.Unix(1709853600, 0).UnixNano()
	record.AddTag("host", "server1")

	line, err := adapter.recordToLineProtocol("metrics", *record)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if line != "" {
		t.Errorf("Expected empty line for record with no fields, got %q", line)
	}
}

func TestTDengineTargetAdapter_RecordToLineProtocol_EmptyTags(t *testing.T) {
	adapter := &TDengineTargetAdapter{}

	record := types.NewRecord()
	record.Time = time.Unix(1709853600, 0).UnixNano()
	record.AddField("cpu", 85.5)

	line, err := adapter.recordToLineProtocol("metrics", *record)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expected := "metrics cpu=85.5 1709853600000000000"
	if line != expected {
		t.Errorf("Expected line %q, got %q", expected, line)
	}
}

func TestTDengineTargetAdapter_RecordToLineProtocol_NilField(t *testing.T) {
	adapter := &TDengineTargetAdapter{}

	record := types.NewRecord()
	record.Time = time.Unix(1709853600, 0).UnixNano()
	record.AddField("cpu", nil)
	record.AddField("memory", 70.2)

	line, err := adapter.recordToLineProtocol("metrics", *record)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expected := "metrics memory=70.2 1709853600000000000"
	if line != expected {
		t.Errorf("Expected line %q, got %q", expected, line)
	}
}

func TestTDengineTargetAdapter_RecordToLineProtocol_EmptyTagValue(t *testing.T) {
	adapter := &TDengineTargetAdapter{}

	record := types.NewRecord()
	record.Time = time.Unix(1709853600, 0).UnixNano()
	record.AddTag("host", "")
	record.AddField("cpu", 85.5)

	line, err := adapter.recordToLineProtocol("metrics", *record)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expected := "metrics cpu=85.5 1709853600000000000"
	if line != expected {
		t.Errorf("Expected line %q, got %q", expected, line)
	}
}

func TestTDengineTargetAdapter_GoTypeToTDengineType(t *testing.T) {
	adapter := &TDengineTargetAdapter{}

	tests := []struct {
		input    string
		expected string
	}{
		{"float", "FLOAT"},
		{"float64", "FLOAT"},
		{"float32", "FLOAT"},
		{"int", "INT"},
		{"int64", "BIGINT"},
		{"string", "VARCHAR(255)"},
		{"bool", "BOOL"},
		{"datetime", "TIMESTAMP"},
		{"unknown", "VARCHAR(255)"},
	}

	for _, tt := range tests {
		result := adapter.goTypeToTDengineType(tt.input)
		if result != tt.expected {
			t.Errorf("goTypeToTDengineType(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestDecodeTDengineTargetConfig(t *testing.T) {
	config := map[string]interface{}{
		"tdengine": map[string]interface{}{
			"host":     "localhost",
			"port":     6030,
			"username": "root",
			"password": "password",
			"database": "testdb",
			"version":  "3.x",
		},
	}

	cfg := &TDengineTargetConfig{}
	err := decodeTDengineTargetConfig(config, cfg)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if cfg.Host != "localhost" {
		t.Errorf("Expected host 'localhost', got %s", cfg.Host)
	}
	if cfg.Port != 6030 {
		t.Errorf("Expected port 6030, got %d", cfg.Port)
	}
	if cfg.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got %s", cfg.Database)
	}
	if cfg.Version != "3.x" {
		t.Errorf("Expected version '3.x', got %s", cfg.Version)
	}
}

func TestDecodeTDengineTargetConfig_Missing(t *testing.T) {
	config := map[string]interface{}{}

	cfg := &TDengineTargetConfig{}
	err := decodeTDengineTargetConfig(config, cfg)
	if err == nil {
		t.Error("Expected error for missing tdengine config")
	}
}

func TestTDengineTargetAdapter_BuildCreateTableDDL(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	adapter.config = &TDengineTargetConfig{
		Database: "testdb",
	}

	schema := &types.Schema{
		Measurement: "metrics",
		Fields: []types.FieldMapping{
			{SourceName: "cpu", TargetName: "cpu", DataType: "float"},
			{SourceName: "memory", TargetName: "memory", DataType: "int64"},
		},
		Tags: []types.FieldMapping{
			{SourceName: "host", TargetName: "host", DataType: "string"},
		},
	}

	ddl, isSTABLE, err := adapter.buildCreateTableDDL(schema)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if ddl == "" {
		t.Error("Expected non-empty DDL")
	}

	if !contains(ddl, "CREATE STABLE IF NOT EXISTS testdb.`metrics`") {
		t.Errorf("Expected DDL to contain CREATE STABLE, got: %s", ddl)
	}

	if !isSTABLE {
		t.Error("Expected isSTABLE to be true")
	}
}

func TestTDengineTargetAdapter_BuildCreateTableDDL_NoTags(t *testing.T) {
	adapter := &TDengineTargetAdapter{}
	adapter.config = &TDengineTargetConfig{
		Database: "testdb",
	}

	schema := &types.Schema{
		Measurement: "metrics",
		Fields: []types.FieldMapping{
			{SourceName: "cpu", TargetName: "cpu", DataType: "float"},
		},
		Tags: []types.FieldMapping{},
	}

	ddl, isSTABLE, err := adapter.buildCreateTableDDL(schema)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if ddl == "" {
		t.Error("Expected non-empty DDL")
	}

	if !contains(ddl, "CREATE TABLE IF NOT EXISTS testdb.`metrics`") {
		t.Errorf("Expected DDL to contain CREATE TABLE (not STABLE), got: %s", ddl)
	}

	if isSTABLE {
		t.Error("Expected isSTABLE to be false")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
