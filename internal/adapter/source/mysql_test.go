package source

import (
	"context"
	"testing"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

// Note: Integration tests for MySQL's DiscoverSchema would require a real MySQL database
// and should be run separately with appropriate test fixtures.

// TestMySQLAdapter_Name tests the Name method returns correct value
func TestMySQLAdapter_Name(t *testing.T) {
	adapter := &MySQLAdapter{}
	if adapter.Name() != "mysql" {
		t.Errorf("Expected name 'mysql', got %s", adapter.Name())
	}
}

// TestMySQLAdapter_SupportedVersions tests SupportedVersions returns expected versions
func TestMySQLAdapter_SupportedVersions(t *testing.T) {
	adapter := &MySQLAdapter{}
	versions := adapter.SupportedVersions()
	if len(versions) != 2 {
		t.Errorf("Expected 2 versions, got %d", len(versions))
	}
	if versions[0] != "5.7" || versions[1] != "8.0" {
		t.Errorf("Expected [5.7, 8.0], got %v", versions)
	}
}

// TestMySQLAdapter_Connect_InvalidConfig tests Connect with missing config
func TestMySQLAdapter_Connect_InvalidConfig(t *testing.T) {
	adapter := &MySQLAdapter{}
	ctx := context.Background()

	err := adapter.Connect(ctx, map[string]interface{}{})
	if err == nil {
		t.Error("Expected error for invalid config")
	}
}

// TestMySQLAdapter_Disconnect_NotConnected tests Disconnect when not connected
func TestMySQLAdapter_Disconnect_NotConnected(t *testing.T) {
	adapter := &MySQLAdapter{}
	ctx := context.Background()

	err := adapter.Disconnect(ctx)
	if err != nil {
		t.Errorf("Unexpected error when disconnecting unconnected adapter: %v", err)
	}
}

// TestMySQLAdapter_Ping_NotConnected tests Ping when not connected
func TestMySQLAdapter_Ping_NotConnected(t *testing.T) {
	adapter := &MySQLAdapter{}
	ctx := context.Background()

	err := adapter.Ping(ctx)
	if err == nil {
		t.Error("Expected error when pinging unconnected adapter")
	}
}

// TestMySQLAdapter_DiscoverTables_NotConnected tests DiscoverTables when not connected
func TestMySQLAdapter_DiscoverTables_NotConnected(t *testing.T) {
	adapter := &MySQLAdapter{}
	ctx := context.Background()

	_, err := adapter.DiscoverTables(ctx)
	if err == nil {
		t.Error("Expected error when discovering tables on unconnected adapter")
	}
}

// TestMySQLAdapter_DiscoverSchema_NotConnected tests DiscoverSchema when not connected
func TestMySQLAdapter_DiscoverSchema_NotConnected(t *testing.T) {
	adapter := &MySQLAdapter{}
	ctx := context.Background()

	_, err := adapter.DiscoverSchema(ctx, "test_table")
	if err == nil {
		t.Error("Expected error when discovering schema on unconnected adapter")
	}
}

// TestMySQLAdapter_QueryData_NotConnected tests QueryData when not connected
func TestMySQLAdapter_QueryData_NotConnected(t *testing.T) {
	adapter := &MySQLAdapter{}
	ctx := context.Background()

	_, err := adapter.QueryData(ctx, "test_table", nil, func([]types.Record) error {
		return nil
	}, nil)
	if err == nil {
		t.Error("Expected error when querying data on unconnected adapter")
	}
}

// TestInfluxDBV1Adapter_Name tests the Name method for InfluxDB V1
func TestInfluxDBV1Adapter_Name(t *testing.T) {
	adapter := &InfluxDBV1Adapter{}
	if adapter.Name() != "influxdb-v1" {
		t.Errorf("Expected name 'influxdb-v1', got %s", adapter.Name())
	}
}

// TestInfluxDBV1Adapter_SupportedVersions tests SupportedVersions for V1
func TestInfluxDBV1Adapter_SupportedVersions(t *testing.T) {
	adapter := &InfluxDBV1Adapter{}
	versions := adapter.SupportedVersions()
	if len(versions) != 1 {
		t.Errorf("Expected 1 version, got %d", len(versions))
	}
	if versions[0] != "1.x" {
		t.Errorf("Expected [1.x], got %v", versions)
	}
}

// TestInfluxDBV1Adapter_DiscoverSchema_Stub tests that DiscoverSchema returns minimal schema (stub)
func TestInfluxDBV1Adapter_DiscoverSchema_Stub(t *testing.T) {
	adapter := &InfluxDBV1Adapter{}
	ctx := context.Background()

	schema, err := adapter.DiscoverSchema(ctx, "test_measurement")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if schema == nil {
		t.Fatal("Expected non-nil schema")
	}

	if schema.TableName != "test_measurement" {
		t.Errorf("Expected TableName 'test_measurement', got %s", schema.TableName)
	}

	// InfluxDB is schemaless, so Columns should be empty
	if len(schema.Columns) != 0 {
		t.Errorf("Expected empty Columns for InfluxDB schemaless adapter, got %d columns", len(schema.Columns))
	}

	// PrimaryKey and TimestampColumn should not be set for InfluxDB
	if schema.PrimaryKey != "" {
		t.Errorf("Expected empty PrimaryKey, got %s", schema.PrimaryKey)
	}
	if schema.TimestampColumn != "" {
		t.Errorf("Expected empty TimestampColumn, got %s", schema.TimestampColumn)
	}
}

// TestInfluxDBV2Adapter_Name tests the Name method for InfluxDB V2
func TestInfluxDBV2Adapter_Name(t *testing.T) {
	adapter := &InfluxDBV2Adapter{}
	if adapter.Name() != "influxdb-v2" {
		t.Errorf("Expected name 'influxdb-v2', got %s", adapter.Name())
	}
}

// TestInfluxDBV2Adapter_SupportedVersions tests SupportedVersions for V2
func TestInfluxDBV2Adapter_SupportedVersions(t *testing.T) {
	adapter := &InfluxDBV2Adapter{}
	versions := adapter.SupportedVersions()
	if len(versions) != 1 {
		t.Errorf("Expected 1 version, got %d", len(versions))
	}
	if versions[0] != "2.x" {
		t.Errorf("Expected [2.x], got %v", versions)
	}
}

// TestInfluxDBV2Adapter_DiscoverSchema_Stub tests that DiscoverSchema returns minimal schema (stub)
func TestInfluxDBV2Adapter_DiscoverSchema_Stub(t *testing.T) {
	adapter := &InfluxDBV2Adapter{}
	ctx := context.Background()

	schema, err := adapter.DiscoverSchema(ctx, "test_measurement")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if schema == nil {
		t.Fatal("Expected non-nil schema")
	}

	if schema.TableName != "test_measurement" {
		t.Errorf("Expected TableName 'test_measurement', got %s", schema.TableName)
	}

	// InfluxDB 2.x is schemaless, so Columns should be empty
	if len(schema.Columns) != 0 {
		t.Errorf("Expected empty Columns for InfluxDB schemaless adapter, got %d columns", len(schema.Columns))
	}
}

// TestTDengineAdapter_Name tests the Name method for TDengine
func TestTDengineAdapter_Name(t *testing.T) {
	adapter := &TDengineAdapter{}
	if adapter.Name() != "tdengine" {
		t.Errorf("Expected name 'tdengine', got %s", adapter.Name())
	}
}

// TestTDengineAdapter_SupportedVersions tests SupportedVersions for TDengine
func TestTDengineAdapter_SupportedVersions(t *testing.T) {
	adapter := &TDengineAdapter{}
	versions := adapter.SupportedVersions()
	if len(versions) != 2 {
		t.Errorf("Expected 2 versions, got %d", len(versions))
	}
	if versions[0] != "2.x" || versions[1] != "3.x" {
		t.Errorf("Expected [2.x, 3.x], got %v", versions)
	}
}

// TestTDengineAdapter_DiscoverSchema_Stub tests that DiscoverSchema returns minimal schema (stub)
func TestTDengineAdapter_DiscoverSchema_Stub(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	schema, err := adapter.DiscoverSchema(ctx, "test_table")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if schema == nil {
		t.Fatal("Expected non-nil schema")
	}

	if schema.TableName != "test_table" {
		t.Errorf("Expected TableName 'test_table', got %s", schema.TableName)
	}

	// TDengine schemas are managed via STABLE definitions, return empty columns
	if len(schema.Columns) != 0 {
		t.Errorf("Expected empty Columns for TDengine adapter, got %d columns", len(schema.Columns))
	}
}

// TestTDengineAdapter_Connect_InvalidConfig tests Connect with invalid config
func TestTDengineAdapter_Connect_InvalidConfig(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	err := adapter.Connect(ctx, map[string]interface{}{})
	if err == nil {
		t.Error("Expected error for invalid config")
	}
}

// TestTDengineAdapter_Disconnect_NotConnected tests Disconnect when not connected
func TestTDengineAdapter_Disconnect_NotConnected(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	err := adapter.Disconnect(ctx)
	if err != nil {
		t.Errorf("Unexpected error when disconnecting unconnected adapter: %v", err)
	}
}

// TestTDengineAdapter_Ping_NotConnected tests Ping when not connected
func TestTDengineAdapter_Ping_NotConnected(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	err := adapter.Ping(ctx)
	if err == nil {
		t.Error("Expected error when pinging unconnected adapter")
	}
}

// TestTDengineAdapter_DiscoverTables_NotConnected tests DiscoverTables when not connected
func TestTDengineAdapter_DiscoverTables_NotConnected(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	_, err := adapter.DiscoverTables(ctx)
	if err == nil {
		t.Error("Expected error when discovering tables on unconnected adapter")
	}
}

// TestTDengineAdapter_QueryData_NotConnected tests QueryData when not connected
func TestTDengineAdapter_QueryData_NotConnected(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	_, err := adapter.QueryData(ctx, "test_table", nil, func([]types.Record) error {
		return nil
	}, nil)
	if err == nil {
		t.Error("Expected error when querying data on unconnected adapter")
	}
}

// TestQuoteIdentifier tests the quoteIdentifier helper
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
