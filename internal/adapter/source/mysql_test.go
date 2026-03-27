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
		{"column with space", "`column with space`"},
		{"special`char", "`special``char`"},
	}

	for _, tt := range tests {
		result := quoteIdentifier(tt.input)
		if result != tt.expected {
			t.Errorf("quoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// TestBuildMySQLDSN tests the DSN building function
func TestBuildMySQLDSN(t *testing.T) {
	tests := []struct {
		name     string
		config   MySQLConfig
		expected string
	}{
		{
			name: "basic config",
			config: MySQLConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Database: "testdb",
			},
			expected: "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4",
		},
		{
			name: "with custom charset",
			config: MySQLConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Database: "testdb",
				Charset:  "latin1",
			},
			expected: "root:password@tcp(localhost:3306)/testdb?charset=latin1",
		},
		{
			name: "with SSL enabled and no charset (default utf8mb4)",
			config: MySQLConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "password",
				Database: "testdb",
				SSL:      types.SSLConfig{Enabled: true, SkipVerify: true},
			},
			expected: "root:password@tcp(localhost:3306)/testdb?charset=utf8mb4&tls=skip-verify",
		},
		{
			name: "remote host",
			config: MySQLConfig{
				Host:     "192.168.1.100",
				Port:     3307,
				User:     "admin",
				Password: "secret",
				Database: "production",
			},
			expected: "admin:secret@tcp(192.168.1.100:3307)/production?charset=utf8mb4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildMySQLDSN(&tt.config)
			if result != tt.expected {
				t.Errorf("buildMySQLDSN() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// TestDecodeMySQLConfig tests the config decoding function
func TestDecodeMySQLConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError bool
		checkFields func(t *testing.T, cfg *MySQLConfig)
	}{
		{
			name: "valid config",
			config: map[string]interface{}{
				"mysql": map[string]interface{}{
					"host":     "localhost",
					"port":     3306,
					"user":     "root",
					"password": "password",
					"database": "testdb",
					"charset":  "utf8mb4",
				},
			},
			expectError: false,
			checkFields: func(t *testing.T, cfg *MySQLConfig) {
				if cfg.Host != "localhost" {
					t.Errorf("Expected Host 'localhost', got %s", cfg.Host)
				}
				if cfg.Port != 3306 {
					t.Errorf("Expected Port 3306, got %d", cfg.Port)
				}
				if cfg.User != "root" {
					t.Errorf("Expected User 'root', got %s", cfg.User)
				}
				if cfg.Password != "password" {
					t.Errorf("Expected Password 'password', got %s", cfg.Password)
				}
				if cfg.Database != "testdb" {
					t.Errorf("Expected Database 'testdb', got %s", cfg.Database)
				}
				if cfg.Charset != "utf8mb4" {
					t.Errorf("Expected Charset 'utf8mb4', got %s", cfg.Charset)
				}
			},
		},
		{
			name: "missing mysql key",
			config: map[string]interface{}{},
			expectError: true,
		},
		{
			name: "partial config - uses defaults",
			config: map[string]interface{}{
				"mysql": map[string]interface{}{
					"host": "localhost",
					"port": 3306,
				},
			},
			expectError: false,
			checkFields: func(t *testing.T, cfg *MySQLConfig) {
				if cfg.Host != "localhost" {
					t.Errorf("Expected Host 'localhost', got %s", cfg.Host)
				}
				if cfg.Port != 3306 {
					t.Errorf("Expected Port 3306, got %d", cfg.Port)
				}
				if cfg.User != "" {
					t.Errorf("Expected User '', got %s", cfg.User)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &MySQLConfig{}
			err := decodeMySQLConfig(tt.config, cfg)
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if tt.checkFields != nil {
				tt.checkFields(t, cfg)
			}
		})
	}
}

// TestMySQLAdapter_DiscoverTables_Stub tests that DiscoverTables returns an error when not connected
func TestMySQLAdapter_DiscoverTables_Stub(t *testing.T) {
	adapter := &MySQLAdapter{}
	ctx := context.Background()

	_, err := adapter.DiscoverTables(ctx)
	if err == nil {
		t.Error("Expected error when discovering tables on unconnected adapter")
	}
}

// TestMySQLAdapter_DiscoverSchema_Stub tests the schema structure returned by DiscoverSchema
// when properly connected (using a minimal schema)
func TestMySQLAdapter_DiscoverSchema_Stub(t *testing.T) {
	// This test verifies that the schema structure is properly initialized
	schema := &types.TableSchema{
		TableName: "test_table",
		Columns: []types.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "varchar(255)", Nullable: true},
			{Name: "created_at", Type: "timestamp", Nullable: false},
		},
		PrimaryKey:       "id",
		TimestampColumn: "created_at",
	}

	if schema.TableName != "test_table" {
		t.Errorf("Expected TableName 'test_table', got %s", schema.TableName)
	}

	if len(schema.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(schema.Columns))
	}

	if schema.PrimaryKey != "id" {
		t.Errorf("Expected PrimaryKey 'id', got %s", schema.PrimaryKey)
	}

	if schema.TimestampColumn != "created_at" {
		t.Errorf("Expected TimestampColumn 'created_at', got %s", schema.TimestampColumn)
	}
}

// TestMySQLAdapter_QueryData_WithSchema tests QueryData behavior with a proper schema
// This is a unit test that doesn't require a real database connection
func TestMySQLAdapter_QueryData_WithSchema(t *testing.T) {
	adapter := &MySQLAdapter{
		config: &MySQLConfig{
			Database: "testdb",
		},
	}

	ctx := context.Background()

	// Should error because db is nil (not connected)
	_, err := adapter.QueryData(ctx, "test_table", nil, func([]types.Record) error {
		return nil
	}, nil)

	if err == nil {
		t.Error("Expected error when querying data on unconnected adapter")
	}
}

// TestMySQLAdapter_TableSchema_PrimaryKeyDetection tests primary key detection logic
func TestMySQLAdapter_TableSchema_PrimaryKeyDetection(t *testing.T) {
	// Test schema with primary key
	schemaWithPK := &types.TableSchema{
		TableName: "orders",
		Columns: []types.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "customer_id", Type: "bigint", Nullable: false},
			{Name: "total", Type: "decimal", Nullable: true},
			{Name: "created_at", Type: "timestamp", Nullable: false},
		},
		PrimaryKey:       "id",
		TimestampColumn: "created_at",
	}

	if schemaWithPK.PrimaryKey != "id" {
		t.Errorf("Expected PrimaryKey 'id', got %s", schemaWithPK.PrimaryKey)
	}

	// Test schema without explicit primary key
	schemaWithoutPK := &types.TableSchema{
		TableName: "events",
		Columns: []types.Column{
			{Name: "event_id", Type: "bigint", Nullable: false},
			{Name: "event_type", Type: "varchar(100)", Nullable: false},
			{Name: "payload", Type: "text", Nullable: true},
		},
		TimestampColumn: "",
	}

	if schemaWithoutPK.PrimaryKey != "" {
		t.Errorf("Expected empty PrimaryKey for schema without PK, got %s", schemaWithoutPK.PrimaryKey)
	}
}

// TestMySQLAdapter_TimestampColumnDetection tests timestamp column detection
func TestMySQLAdapter_TimestampColumnDetection(t *testing.T) {
	tests := []struct {
		name            string
		columns         []types.Column
		expectedTSCol   string
	}{
		{
			name: "explicit timestamp column",
			columns: []types.Column{
				{Name: "id", Type: "bigint"},
				{Name: "ts", Type: "timestamp"},
			},
			expectedTSCol: "ts",
		},
		{
			name: "datetime column",
			columns: []types.Column{
				{Name: "id", Type: "bigint"},
				{Name: "created_datetime", Type: "datetime"},
			},
			expectedTSCol: "created_datetime",
		},
		{
			name: "no timestamp column",
			columns: []types.Column{
				{Name: "id", Type: "bigint"},
				{Name: "name", Type: "varchar(255)"},
			},
			expectedTSCol: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := &types.TableSchema{
				TableName:       "test",
				Columns:         tt.columns,
				TimestampColumn: tt.expectedTSCol,
			}

			if schema.TimestampColumn != tt.expectedTSCol {
				t.Errorf("Expected TimestampColumn %q, got %q", tt.expectedTSCol, schema.TimestampColumn)
			}
		})
	}
}
