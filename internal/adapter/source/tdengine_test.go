package source

import (
	"context"
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

// TestDecodeTDConfig tests the decodeTDConfig function
func TestDecodeTDConfig(t *testing.T) {
	config := map[string]interface{}{
		"tdengine": map[string]interface{}{
			"host":     "localhost",
			"port":     6030,
			"user":     "root",
			"password": "taosdata",
			"database": "testdb",
			"version":  "3.x",
		},
	}

	cfg := &TDengineConfig{}
	err := decodeTDConfig(config, cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if cfg.Host != "localhost" {
		t.Errorf("Expected Host 'localhost', got %s", cfg.Host)
	}
	if cfg.Port != 6030 {
		t.Errorf("Expected Port 6030, got %d", cfg.Port)
	}
	if cfg.User != "root" {
		t.Errorf("Expected User 'root', got %s", cfg.User)
	}
	if cfg.Password != "taosdata" {
		t.Errorf("Expected Password 'taosdata', got %s", cfg.Password)
	}
	if cfg.Database != "testdb" {
		t.Errorf("Expected Database 'testdb', got %s", cfg.Database)
	}
	if cfg.Version != "3.x" {
		t.Errorf("Expected Version '3.x', got %s", cfg.Version)
	}
}

// TestDecodeTDConfig_SSL tests SSL configuration extraction
func TestDecodeTDConfig_SSL(t *testing.T) {
	config := map[string]interface{}{
		"tdengine": map[string]interface{}{
			"host":     "localhost",
			"port":     6030,
			"user":     "root",
			"password": "taosdata",
			"database": "testdb",
		},
		"ssl": map[string]interface{}{
			"enabled":     true,
			"skip_verify": true,
			"ca_cert":     "/path/to/ca.pem",
			"client_cert": "/path/to/cert.pem",
			"client_key":  "/path/to/key.pem",
		},
	}

	cfg := &TDengineConfig{}
	err := decodeTDConfig(config, cfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !cfg.SSL.Enabled {
		t.Error("Expected SSL.Enabled to be true")
	}
	if !cfg.SSL.SkipVerify {
		t.Error("Expected SSL.SkipVerify to be true")
	}
	if cfg.SSL.CaCert != "/path/to/ca.pem" {
		t.Errorf("Expected SSL.CaCert '/path/to/ca.pem', got %s", cfg.SSL.CaCert)
	}
	if cfg.SSL.ClientCert != "/path/to/cert.pem" {
		t.Errorf("Expected SSL.ClientCert '/path/to/cert.pem', got %s", cfg.SSL.ClientCert)
	}
	if cfg.SSL.ClientKey != "/path/to/key.pem" {
		t.Errorf("Expected SSL.ClientKey '/path/to/key.pem', got %s", cfg.SSL.ClientKey)
	}
}

// TestDecodeTDConfig_MissingConfig tests error handling for missing config
func TestDecodeTDConfig_MissingConfig(t *testing.T) {
	config := map[string]interface{}{}

	cfg := &TDengineConfig{}
	err := decodeTDConfig(config, cfg)
	if err == nil {
		t.Error("Expected error for missing tdengine config")
	}
}

// TestTDengineQuoteIdentifier tests the tdengineQuoteIdentifier helper
func TestTDengineQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"table", "`table`"},
		{"table`name", "`table``name`"},
		{"", "``"},
		{"column with space", "`column with space`"},
		{"special`char", "`special``char`"},
		{"db.table", "`db.table`"}, // Note: function quotes whole string, doesn't split on dots
	}

	for _, tt := range tests {
		result := tdengineQuoteIdentifier(tt.input)
		if result != tt.expected {
			t.Errorf("tdengineQuoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// TestTDengineAdapter_QueryDataBatch_NotConnected tests QueryDataBatch returns error when not connected
func TestTDengineAdapter_QueryDataBatch_NotConnected(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	_, err := adapter.QueryDataBatch(ctx, "test_table", nil, time.Now(), time.Now(), nil, func([]types.Record) error {
		return nil
	}, nil)
	if err == nil {
		t.Error("Expected error when calling QueryDataBatch on unconnected adapter")
	}
}

// TestTDengineAdapter_DiscoverSeries_NotConnected tests DiscoverSeries returns error when not connected
func TestTDengineAdapter_DiscoverSeries_NotConnected(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	_, err := adapter.DiscoverSeries(ctx, "test_table")
	if err == nil {
		t.Error("Expected error when calling DiscoverSeries on unconnected adapter")
	}
}

// TestTDengineAdapter_DiscoverSeriesInTimeWindow_NotConnected tests DiscoverSeriesInTimeWindow returns error when not connected
func TestTDengineAdapter_DiscoverSeriesInTimeWindow_NotConnected(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	_, err := adapter.DiscoverSeriesInTimeWindow(ctx, "test_table", time.Now(), time.Now())
	if err == nil {
		t.Error("Expected error when calling DiscoverSeriesInTimeWindow on unconnected adapter")
	}
}

// TestTDengineAdapter_DiscoverTagKeys_Stub tests DiscoverTagKeys returns nil (stub implementation)
// TDengine doesn't need tag/field distinction like InfluxDB
func TestTDengineAdapter_DiscoverTagKeys_Stub(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	keys, err := adapter.DiscoverTagKeys(ctx, "test_table")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if keys != nil {
		t.Errorf("Expected nil keys for TDengine adapter, got %v", keys)
	}
}

// TestTDengineAdapter_DiscoverShardGroups_NotConnected tests DiscoverShardGroups returns error when not connected
func TestTDengineAdapter_DiscoverShardGroups_NotConnected(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	_, err := adapter.DiscoverShardGroups(ctx)
	if err == nil {
		t.Error("Expected error when calling DiscoverShardGroups on unconnected adapter")
	}
}

// TestTDengineAdapter_Connect_ValidConfig tests Connect with valid config (no server)
func TestTDengineAdapter_Connect_ValidConfig(t *testing.T) {
	adapter := &TDengineAdapter{}
	ctx := context.Background()

	config := map[string]interface{}{
		"tdengine": map[string]interface{}{
			"host":     "localhost",
			"port":     6030,
			"user":     "root",
			"password": "taosdata",
			"database": "testdb",
			"version":  "3.x",
		},
	}

	// Connect should succeed even without a real server (lazy connection)
	err := adapter.Connect(ctx, config)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if adapter.config == nil {
		t.Fatal("Expected config to be set")
	}
	if adapter.baseURL == "" {
		t.Fatal("Expected baseURL to be set")
	}
}

// TestTDengineConfig_BasicFields tests that all basic config fields are properly set
func TestTDengineConfig_BasicFields(t *testing.T) {
	cfg := &TDengineConfig{
		Host:     "testhost",
		Port:     6043,
		User:     "testuser",
		Password: "testpass",
		Database: "testdb",
		Version:  "3.0",
	}

	if cfg.Host != "testhost" {
		t.Errorf("Expected Host 'testhost', got %s", cfg.Host)
	}
	if cfg.Port != 6043 {
		t.Errorf("Expected Port 6043, got %d", cfg.Port)
	}
	if cfg.User != "testuser" {
		t.Errorf("Expected User 'testuser', got %s", cfg.User)
	}
	if cfg.Password != "testpass" {
		t.Errorf("Expected Password 'testpass', got %s", cfg.Password)
	}
	if cfg.Database != "testdb" {
		t.Errorf("Expected Database 'testdb', got %s", cfg.Database)
	}
	if cfg.Version != "3.0" {
		t.Errorf("Expected Version '3.0', got %s", cfg.Version)
	}
}

// TestTDengineResponse tests the TDengineResponse struct
func TestTDengineResponse(t *testing.T) {
	resp := TDengineResponse{
		Code:    0,
		Message: "success",
		Data:    [][]any{{"val1", "val2"}, {"val3", "val4"}},
	}

	if resp.Code != 0 {
		t.Errorf("Expected Code 0, got %d", resp.Code)
	}
	if resp.Message != "success" {
		t.Errorf("Expected Message 'success', got %s", resp.Message)
	}
	if len(resp.Data) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(resp.Data))
	}
	if len(resp.Data[0]) != 2 {
		t.Errorf("Expected 2 columns in first row, got %d", len(resp.Data[0]))
	}
}

// TestTimestampRegex tests the timestampRegex pattern
func TestTimestampRegex(t *testing.T) {
	validTimestamps := []string{
		"2024-01-15 10:30:45.123",
		"2024-12-31 23:59:59.999",
		"2024-01-01 00:00:00.000",
	}

	for _, ts := range validTimestamps {
		if !timestampRegex.MatchString(ts) {
			t.Errorf("Expected timestamp %q to match regex", ts)
		}
	}

	invalidTimestamps := []string{
		"2024-1-15 10:30:45.123",    // missing leading zero
		"2024-01-15 10:30:45",        // missing milliseconds
		"01-15-2024 10:30:45.123",    // wrong format
		"2024/01/15 10:30:45.123",    // wrong separator
	}

	for _, ts := range invalidTimestamps {
		if timestampRegex.MatchString(ts) {
			t.Errorf("Expected timestamp %q NOT to match regex", ts)
		}
	}
}
