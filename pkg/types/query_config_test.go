package types

import (
	"testing"
	"time"
)

func TestQueryConfig_Defaults(t *testing.T) {
	cfg := &QueryConfig{}

	if cfg.BatchSize != 0 {
		t.Errorf("Expected BatchSize 0, got %d", cfg.BatchSize)
	}

	if cfg.TimeWindow != 0 {
		t.Errorf("Expected TimeWindow 0, got %v", cfg.TimeWindow)
	}
}

func TestQueryConfig_WithBatchSize(t *testing.T) {
	cfg := &QueryConfig{
		BatchSize: 5000,
	}

	if cfg.BatchSize != 5000 {
		t.Errorf("Expected BatchSize 5000, got %d", cfg.BatchSize)
	}
}

func TestQueryConfig_WithTimeWindow(t *testing.T) {
	cfg := &QueryConfig{
		TimeWindow: 24 * time.Hour,
	}

	expected := 24 * time.Hour
	if cfg.TimeWindow != expected {
		t.Errorf("Expected TimeWindow %v, got %v", expected, cfg.TimeWindow)
	}
}

func TestQueryConfig_WithBothFields(t *testing.T) {
	cfg := &QueryConfig{
		BatchSize:  10000,
		TimeWindow: 168 * time.Hour,
	}

	if cfg.BatchSize != 10000 {
		t.Errorf("Expected BatchSize 10000, got %d", cfg.BatchSize)
	}

	expected := 168 * time.Hour
	if cfg.TimeWindow != expected {
		t.Errorf("Expected TimeWindow %v, got %v", expected, cfg.TimeWindow)
	}
}

func TestQueryConfig_EdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		batchSize  int
		timeWindow time.Duration
	}{
		{"Zero values", 0, 0},
		{"Large batch size", 1000000, 0},
		{"Small batch size", 1, 0},
		{"Negative batch size", -1, 0},
		{"Very small time window", 1, time.Nanosecond},
		{"Large time window", 0, 8760 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &QueryConfig{
				BatchSize:  tt.batchSize,
				TimeWindow: tt.timeWindow,
			}

			if cfg.BatchSize != tt.batchSize {
				t.Errorf("Expected BatchSize %d, got %d", tt.batchSize, cfg.BatchSize)
			}

			if cfg.TimeWindow != tt.timeWindow {
				t.Errorf("Expected TimeWindow %v, got %v", tt.timeWindow, cfg.TimeWindow)
			}
		})
	}
}

func TestQueryConfig_TimeWindowConversions(t *testing.T) {
	cfg := &QueryConfig{
		BatchSize:  10000,
		TimeWindow: 7 * 24 * time.Hour,
	}

	hours := cfg.TimeWindow.Hours()
	if hours != 168 {
		t.Errorf("Expected 168 hours, got %v", hours)
	}

	days := cfg.TimeWindow.Hours() / 24
	if days != 7 {
		t.Errorf("Expected 7 days, got %v days", days)
	}
}

func TestQueryConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		batchSize   int
		timeWindow  time.Duration
		expectError bool
	}{
		{"Valid config", 10000, 168 * time.Hour, false},
		{"Zero values (use ApplyDefaults first)", 0, 0, true},
		{"Min batch size", 1, 168 * time.Hour, false},
		{"Max batch size", 1000000, 168 * time.Hour, false},
		{"Min time window", 10000, 1 * time.Hour, false},
		{"Max time window", 10000, 720 * time.Hour, false},
		{"Below min batch size", 0, 168 * time.Hour, true},
		{"Above max batch size", 1000001, 168 * time.Hour, true},
		{"Below min time window", 10000, 30 * time.Minute, true},
		{"Above max time window", 10000, 31 * 24 * time.Hour, true},
		{"Negative batch size", -1, 168 * time.Hour, true},
		{"Negative time window", 10000, -1 * time.Hour, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &QueryConfig{
				BatchSize:  tt.batchSize,
				TimeWindow: tt.timeWindow,
			}

			err := cfg.Validate()
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestQueryConfig_ApplyDefaults(t *testing.T) {
	tests := []struct {
		name               string
		inputBatchSize     int
		inputTimeWindow    time.Duration
		expectedBatchSize  int
		expectedTimeWindow time.Duration
	}{
		{"Zero values", 0, 0, DefaultBatchSize, DefaultTimeWindow},
		{"Non-zero values unchanged", 5000, 24 * time.Hour, 5000, 24 * time.Hour},
		{"BatchSize zero only", 0, 24 * time.Hour, DefaultBatchSize, 24 * time.Hour},
		{"TimeWindow zero only", 5000, 0, 5000, DefaultTimeWindow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &QueryConfig{
				BatchSize:  tt.inputBatchSize,
				TimeWindow: tt.inputTimeWindow,
			}

			cfg.ApplyDefaults()

			if cfg.BatchSize != tt.expectedBatchSize {
				t.Errorf("Expected BatchSize %d, got %d", tt.expectedBatchSize, cfg.BatchSize)
			}

			if cfg.TimeWindow != tt.expectedTimeWindow {
				t.Errorf("Expected TimeWindow %v, got %v", tt.expectedTimeWindow, cfg.TimeWindow)
			}
		})
	}
}

func TestQueryConfig_ApplyDefaultsThenValidate(t *testing.T) {
	cfg := &QueryConfig{BatchSize: 0, TimeWindow: 0}
	cfg.ApplyDefaults()
	err := cfg.Validate()
	if err != nil {
		t.Errorf("Expected valid config after ApplyDefaults, got error: %v", err)
	}

	cfg2 := &QueryConfig{BatchSize: 5000, TimeWindow: 24 * time.Hour}
	cfg2.ApplyDefaults()
	err = cfg2.Validate()
	if err != nil {
		t.Errorf("Expected valid config with non-zero values, got error: %v", err)
	}
}
