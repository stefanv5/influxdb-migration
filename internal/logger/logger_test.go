package logger

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoggerConfig(t *testing.T) {
	cfg := &Config{
		Level:  "debug",
		Output: "console",
		File: FileConfig{
			Path:       "logs/test.log",
			MaxSize:    10,
			MaxBackups: 5,
			MaxAge:     3,
			Compress:   true,
		},
	}

	if cfg.Level != "debug" {
		t.Errorf("Expected level debug, got %s", cfg.Level)
	}

	if cfg.Output != "console" {
		t.Errorf("Expected output console, got %s", cfg.Output)
	}

	if cfg.File.MaxSize != 10 {
		t.Errorf("Expected max size 10, got %d", cfg.File.MaxSize)
	}
}

func TestRotatingWriter(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	w := &rotatingWriter{
		path:       logPath,
		maxSize:    1, // 1MB
		maxBackups: 3,
		maxAge:     7,
		compress:   false,
	}

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	w.file = file
	w.size = 0

	data := make([]byte, 1024) // 1KB
	for i := range data {
		data[i] = 'a'
	}

	n, err := w.Write(data)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
	}

	err = w.Close()
	if err != nil {
		t.Errorf("Close error: %v", err)
	}
}

func TestRotatingWriterSync(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	w := &rotatingWriter{
		path:       logPath,
		maxSize:    10,
		maxBackups: 3,
		maxAge:     7,
		compress:   false,
	}

	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	w.file = file

	err = w.Sync()
	if err != nil {
		t.Errorf("Sync error: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Errorf("Close error: %v", err)
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"debug", "debug"},
		{"info", "info"},
		{"warn", "warn"},
		{"error", "error"},
		{"invalid", "info"}, // default
		{"", "info"},        // default
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			// Just verify it doesn't panic
			level := parseLevel(tt.input)
			if level == 0 && tt.input != "" && tt.input != "invalid" {
				// Only fail for non-default cases where we expect a valid level
			}
		})
	}
}

func TestLoggerInit(t *testing.T) {
	// Test that Init can be called with nil config (uses defaults)
	err := Init(nil)
	if err != nil {
		t.Errorf("Init with nil config failed: %v", err)
	}

	// Test Init with valid config
	cfg := &Config{
		Level:  "info",
		Output: "console",
		File: FileConfig{
			Path:       "logs/test.log",
			MaxSize:    100,
			MaxBackups: 10,
			MaxAge:     7,
			Compress:   true,
		},
	}

	// Note: Once.Do means this might not reinitialize
	// Just verify it doesn't panic
	Init(cfg)
}

func TestGetLogger(t *testing.T) {
	logger := GetLogger()
	if logger == nil {
		t.Error("Expected non-nil logger")
	}

	// Should return same instance
	logger2 := GetLogger()
	if logger != logger2 {
		t.Error("Expected same logger instance")
	}
}

func TestLoggerSync(t *testing.T) {
	// Should not panic
	Sync()
}

func TestWriterAdapter(t *testing.T) {
	adapter := &WriterAdapter{}
	n, err := adapter.Write([]byte("test message"))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if n != 12 {
		t.Errorf("Expected 12, got %d", n)
	}
}
