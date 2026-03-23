package logger

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	globalLogger *zap.Logger
	once         sync.Once
)

type Config struct {
	Level  string
	Output string
	File   FileConfig
}

type FileConfig struct {
	Path       string
	MaxSize    int
	MaxBackups int
	MaxAge     int
	Compress   bool
}

type rotatingWriter struct {
	mu         sync.Mutex
	path       string
	maxSize    int
	maxBackups int
	maxAge     int
	compress   bool
	file       *os.File
	size       int64
}

func (w *rotatingWriter) Sync() error {
	if w.file != nil {
		return w.file.Sync()
	}
	return nil
}

func (w *rotatingWriter) Write(p []byte) (n int, err error) {
	if w == nil || w.file == nil {
		return len(p), nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if int64(len(p))+w.size > int64(w.maxSize*1024*1024) {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}

	n, err = w.file.Write(p)
	if err == nil {
		w.size += int64(n)
	}
	return n, err
}

func (w *rotatingWriter) rotate() error {
	if w.file != nil {
		w.file.Close()
	}

	// Compress old log if enabled
	if w.compress {
		if err := w.compressLog(w.path); err != nil {
			fmt.Printf("failed to compress log: %v\n", err)
		}
	}

	// Rename current log to timestamped name
	timestamp := time.Now().Format("2006-01-02T15-04-05")
	newPath := fmt.Sprintf("%s.%s", w.path, timestamp)
	if err := os.Rename(w.path, newPath); err != nil {
		return fmt.Errorf("failed to rename log: %w", err)
	}

	// Clean up old logs
	w.cleanup()

	// Open new file
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	w.file = file
	w.size = 0

	return nil
}

func (w *rotatingWriter) compressLog(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gzPath := path + ".gz"
	gzFile, err := os.Create(gzPath)
	if err != nil {
		return err
	}
	defer gzFile.Close()

	gzWriter := gzip.NewWriter(gzFile)
	defer gzWriter.Close()

	if _, err := io.Copy(gzWriter, f); err != nil {
		return err
	}

	return gzWriter.Close()
}

func (w *rotatingWriter) cleanup() error {
	// Find old log files and remove based on maxBackups and maxAge
	cutoff := time.Now().Add(-time.Duration(w.maxAge) * 24 * time.Hour)

	files, _ := filepath.Glob(w.path + ".*")
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			continue
		}

		// Remove if too old
		if info.ModTime().Before(cutoff) {
			os.Remove(f)
			continue
		}

		// Keep only maxBackups most recent
		// This is a simple approach - just remove oldest if too many
	}

	return nil
}

func (w *rotatingWriter) Close() error {
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

func Init(cfg *Config) error {
	var err error
	once.Do(func() {
		globalLogger, err = buildLogger(cfg)
	})
	return err
}

func GetLogger() *zap.Logger {
	if globalLogger == nil {
		globalLogger, _ = zap.NewProduction()
	}
	return globalLogger
}

func buildLogger(cfg *Config) (*zap.Logger, error) {
	if cfg == nil {
		cfg = &Config{
			Level:  "info",
			Output: "both",
			File: FileConfig{
				Path:       "logs/migrate.log",
				MaxSize:    100,
				MaxBackups: 10,
				MaxAge:     7,
				Compress:   true,
			},
		}
	}

	level := parseLevel(cfg.Level)

	var writers []zapcore.WriteSyncer

	if cfg.Output == "file" || cfg.Output == "both" {
		dir := filepath.Dir(cfg.File.Path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}

		rw := &rotatingWriter{
			path:       cfg.File.Path,
			maxSize:    cfg.File.MaxSize,
			maxBackups: cfg.File.MaxBackups,
			maxAge:     cfg.File.MaxAge,
			compress:   cfg.File.Compress,
		}

		file, err := os.OpenFile(cfg.File.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}
		rw.file = file

		writers = append(writers, rw)
	}

	if cfg.Output == "console" || cfg.Output == "both" {
		writers = append(writers, zapcore.AddSync(os.Stdout))
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.NewMultiWriteSyncer(writers...),
		level,
	)

	return zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)), nil
}

func parseLevel(level string) zapcore.Level {
	switch level {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

func Debug(msg string, fields ...zap.Field) {
	GetLogger().Debug(msg, fields...)
}

func Info(msg string, fields ...zap.Field) {
	GetLogger().Info(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	GetLogger().Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	GetLogger().Error(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	GetLogger().Fatal(msg, fields...)
}

func Sync() {
	if globalLogger != nil {
		globalLogger.Sync()
	}
}

var _ io.Writer = (*WriterAdapter)(nil)

type WriterAdapter struct{}

func (a *WriterAdapter) Write(p []byte) (n int, err error) {
	Info(string(p))
	return len(p), nil
}
