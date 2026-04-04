package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

type ConfigValidator struct {
	config *types.MigrationConfig
	errors []error
}

func NewValidator(cfg *types.MigrationConfig) *ConfigValidator {
	return &ConfigValidator{
		config: cfg,
		errors: make([]error, 0),
	}
}

func ApplyDefaults(cfg *types.MigrationConfig) {
	if cfg.Global.CheckpointDir == "" {
		cfg.Global.CheckpointDir = "./checkpoints"
	}
	if cfg.Global.ReportDir == "" {
		cfg.Global.ReportDir = "./reports"
	}
	if cfg.Migration.ChunkSize == 0 {
		cfg.Migration.ChunkSize = 10000
	}
	if cfg.Migration.ParallelTasks == 0 {
		cfg.Migration.ParallelTasks = 4
	}
	if cfg.Migration.ChunkInterval == 0 {
		cfg.Migration.ChunkInterval = 100 * time.Millisecond
	}
	if cfg.Migration.MaxSeriesParallel == 0 {
		cfg.Migration.MaxSeriesParallel = 2
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = 3
	}
	if cfg.Retry.InitialDelay == 0 {
		cfg.Retry.InitialDelay = 1 * time.Second
	}
	if cfg.Retry.MaxDelay == 0 {
		cfg.Retry.MaxDelay = 60 * time.Second
	}
	if cfg.Retry.BackoffMultiplier == 0 {
		cfg.Retry.BackoffMultiplier = 2.0
	}
}

func (v *ConfigValidator) Validate() error {
	v.validateGlobal()
	v.validateSources()
	v.validateTargets()
	v.validateTasks()
	v.validateMigrationSettings()
	v.validateRetrySettings()
	v.validateInfluxToInfluxSettings()

	if len(v.errors) > 0 {
		return fmt.Errorf("config validation failed: %v", v.errors)
	}
	return nil
}

func (v *ConfigValidator) validateGlobal() {
	if v.config.Global.Name == "" {
		v.errors = append(v.errors, fmt.Errorf("global.name is required"))
	}
}

func (v *ConfigValidator) validateSources() {
	if len(v.config.Sources) == 0 {
		v.errors = append(v.errors, fmt.Errorf("at least one source is required"))
		return
	}

	seen := make(map[string]bool)
	for _, src := range v.config.Sources {
		if seen[src.Name] {
			v.errors = append(v.errors, fmt.Errorf("duplicate source name: %s", src.Name))
		}
		seen[src.Name] = true

		switch src.Type {
		case "influxdb":
			if src.InfluxDB.URL == "" {
				v.errors = append(v.errors, fmt.Errorf("source %s: influxdb URL is required", src.Name))
			}
		case "mysql":
			if src.Host == "" {
				v.errors = append(v.errors, fmt.Errorf("source %s: mysql host is required", src.Name))
			}
			if src.Port == 0 {
				v.errors = append(v.errors, fmt.Errorf("source %s: mysql port is required", src.Name))
			}
		case "tdengine":
			if src.Host == "" {
				v.errors = append(v.errors, fmt.Errorf("source %s: tdengine host is required", src.Name))
			}
			if src.Port == 0 {
				v.errors = append(v.errors, fmt.Errorf("source %s: tdengine port is required", src.Name))
			}
		default:
			v.errors = append(v.errors, fmt.Errorf("source %s: type must be influxdb, mysql, or tdengine", src.Name))
		}

		v.validateSourceSSL(src)
	}
}

func (v *ConfigValidator) validateSourceSSL(src types.SourceConfig) {
	if !src.SSL.Enabled {
		return
	}

	if src.Type == "influxdb" {
		if len(src.InfluxDB.URL) >= 5 && !strings.HasPrefix(src.InfluxDB.URL, "https") {
			v.errors = append(v.errors, fmt.Errorf("source %s: influxdb URL must use https when SSL is enabled", src.Name))
		}
	}
}

func (v *ConfigValidator) validateTargets() {
	if len(v.config.Targets) == 0 {
		v.errors = append(v.errors, fmt.Errorf("at least one target is required"))
		return
	}

	seen := make(map[string]bool)
	for _, tgt := range v.config.Targets {
		if seen[tgt.Name] {
			v.errors = append(v.errors, fmt.Errorf("duplicate target name: %s", tgt.Name))
		}
		seen[tgt.Name] = true

		switch tgt.Type {
		case "influxdb-v1", "influxdb-v2":
			if tgt.InfluxDB.URL == "" {
				v.errors = append(v.errors, fmt.Errorf("target %s: influxdb URL is required", tgt.Name))
			}
		case "mysql":
			if tgt.Host == "" {
				v.errors = append(v.errors, fmt.Errorf("target %s: mysql host is required", tgt.Name))
			}
			if tgt.Port == 0 {
				v.errors = append(v.errors, fmt.Errorf("target %s: mysql port is required", tgt.Name))
			}
			if tgt.Database == "" {
				v.errors = append(v.errors, fmt.Errorf("target %s: mysql database is required", tgt.Name))
			}
		case "tdengine":
			if tgt.Host == "" {
				v.errors = append(v.errors, fmt.Errorf("target %s: tdengine host is required", tgt.Name))
			}
			if tgt.Port == 0 {
				v.errors = append(v.errors, fmt.Errorf("target %s: tdengine port is required", tgt.Name))
			}
			if tgt.Database == "" {
				v.errors = append(v.errors, fmt.Errorf("target %s: tdengine database is required", tgt.Name))
			}
		default:
			v.errors = append(v.errors, fmt.Errorf(
				"target %s: type must be influxdb-v1, influxdb-v2, mysql, or tdengine", tgt.Name))
		}
	}
}

func (v *ConfigValidator) validateTasks() {
	if len(v.config.Tasks) == 0 {
		v.errors = append(v.errors, fmt.Errorf("at least one task is required"))
		return
	}

	sourceNames := make(map[string]bool)
	for _, s := range v.config.Sources {
		sourceNames[s.Name] = true
	}
	targetNames := make(map[string]bool)
	for _, t := range v.config.Targets {
		targetNames[t.Name] = true
	}

	for _, task := range v.config.Tasks {
		if !sourceNames[task.Source] {
			v.errors = append(v.errors, fmt.Errorf("task %s: unknown source: %s", task.Name, task.Source))
		}
		if !targetNames[task.Target] {
			v.errors = append(v.errors, fmt.Errorf("task %s: unknown target: %s", task.Name, task.Target))
		}

		if len(task.Mappings) == 0 {
			v.errors = append(v.errors, fmt.Errorf("task %s: at least one mapping is required", task.Name))
		}

		for _, mapping := range task.Mappings {
			if mapping.SourceTable == "" && mapping.Measurement == "" {
				v.errors = append(v.errors, fmt.Errorf("task %s: source_table or measurement is required", task.Name))
			}
			if mapping.TargetMeasurement == "" {
				v.errors = append(v.errors, fmt.Errorf("task %s: target_measurement is required", task.Name))
			}
		}
	}
}

func (v *ConfigValidator) validateMigrationSettings() {
	if v.config.Migration.ChunkSize < 0 {
		v.errors = append(v.errors, fmt.Errorf("migration.chunk_size must be non-negative"))
	}
	if v.config.Migration.ParallelTasks < 0 {
		v.errors = append(v.errors, fmt.Errorf("migration.parallel_tasks must be non-negative"))
	}
	if v.config.Migration.ChunkInterval < 0 {
		v.errors = append(v.errors, fmt.Errorf("migration.chunk_interval must be non-negative"))
	}
	if v.config.Migration.MaxSeriesParallel < 0 {
		v.errors = append(v.errors, fmt.Errorf("migration.max_series_parallel must be non-negative"))
	}
}

func (v *ConfigValidator) validateRetrySettings() {
	if v.config.Retry.MaxAttempts < 0 {
		v.errors = append(v.errors, fmt.Errorf("retry.max_attempts must be non-negative"))
	}
	if v.config.Retry.InitialDelay < 0 {
		v.errors = append(v.errors, fmt.Errorf("retry.initial_delay must be non-negative"))
	}
	if v.config.Retry.MaxDelay < 0 {
		v.errors = append(v.errors, fmt.Errorf("retry.max_delay must be non-negative"))
	}
	if v.config.Retry.BackoffMultiplier < 0 {
		v.errors = append(v.errors, fmt.Errorf("retry.backoff_multiplier must be non-negative"))
	}
}

func (v *ConfigValidator) validateInfluxToInfluxSettings() {
	if !v.config.InfluxToInflux.Enabled {
		return
	}
	if v.config.InfluxToInflux.QueryMode != "" &&
		v.config.InfluxToInflux.QueryMode != "single" &&
		v.config.InfluxToInflux.QueryMode != "batch" {
		v.errors = append(v.errors, fmt.Errorf(
			"influx_to_influx.query_mode must be 'single' or 'batch', got '%s'",
			v.config.InfluxToInflux.QueryMode))
	}
	if v.config.InfluxToInflux.MaxSeriesPerQuery < 0 {
		v.errors = append(v.errors, fmt.Errorf(
			"influx_to_influx.max_series_per_query must be non-negative, got %d",
			v.config.InfluxToInflux.MaxSeriesPerQuery))
	}
	if v.config.InfluxToInflux.MaxSeriesPerQuery > 1000 {
		v.config.InfluxToInflux.MaxSeriesPerQuery = 1000
	}
}
