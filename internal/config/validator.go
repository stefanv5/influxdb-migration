package config

import (
	"fmt"
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

func (v *ConfigValidator) Validate() error {
	v.validateGlobal()
	v.validateSources()
	v.validateTargets()
	v.validateTasks()
	v.validateMigrationSettings()
	v.validateRetrySettings()

	if len(v.errors) > 0 {
		return fmt.Errorf("config validation failed: %v", v.errors)
	}
	return nil
}

func (v *ConfigValidator) validateGlobal() {
	if v.config.Global.Name == "" {
		v.errors = append(v.errors, fmt.Errorf("global.name is required"))
	}
	if v.config.Global.CheckpointDir == "" {
		v.config.Global.CheckpointDir = "./checkpoints"
	}
	if v.config.Global.ReportDir == "" {
		v.config.Global.ReportDir = "./reports"
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

		if src.Host == "" {
			v.errors = append(v.errors, fmt.Errorf("source %s: host is required", src.Name))
		}
		if src.Port == 0 {
			v.errors = append(v.errors, fmt.Errorf("source %s: port is required", src.Name))
		}

		v.validateSourceSSL(src)
	}
}

func (v *ConfigValidator) validateSourceSSL(src types.SourceConfig) {
	if !src.SSL.Enabled {
		return
	}

	if src.Type == "influxdb" {
		if len(src.InfluxDB.URL) > 0 && src.InfluxDB.URL[:5] != "https" {
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
			// valid InfluxDB target types
		case "mysql":
			v.validateMySQLTarget(tgt)
		case "tdengine":
			v.validateTDengineTarget(tgt)
		default:
			v.errors = append(v.errors, fmt.Errorf(
				"target %s: type must be influxdb-v1, influxdb-v2, mysql, or tdengine", tgt.Name))
		}
	}
}

func (v *ConfigValidator) validateMySQLTarget(tgt types.TargetConfig) {
	if tgt.Host == "" {
		v.errors = append(v.errors, fmt.Errorf("target %s: mysql host is required", tgt.Name))
	}
	if tgt.Port == 0 {
		v.errors = append(v.errors, fmt.Errorf("target %s: mysql port is required", tgt.Name))
	}
	if tgt.Database == "" {
		v.errors = append(v.errors, fmt.Errorf("target %s: mysql database is required", tgt.Name))
	}
}

func (v *ConfigValidator) validateTDengineTarget(tgt types.TargetConfig) {
	if tgt.Host == "" {
		v.errors = append(v.errors, fmt.Errorf("target %s: tdengine host is required", tgt.Name))
	}
	if tgt.Port == 0 {
		v.errors = append(v.errors, fmt.Errorf("target %s: tdengine port is required", tgt.Name))
	}
	if tgt.Database == "" {
		v.errors = append(v.errors, fmt.Errorf("target %s: tdengine database is required", tgt.Name))
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
	if v.config.Migration.ChunkSize == 0 {
		v.config.Migration.ChunkSize = 10000
	}
	if v.config.Migration.ParallelTasks == 0 {
		v.config.Migration.ParallelTasks = 4
	}
	if v.config.Migration.ChunkInterval == 0 {
		v.config.Migration.ChunkInterval = 100 * time.Millisecond
	}
	if v.config.Migration.MaxSeriesParallel == 0 {
		v.config.Migration.MaxSeriesParallel = 2
	}
}

func (v *ConfigValidator) validateRetrySettings() {
	if v.config.Retry.MaxAttempts == 0 {
		v.config.Retry.MaxAttempts = 3
	}
	if v.config.Retry.InitialDelay == 0 {
		v.config.Retry.InitialDelay = 1 * time.Second
	}
	if v.config.Retry.MaxDelay == 0 {
		v.config.Retry.MaxDelay = 60 * time.Second
	}
	if v.config.Retry.BackoffMultiplier == 0 {
		v.config.Retry.BackoffMultiplier = 2.0
	}
}

func ApplyDefaults(cfg *types.MigrationConfig) error {
	v := NewValidator(cfg)
	v.validateGlobal()
	v.validateMigrationSettings()
	v.validateRetrySettings()
	return nil
}
