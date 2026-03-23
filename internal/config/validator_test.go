package config

import (
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

func TestNewValidator(t *testing.T) {
	cfg := &types.MigrationConfig{}
	validator := NewValidator(cfg)

	if validator == nil {
		t.Error("Expected non-nil validator")
	}

	if validator.errors == nil {
		t.Error("Expected errors slice to be initialized")
	}
}

func TestValidate_EmptyConfig(t *testing.T) {
	cfg := &types.MigrationConfig{}
	validator := NewValidator(cfg)

	err := validator.Validate()
	if err == nil {
		t.Error("Expected validation error for empty config")
	}
}

func TestValidate_GlobalNameRequired(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global: types.GlobalConfig{
			Name: "",
		},
		Sources: []types.SourceConfig{
			{Name: "src1", Host: "localhost", Port: 3306},
		},
		Targets: []types.TargetConfig{
			{Name: "tgt1", Type: "influxdb"},
		},
		Tasks: []types.TaskConfig{
			{
				Name:   "task1",
				Source: "src1",
				Target: "tgt1",
				Mappings: []types.MappingConfig{
					{SourceTable: "table1", TargetMeasurement: "m1"},
				},
			},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for empty global name")
	}
}

func TestValidate_AtLeastOneSource(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for no sources")
	}
}

func TestValidate_AtLeastOneTarget(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 3306}},
		Targets: []types.TargetConfig{},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for no targets")
	}
}

func TestValidate_AtLeastOneTask(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 3306}},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "influxdb"}},
		Tasks:   []types.TaskConfig{},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for no tasks")
	}
}

func TestValidate_DuplicateSourceName(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global: types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{
			{Name: "src1", Host: "localhost", Port: 3306},
			{Name: "src1", Host: "localhost", Port: 3307},
		},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "influxdb"}},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "src1", Target: "tgt1", Mappings: []types.MappingConfig{
				{SourceTable: "t1", TargetMeasurement: "m1"},
			}},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for duplicate source names")
	}
}

func TestValidate_DuplicateTargetName(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 3306}},
		Targets: []types.TargetConfig{
			{Name: "tgt1", Type: "influxdb"},
			{Name: "tgt1", Type: "influxdb"},
		},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "src1", Target: "tgt1", Mappings: []types.MappingConfig{
				{SourceTable: "t1", TargetMeasurement: "m1"},
			}},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for duplicate target names")
	}
}

func TestValidate_SourceHostRequired(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "", Port: 3306}},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "influxdb"}},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "src1", Target: "tgt1", Mappings: []types.MappingConfig{
				{SourceTable: "t1", TargetMeasurement: "m1"},
			}},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for empty source host")
	}
}

func TestValidate_SourcePortRequired(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 0}},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "influxdb"}},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "src1", Target: "tgt1", Mappings: []types.MappingConfig{
				{SourceTable: "t1", TargetMeasurement: "m1"},
			}},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for zero source port")
	}
}

func TestValidate_UnknownSource(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 3306}},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "influxdb"}},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "unknown", Target: "tgt1", Mappings: []types.MappingConfig{
				{SourceTable: "t1", TargetMeasurement: "m1"},
			}},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for unknown source")
	}
}

func TestValidate_UnknownTarget(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 3306}},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "influxdb"}},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "src1", Target: "unknown", Mappings: []types.MappingConfig{
				{SourceTable: "t1", TargetMeasurement: "m1"},
			}},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for unknown target")
	}
}

func TestValidate_TargetMustBeInfluxDB(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 3306}},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "mysql"}},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "src1", Target: "tgt1", Mappings: []types.MappingConfig{
				{SourceTable: "t1", TargetMeasurement: "m1"},
			}},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for non-influxdb target")
	}
}

func TestValidate_MappingRequiresSourceOrMeasurement(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 3306}},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "influxdb"}},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "src1", Target: "tgt1", Mappings: []types.MappingConfig{
				{SourceTable: "", Measurement: "", TargetMeasurement: "m1"},
			}},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for mapping without source or measurement")
	}
}

func TestValidate_MappingRequiresTargetMeasurement(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 3306}},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "influxdb"}},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "src1", Target: "tgt1", Mappings: []types.MappingConfig{
				{SourceTable: "t1", TargetMeasurement: ""},
			}},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err == nil {
		t.Error("Expected error for mapping without target measurement")
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 3306}},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "influxdb"}},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "src1", Target: "tgt1", Mappings: []types.MappingConfig{
				{SourceTable: "t1", TargetMeasurement: "m1"},
			}},
		},
	}

	validator := NewValidator(cfg)
	err := validator.Validate()

	if err != nil {
		t.Errorf("Unexpected error for valid config: %v", err)
	}
}

func TestValidate_DefaultValues(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global:  types.GlobalConfig{Name: "test"},
		Sources: []types.SourceConfig{{Name: "src1", Host: "localhost", Port: 3306}},
		Targets: []types.TargetConfig{{Name: "tgt1", Type: "influxdb"}},
		Tasks: []types.TaskConfig{
			{Name: "task1", Source: "src1", Target: "tgt1", Mappings: []types.MappingConfig{
				{SourceTable: "t1", TargetMeasurement: "m1"},
			}},
		},
	}

	validator := NewValidator(cfg)
	validator.Validate()

	if cfg.Global.CheckpointDir != "./checkpoints" {
		t.Errorf("Expected default checkpoint dir ./checkpoints, got %s", cfg.Global.CheckpointDir)
	}

	if cfg.Global.ReportDir != "./reports" {
		t.Errorf("Expected default report dir ./reports, got %s", cfg.Global.ReportDir)
	}

	if cfg.Migration.ChunkSize != 10000 {
		t.Errorf("Expected default chunk size 10000, got %d", cfg.Migration.ChunkSize)
	}

	if cfg.Migration.ParallelTasks != 4 {
		t.Errorf("Expected default parallel tasks 4, got %d", cfg.Migration.ParallelTasks)
	}

	if cfg.Migration.ChunkInterval != 100*time.Millisecond {
		t.Errorf("Expected default chunk interval 100ms, got %v", cfg.Migration.ChunkInterval)
	}

	if cfg.Retry.MaxAttempts != 3 {
		t.Errorf("Expected default max attempts 3, got %d", cfg.Retry.MaxAttempts)
	}
}

func TestApplyDefaults(t *testing.T) {
	cfg := &types.MigrationConfig{
		Global: types.GlobalConfig{Name: "test"},
	}

	err := ApplyDefaults(cfg)
	if err != nil {
		t.Errorf("ApplyDefaults failed: %v", err)
	}

	if cfg.Global.CheckpointDir != "./checkpoints" {
		t.Errorf("Expected default checkpoint dir, got %s", cfg.Global.CheckpointDir)
	}
}
