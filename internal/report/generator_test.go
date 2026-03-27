package report

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/pkg/types"
)

func TestNewGenerator(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := checkpoint.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	gen := NewGenerator(mgr, tmpDir)
	if gen == nil {
		t.Error("Expected non-nil generator")
	}

	if gen.checkpointMgr == nil {
		t.Error("Expected checkpointMgr to be set")
	}

	if gen.reportDir != tmpDir {
		t.Errorf("Expected reportDir %s, got %s", tmpDir, gen.reportDir)
	}
}

func TestGenerator_Generate(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := checkpoint.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	// Create some checkpoints
	cp := &types.Checkpoint{
		TaskID:        "test-task",
		TaskName:      "Test Task",
		SourceTable:   "source_table",
		TargetMeas:    "target_meas",
		ProcessedRows: 1000,
		Status:        types.StatusCompleted,
		CreatedAt:     time.Now().Add(-1 * time.Hour),
		UpdatedAt:     time.Now(),
	}
	mgr.CreateCheckpoint(cp)

	gen := NewGenerator(mgr, tmpDir)
	ctx := context.Background()

	report, err := gen.Generate(ctx, "test-task", "Test Task")
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if report == nil {
		t.Fatal("Expected non-nil report")
	}

	if report.MigrationID != "test-task" {
		t.Errorf("Expected MigrationID 'test-task', got %s", report.MigrationID)
	}

	if report.TaskName != "Test Task" {
		t.Errorf("Expected TaskName 'Test Task', got %s", report.TaskName)
	}

	if report.Status != "completed" {
		t.Errorf("Expected status 'completed', got %s", report.Status)
	}

	if report.Summary.TotalRows != 1000 {
		t.Errorf("Expected TotalRows 1000, got %d", report.Summary.TotalRows)
	}
}

func TestGenerator_Generate_WithFailedTask(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := checkpoint.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	// Create completed checkpoint
	cp1 := &types.Checkpoint{
		TaskID:        "task1",
		TaskName:      "Task 1",
		SourceTable:   "table1",
		TargetMeas:    "meas1",
		ProcessedRows: 500,
		Status:        types.StatusCompleted,
		CreatedAt:     time.Now().Add(-1 * time.Hour),
		UpdatedAt:     time.Now(),
	}
	mgr.CreateCheckpoint(cp1)

	// Create failed checkpoint
	cp2 := &types.Checkpoint{
		TaskID:        "task2",
		TaskName:      "Task 2",
		SourceTable:   "table2",
		TargetMeas:    "meas2",
		ProcessedRows: 200,
		Status:        types.StatusFailed,
		CreatedAt:     time.Now().Add(-1 * time.Hour),
		UpdatedAt:     time.Now(),
	}
	mgr.CreateCheckpoint(cp2)

	gen := NewGenerator(mgr, tmpDir)
	ctx := context.Background()

	report, err := gen.Generate(ctx, "task1", "Test Migration")
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if report.Status != "failed" {
		t.Errorf("Expected status 'failed', got %s", report.Status)
	}

	if report.Summary.TotalRows != 700 {
		t.Errorf("Expected TotalRows 700, got %d", report.Summary.TotalRows)
	}

	if report.Summary.TransferredRows != 500 {
		t.Errorf("Expected TransferredRows 500, got %d", report.Summary.TransferredRows)
	}

	if report.Summary.FailedRows != 200 {
		t.Errorf("Expected FailedRows 200, got %d", report.Summary.FailedRows)
	}
}

func TestGenerator_SaveJSON(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := checkpoint.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	gen := NewGenerator(mgr, tmpDir)

	report := &Report{
		MigrationID:  "test-migration",
		TaskName:     "Test Task",
		Status:       "completed",
		StartedAt:    time.Now().Add(-1 * time.Hour),
		CompletedAt:  time.Now(),
		DurationSecs: 3600,
		Summary: Summary{
			TotalRows:       1000,
			TransferredRows: 900,
			FailedRows:      100,
			BatchCount:      10,
		},
	}

	err = gen.SaveJSON(report)
	if err != nil {
		t.Fatalf("SaveJSON failed: %v", err)
	}

	// Check that file was created
	files, _ := filepath.Glob(filepath.Join(tmpDir, "test-migration_*.json"))
	if len(files) != 1 {
		t.Errorf("Expected 1 JSON file, got %d", len(files))
	}
}

func TestGenerator_SaveMarkdown(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := checkpoint.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	gen := NewGenerator(mgr, tmpDir)

	report := &Report{
		MigrationID:  "test-migration",
		TaskName:     "Test Task",
		Status:       "completed",
		StartedAt:    time.Now().Add(-1 * time.Hour),
		CompletedAt:  time.Now(),
		DurationSecs: 3600,
		Summary: Summary{
			TotalRows:       1000,
			TransferredRows: 900,
			FailedRows:      100,
			BatchCount:      10,
		},
	}

	err = gen.SaveMarkdown(report)
	if err != nil {
		t.Fatalf("SaveMarkdown failed: %v", err)
	}

	// Check that file was created
	files, _ := filepath.Glob(filepath.Join(tmpDir, "test-migration_*.md"))
	if len(files) != 1 {
		t.Errorf("Expected 1 Markdown file, got %d", len(files))
	}
}

func TestGenerator_SaveHTML(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := checkpoint.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	gen := NewGenerator(mgr, tmpDir)

	report := &Report{
		MigrationID:  "test-migration",
		TaskName:     "Test Task",
		Status:       "completed",
		StartedAt:    time.Now().Add(-1 * time.Hour),
		CompletedAt:  time.Now(),
		DurationSecs: 3600,
		Summary: Summary{
			TotalRows:       1000,
			TransferredRows: 900,
			FailedRows:      100,
			BatchCount:      10,
		},
	}

	err = gen.SaveHTML(report)
	if err != nil {
		t.Fatalf("SaveHTML failed: %v", err)
	}

	// Check that file was created
	files, _ := filepath.Glob(filepath.Join(tmpDir, "test-migration_*.html"))
	if len(files) != 1 {
		t.Errorf("Expected 1 HTML file, got %d", len(files))
	}
}

func TestGenerator_SaveJSON_EmptyDir(t *testing.T) {
	mgr, err := checkpoint.NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	gen := NewGenerator(mgr, "")

	report := &Report{
		MigrationID: "test",
		TaskName:    "Test",
	}

	err = gen.SaveJSON(report)
	if err == nil {
		t.Error("Expected error for empty report dir")
	}
}

func TestGenerator_SaveMarkdown_EmptyDir(t *testing.T) {
	mgr, err := checkpoint.NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	gen := NewGenerator(mgr, "")

	report := &Report{
		MigrationID: "test",
		TaskName:    "Test",
	}

	err = gen.SaveMarkdown(report)
	if err == nil {
		t.Error("Expected error for empty report dir")
	}
}

func TestGenerator_SaveHTML_EmptyDir(t *testing.T) {
	mgr, err := checkpoint.NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	gen := NewGenerator(mgr, "")

	report := &Report{
		MigrationID: "test",
		TaskName:    "Test",
	}

	err = gen.SaveHTML(report)
	if err == nil {
		t.Error("Expected error for empty report dir")
	}
}

func TestGenerator_Generate_EmptyCheckpoints(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := checkpoint.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	gen := NewGenerator(mgr, tmpDir)
	ctx := context.Background()

	report, err := gen.Generate(ctx, "nonexistent-task", "Test")
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	if report.Status != "completed" {
		t.Errorf("Expected status 'completed', got %s", report.Status)
	}

	if report.Summary.TotalRows != 0 {
		t.Errorf("Expected TotalRows 0, got %d", report.Summary.TotalRows)
	}
}

func TestFormatCheckpointsTable(t *testing.T) {
	checkpoints := []CheckpointEntry{
		{
			Table:         "table1",
			LastID:        100,
			LastTimestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			ProcessedRows: 500,
		},
	}

	gen := &Generator{}
	result := gen.formatCheckpointsTable(checkpoints)

	if result == "" {
		t.Error("Expected non-empty result")
	}

	// Check that it contains expected values
	expected := "| table1 |"
	if !contains(result, expected) {
		t.Errorf("Expected to contain %q", expected)
	}
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

func TestNewGenerator_CreateDir(t *testing.T) {
	tmpDir := t.TempDir()
	reportDir := filepath.Join(tmpDir, "reports")

	mgr, err := checkpoint.NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	// This should create the directory
	gen := NewGenerator(mgr, reportDir)
	if gen == nil {
		t.Error("Expected non-nil generator")
	}

	if _, err := os.Stat(reportDir); os.IsNotExist(err) {
		t.Error("Expected report dir to be created")
	}
}
