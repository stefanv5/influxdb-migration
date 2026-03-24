package checkpoint

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

func TestNewManager(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	if mgr.store == nil {
		t.Error("Expected non-nil store")
	}
}

func TestNewManager_CreateDir(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "subdir", "checkpoints")
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Error("Expected checkpoint dir to be created")
	}
}

func TestManager_CreateCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	cp := &types.Checkpoint{
		TaskID:      "task1",
		TaskName:    "Test Task",
		SourceTable: "source_table1",
		TargetMeas:  "target_meas1",
		Status:      types.StatusPending,
	}

	err = mgr.CreateCheckpoint(cp)
	if err != nil {
		t.Errorf("CreateCheckpoint failed: %v", err)
	}
}

func TestManager_SaveAndLoadCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	cp := &types.Checkpoint{
		TaskID:        "task1",
		TaskName:      "Test Task",
		SourceTable:   "source_table1",
		TargetMeas:    "target_meas1",
		LastID:        100,
		LastTimestamp: time.Now().UnixNano(),
		ProcessedRows: 500,
		Status:        types.StatusInProgress,
	}

	err = mgr.CreateCheckpoint(cp)
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	ctx := context.Background()
	loaded, err := mgr.LoadCheckpoint(ctx, "task1", "source_table1")
	if err != nil {
		t.Fatalf("LoadCheckpoint failed: %v", err)
	}

	if loaded == nil {
		t.Fatal("Expected non-nil checkpoint")
	}

	if loaded.LastID != 100 {
		t.Errorf("Expected LastID 100, got %d", loaded.LastID)
	}

	if loaded.ProcessedRows != 500 {
		t.Errorf("Expected ProcessedRows 500, got %d", loaded.ProcessedRows)
	}

	if loaded.Status != types.StatusInProgress {
		t.Errorf("Expected StatusInProgress, got %s", loaded.Status)
	}
}

func TestManager_LoadNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()
	loaded, err := mgr.LoadCheckpoint(ctx, "nonexistent", "table")
	if err != nil {
		t.Fatalf("LoadCheckpoint failed: %v", err)
	}

	if loaded != nil {
		t.Error("Expected nil for non-existent checkpoint")
	}
}

func TestManager_MarkTaskCompleted(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	cp := &types.Checkpoint{
		TaskID:      "task1",
		TaskName:    "Test Task",
		SourceTable: "source_table1",
		TargetMeas:  "target_meas1",
		Status:      types.StatusPending,
	}

	err = mgr.CreateCheckpoint(cp)
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	ctx := context.Background()
	err = mgr.MarkTaskCompleted(ctx, "task1", "source_table1")
	if err != nil {
		t.Errorf("MarkTaskCompleted failed: %v", err)
	}

	loaded, err := mgr.LoadCheckpoint(ctx, "task1", "source_table1")
	if err != nil {
		t.Fatalf("LoadCheckpoint failed: %v", err)
	}

	if loaded.Status != types.StatusCompleted {
		t.Errorf("Expected StatusCompleted, got %s", loaded.Status)
	}
}

func TestManager_MarkTaskFailed(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	cp := &types.Checkpoint{
		TaskID:      "task1",
		TaskName:    "Test Task",
		SourceTable: "source_table1",
		TargetMeas:  "target_meas1",
		Status:      types.StatusPending,
	}

	err = mgr.CreateCheckpoint(cp)
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	ctx := context.Background()
	err = mgr.MarkTaskFailed(ctx, "task1", "source_table1", "test error message")
	if err != nil {
		t.Errorf("MarkTaskFailed failed: %v", err)
	}

	loaded, err := mgr.LoadCheckpoint(ctx, "task1", "source_table1")
	if err != nil {
		t.Fatalf("LoadCheckpoint failed: %v", err)
	}

	if loaded.Status != types.StatusFailed {
		t.Errorf("Expected StatusFailed, got %s", loaded.Status)
	}

	if loaded.ErrorMessage != "test error message" {
		t.Errorf("Expected error message 'test error message', got %s", loaded.ErrorMessage)
	}
}

func TestManager_MarkTaskInProgress(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	cp := &types.Checkpoint{
		TaskID:      "task1",
		TaskName:    "Test Task",
		SourceTable: "source_table1",
		TargetMeas:  "target_meas1",
		Status:      types.StatusPending,
	}

	err = mgr.CreateCheckpoint(cp)
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}

	ctx := context.Background()
	err = mgr.MarkTaskInProgress(ctx, "task1", "source_table1")
	if err != nil {
		t.Errorf("MarkTaskInProgress failed: %v", err)
	}

	loaded, err := mgr.LoadCheckpoint(ctx, "task1", "source_table1")
	if err != nil {
		t.Fatalf("LoadCheckpoint failed: %v", err)
	}

	if loaded.Status != types.StatusInProgress {
		t.Errorf("Expected StatusInProgress, got %s", loaded.Status)
	}
}

func TestManager_ListCheckpoints(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	for i := 1; i <= 3; i++ {
		cp := &types.Checkpoint{
			TaskID:      "task1",
			TaskName:    "Test Task",
			SourceTable: "source_table" + string(rune('0'+i)),
			TargetMeas:  "target_meas",
			Status:      types.StatusPending,
		}
		err = mgr.CreateCheckpoint(cp)
		if err != nil {
			t.Fatalf("CreateCheckpoint failed: %v", err)
		}
	}

	ctx := context.Background()
	checkpoints, err := mgr.ListCheckpoints(ctx, "task1")
	if err != nil {
		t.Fatalf("ListCheckpoints failed: %v", err)
	}

	if len(checkpoints) != 3 {
		t.Errorf("Expected 3 checkpoints, got %d", len(checkpoints))
	}
}

func TestManager_GetPendingTasks(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	// Create tasks with different statuses
	tasks := []struct {
		taskID      string
		sourceTable string
		status      types.CheckpointStatus
	}{
		{"task1", "table1", types.StatusPending},
		{"task2", "table2", types.StatusPending},
		{"task3", "table3", types.StatusInProgress},
		{"task4", "table4", types.StatusCompleted},
		{"task5", "table5", types.StatusFailed},
	}

	for _, task := range tasks {
		cp := &types.Checkpoint{
			TaskID:      task.taskID,
			TaskName:    "Test",
			SourceTable: task.sourceTable,
			TargetMeas:  "target",
			Status:      task.status,
		}
		mgr.CreateCheckpoint(cp)
	}

	ctx := context.Background()
	pending, err := mgr.GetPendingTasks(ctx)
	if err != nil {
		t.Fatalf("GetPendingTasks failed: %v", err)
	}

	if len(pending) != 2 {
		t.Errorf("Expected 2 pending tasks, got %d", len(pending))
	}
}

func TestManager_GetFailedTasks(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	tasks := []struct {
		taskID      string
		sourceTable string
		status      types.CheckpointStatus
	}{
		{"task1", "table1", types.StatusPending},
		{"task2", "table2", types.StatusFailed},
		{"task3", "table3", types.StatusFailed},
	}

	for _, task := range tasks {
		cp := &types.Checkpoint{
			TaskID:      task.taskID,
			TaskName:    "Test",
			SourceTable: task.sourceTable,
			TargetMeas:  "target",
			Status:      task.status,
		}
		mgr.CreateCheckpoint(cp)
	}

	ctx := context.Background()
	failed, err := mgr.GetFailedTasks(ctx)
	if err != nil {
		t.Fatalf("GetFailedTasks failed: %v", err)
	}

	if len(failed) != 2 {
		t.Errorf("Expected 2 failed tasks, got %d", len(failed))
	}
}

func TestManager_GetInProgressTasks(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	tasks := []struct {
		taskID      string
		sourceTable string
		status      types.CheckpointStatus
	}{
		{"task1", "table1", types.StatusInProgress},
		{"task2", "table2", types.StatusPending},
	}

	for _, task := range tasks {
		cp := &types.Checkpoint{
			TaskID:      task.taskID,
			TaskName:    "Test",
			SourceTable: task.sourceTable,
			TargetMeas:  "target",
			Status:      task.status,
		}
		mgr.CreateCheckpoint(cp)
	}

	ctx := context.Background()
	inProgress, err := mgr.GetInProgressTasks(ctx)
	if err != nil {
		t.Fatalf("GetInProgressTasks failed: %v", err)
	}

	if len(inProgress) != 1 {
		t.Errorf("Expected 1 in-progress task, got %d", len(inProgress))
	}
}
