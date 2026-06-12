package checkpoint

import (
	"context"
	"database/sql"
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
	checkpoints, err := mgr.ListCheckpoints(ctx, "Test Task")
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

func TestManager_ShardGroupCheckpointUniqueIncludesWindowEnd(t *testing.T) {
	mgr, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()
	first := &types.ShardGroupCheckpoint{
		TaskID:             "task1",
		ShardGroupID:       "sg1",
		WindowStart:        100,
		WindowEnd:          200,
		LastCompletedBatch: 1,
		LastTimestamp:      1000,
		TotalProcessedRows: 10,
		Status:             types.StatusInProgress,
	}
	second := &types.ShardGroupCheckpoint{
		TaskID:             "task1",
		ShardGroupID:       "sg1",
		WindowStart:        100,
		WindowEnd:          300,
		LastCompletedBatch: 2,
		LastTimestamp:      2000,
		TotalProcessedRows: 20,
		Status:             types.StatusCompleted,
	}

	if err := mgr.SaveShardGroupCheckpoint(ctx, first); err != nil {
		t.Fatalf("SaveShardGroupCheckpoint first failed: %v", err)
	}
	if err := mgr.SaveShardGroupCheckpoint(ctx, second); err != nil {
		t.Fatalf("SaveShardGroupCheckpoint second failed: %v", err)
	}

	checkpoints, err := mgr.ListShardGroupCheckpoints(ctx, "task1")
	if err != nil {
		t.Fatalf("ListShardGroupCheckpoints failed: %v", err)
	}
	if len(checkpoints) != 2 {
		t.Fatalf("expected 2 checkpoints with same start and different end, got %d", len(checkpoints))
	}
}

func TestManager_ShardGroupCheckpointUpsertUpdatesSameWindow(t *testing.T) {
	mgr, err := NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()
	original := &types.ShardGroupCheckpoint{
		TaskID:             "task1",
		ShardGroupID:       "sg1",
		WindowStart:        100,
		WindowEnd:          200,
		LastCompletedBatch: 0,
		LastTimestamp:      1000,
		TotalProcessedRows: 10,
		Status:             types.StatusInProgress,
	}
	updated := &types.ShardGroupCheckpoint{
		TaskID:             "task1",
		ShardGroupID:       "sg1",
		WindowStart:        100,
		WindowEnd:          200,
		LastCompletedBatch: 3,
		LastTimestamp:      4000,
		TotalProcessedRows: 40,
		Status:             types.StatusCompleted,
	}

	if err := mgr.SaveShardGroupCheckpoint(ctx, original); err != nil {
		t.Fatalf("SaveShardGroupCheckpoint original failed: %v", err)
	}
	if err := mgr.SaveShardGroupCheckpoint(ctx, updated); err != nil {
		t.Fatalf("SaveShardGroupCheckpoint updated failed: %v", err)
	}

	loaded, err := mgr.LoadShardGroupCheckpointForWindow(ctx, "task1", "sg1", 100, 200)
	if err != nil {
		t.Fatalf("LoadShardGroupCheckpointForWindow failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected checkpoint")
	}
	if loaded.WindowEnd != 200 {
		t.Fatalf("expected window_end 200, got %d", loaded.WindowEnd)
	}
	if loaded.LastCompletedBatch != 3 || loaded.LastTimestamp != 4000 || loaded.TotalProcessedRows != 40 || loaded.Status != types.StatusCompleted {
		t.Fatalf("loaded checkpoint was not updated: %+v", loaded)
	}
}

func TestManager_MigratesLegacyCompletedShardGroupCheckpointAsInProgressWithResetProgress(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(dir, "checkpoints.db"))
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err = db.Exec(`
	CREATE TABLE shard_group_checkpoints (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		task_id TEXT NOT NULL,
		shard_group_id TEXT NOT NULL,
		window_start INTEGER NOT NULL,
		window_end INTEGER NOT NULL,
		last_completed_batch INTEGER NOT NULL DEFAULT 0,
		last_timestamp INTEGER NOT NULL DEFAULT 0,
		total_processed_rows INTEGER NOT NULL DEFAULT 0,
		status TEXT NOT NULL DEFAULT 'pending',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		UNIQUE(task_id, shard_group_id, window_start)
	);
	INSERT INTO shard_group_checkpoints
		(task_id, shard_group_id, window_start, window_end, last_completed_batch,
		 last_timestamp, total_processed_rows, status, created_at, updated_at)
	VALUES ('task1', 'sg1', 100, 200, 7, 150, 99, 'completed', ?, ?);
	`, now, now)
	if err != nil {
		t.Fatalf("failed to seed legacy checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close seed db: %v", err)
	}

	mgr, err := NewManager(dir)
	if err != nil {
		t.Fatalf("NewManager failed: %v", err)
	}
	defer mgr.Close()

	loaded, err := mgr.LoadShardGroupCheckpointForWindow(context.Background(), "task1", "sg1", 100, 200)
	if err != nil {
		t.Fatalf("LoadShardGroupCheckpointForWindow failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected migrated checkpoint")
	}
	if loaded.Status != types.StatusInProgress {
		t.Fatalf("expected legacy completed checkpoint to be downgraded to %s, got %s", types.StatusInProgress, loaded.Status)
	}
	if loaded.LastCompletedBatch != -1 || loaded.LastTimestamp != 0 || loaded.TotalProcessedRows != 0 {
		t.Fatalf("expected migration to reset progress fields, got %+v", loaded)
	}
}
