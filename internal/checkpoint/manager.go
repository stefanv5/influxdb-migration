package checkpoint

import (
	"context"
	"fmt"
	"time"

	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/migration-tools/influx-migrator/pkg/types"
	"go.uber.org/zap"
)

type Manager struct {
	store *SQLiteStore
}

func NewManager(dir string) (*Manager, error) {
	store, err := NewSQLiteStore(dir)
	if err != nil {
		return nil, err
	}
	return &Manager{store: store}, nil
}

func (m *Manager) Close() error {
	return m.store.Close()
}

func (m *Manager) CreateCheckpoint(task *types.Checkpoint) error {
	return m.store.SaveCheckpoint(task)
}

func (m *Manager) SaveCheckpoint(ctx context.Context, taskID, sourceTable string, lastID int64, lastTS time.Time, processedRows int64, status types.CheckpointStatus) error {
	cp, err := m.store.LoadCheckpoint(taskID, sourceTable)
	if err != nil {
		return err
	}
	if cp == nil {
		return fmt.Errorf("checkpoint not found for task %s, table %s", taskID, sourceTable)
	}

	cp.LastID = lastID
	cp.LastTimestamp = lastTS
	cp.ProcessedRows = processedRows
	cp.Status = status

	return m.store.SaveCheckpoint(cp)
}

func (m *Manager) LoadCheckpoint(ctx context.Context, taskID, sourceTable string) (*types.Checkpoint, error) {
	return m.store.LoadCheckpoint(taskID, sourceTable)
}

func (m *Manager) ListCheckpoints(ctx context.Context, taskID string) ([]*types.Checkpoint, error) {
	return m.store.ListCheckpoints(taskID)
}

func (m *Manager) GetPendingTasks(ctx context.Context) ([]*types.Checkpoint, error) {
	return m.store.GetTasksByStatus(types.StatusPending)
}

func (m *Manager) GetFailedTasks(ctx context.Context) ([]*types.Checkpoint, error) {
	return m.store.GetTasksByStatus(types.StatusFailed)
}

func (m *Manager) GetInProgressTasks(ctx context.Context) ([]*types.Checkpoint, error) {
	return m.store.GetTasksByStatus(types.StatusInProgress)
}

func (m *Manager) MarkTaskCompleted(ctx context.Context, taskID, sourceTable string) error {
	cp, err := m.store.LoadCheckpoint(taskID, sourceTable)
	if err != nil {
		return err
	}
	if cp == nil {
		return fmt.Errorf("checkpoint not found")
	}

	cp.Status = types.StatusCompleted
	logger.Info("task marked as completed",
		zap.String("task_id", taskID),
		zap.String("source_table", sourceTable))
	return m.store.SaveCheckpoint(cp)
}

func (m *Manager) MarkTaskFailed(ctx context.Context, taskID, sourceTable, errMsg string) error {
	cp, err := m.store.LoadCheckpoint(taskID, sourceTable)
	if err != nil {
		return err
	}
	if cp == nil {
		return fmt.Errorf("checkpoint not found")
	}

	cp.Status = types.StatusFailed
	cp.ErrorMessage = errMsg
	logger.Error("task marked as failed",
		zap.String("task_id", taskID),
		zap.String("source_table", sourceTable),
		zap.String("error", errMsg))
	return m.store.SaveCheckpoint(cp)
}

func (m *Manager) MarkTaskInProgress(ctx context.Context, taskID, sourceTable string) error {
	cp, err := m.store.LoadCheckpoint(taskID, sourceTable)
	if err != nil {
		return err
	}
	if cp == nil {
		return fmt.Errorf("checkpoint not found")
	}

	cp.Status = types.StatusInProgress
	logger.Debug("task marked as in_progress",
		zap.String("task_id", taskID),
		zap.String("source_table", sourceTable))
	return m.store.SaveCheckpoint(cp)
}
