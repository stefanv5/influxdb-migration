package checkpoint

import (
	"context"
	"fmt"
	"sync"

	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/migration-tools/influx-migrator/pkg/types"
	"go.uber.org/zap"
)

type Manager struct {
	store *SQLiteStore
	mu    sync.RWMutex
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

func (m *Manager) SaveCheckpoint(ctx context.Context, cp *types.Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.store.SaveCheckpoint(cp)
}

func (m *Manager) UpdateCheckpointStatus(ctx context.Context, taskID, sourceTable string, status types.CheckpointStatus, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp, err := m.store.LoadCheckpoint(taskID, sourceTable)
	if err != nil {
		return err
	}
	if cp == nil {
		return fmt.Errorf("checkpoint not found")
	}

	cp.Status = status
	if errMsg != "" {
		cp.ErrorMessage = errMsg
	}

	return m.store.SaveCheckpoint(cp)
}

func (m *Manager) LoadCheckpoint(ctx context.Context, taskID, sourceTable string) (*types.Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store.LoadCheckpoint(taskID, sourceTable)
}

func (m *Manager) ListCheckpoints(ctx context.Context, taskID string) ([]*types.Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
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
	err := m.UpdateCheckpointStatus(ctx, taskID, sourceTable, types.StatusCompleted, "")
	if err != nil {
		return err
	}
	logger.Info("task marked as completed",
		zap.String("task_id", taskID),
		zap.String("source_table", sourceTable))
	return nil
}

func (m *Manager) MarkTaskFailed(ctx context.Context, taskID, sourceTable, errMsg string) error {
	err := m.UpdateCheckpointStatus(ctx, taskID, sourceTable, types.StatusFailed, errMsg)
	if err != nil {
		return err
	}
	logger.Error("task marked as failed",
		zap.String("task_id", taskID),
		zap.String("source_table", sourceTable),
		zap.String("error", errMsg))
	return nil
}

func (m *Manager) MarkTaskInProgress(ctx context.Context, taskID, sourceTable string) error {
	err := m.UpdateCheckpointStatus(ctx, taskID, sourceTable, types.StatusInProgress, "")
	if err != nil {
		return err
	}
	logger.Debug("task marked as in_progress",
		zap.String("task_id", taskID),
		zap.String("source_table", sourceTable))
	return nil
}

func (m *Manager) ResetAll(ctx context.Context) error {
	return m.store.ResetAll()
}

func (m *Manager) DeleteCheckpoint(ctx context.Context, taskID, sourceTable string) error {
	return m.store.DeleteCheckpoint(taskID, sourceTable)
}

func (m *Manager) SaveShardGroupCheckpoint(ctx context.Context, sgCP *types.ShardGroupCheckpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.store.SaveShardGroupCheckpoint(sgCP)
}

func (m *Manager) LoadShardGroupCheckpoint(ctx context.Context, taskID, shardGroupID string) (*types.ShardGroupCheckpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store.LoadShardGroupCheckpoint(taskID, shardGroupID)
}

func (m *Manager) LoadShardGroupCheckpointForWindow(ctx context.Context, taskID, shardGroupID string, windowStart, windowEnd int64) (*types.ShardGroupCheckpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store.LoadShardGroupCheckpointForWindow(taskID, shardGroupID, windowStart, windowEnd)
}

func (m *Manager) MarkShardGroupCompleted(ctx context.Context, taskID, shardGroupID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.store.UpdateShardGroupStatus(taskID, shardGroupID, types.StatusCompleted)
}

func (m *Manager) ListShardGroupCheckpoints(ctx context.Context, taskID string) ([]*types.ShardGroupCheckpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store.ListShardGroupCheckpoints(taskID)
}
