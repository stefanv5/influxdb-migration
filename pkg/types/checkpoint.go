package types

import (
	"time"
)

type CheckpointStatus string

const (
	StatusPending    CheckpointStatus = "pending"
	StatusInProgress CheckpointStatus = "in_progress"
	StatusCompleted  CheckpointStatus = "completed"
	StatusFailed     CheckpointStatus = "failed"
)

type Checkpoint struct {
	ID            int64
	TaskID        string
	TaskName      string
	SourceTable   string
	TargetMeas    string
	LastID        int64
	LastTimestamp int64
	ProcessedRows int64
	Status        CheckpointStatus
	CreatedAt     time.Time
	UpdatedAt     time.Time
	ErrorMessage  string
	MappingConfig MappingConfig
}

type MigrationTask struct {
	ID            int64
	TaskID        string
	TaskName      string
	SourceAdapter string
	TargetAdapter string
	Status        CheckpointStatus
	TotalRows     int64
	MigratedRows  int64
	FailedRows    int64
	StartedAt     time.Time
	CompletedAt   time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
