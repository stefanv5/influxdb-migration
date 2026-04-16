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

type CheckpointType string

const (
	CheckpointTypeTask       CheckpointType = "task"
	CheckpointTypeShardGroup CheckpointType = "shard_group"
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

// ShardGroupCheckpoint tracks migration progress per shard group and time window
type ShardGroupCheckpoint struct {
	TaskID              string           `json:"task_id"`
	ShardGroupID        string           `json:"shard_group_id"`
	WindowStart         int64            `json:"window_start"`          // Unix nano - start of time window
	WindowEnd           int64            `json:"window_end"`            // Unix nano - end of time window
	LastCompletedBatch  int              `json:"last_completed_batch"`  // 0-indexed batch within window
	LastTimestamp       int64            `json:"last_timestamp"`        // max timestamp of last batch
	TotalProcessedRows  int64            `json:"total_processed_rows"`
	Status              CheckpointStatus `json:"status"`
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
