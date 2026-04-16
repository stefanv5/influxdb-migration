# Checkpoint Manager Spec: Shard Group-Aware Migration

## ADDED Requirements

### Requirement: Shard Group Checkpoint Storage

The checkpoint manager SHALL store and retrieve shard group-level checkpoints.

#### Scenario: Save shard group checkpoint
- **WHEN** `SaveShardGroupCheckpoint` is called with task ID, shard group ID, and checkpoint data
- **THEN** the manager SHALL persist the checkpoint to SQLite
- **AND** SHALL use `ON CONFLICT` to update existing checkpoints
- **AND** SHALL store: shard_group_id, start_time, end_time, last_completed_batch, last_timestamp, total_processed_rows, status

#### Scenario: Load shard group checkpoint
- **WHEN** `LoadShardGroupCheckpoint` is called with task ID and shard group ID
- **THEN** the manager SHALL retrieve the checkpoint from SQLite
- **AND** SHALL return `nil` if no checkpoint exists (first run)
- **AND** SHALL return the checkpoint data if found

#### Scenario: Mark shard group completed
- **WHEN** `MarkShardGroupCompleted` is called
- **THEN** the manager SHALL update the status to `StatusCompleted`
- **AND** SHALL record the completion timestamp

#### Scenario: List all shard group checkpoints for a task
- **WHEN** `ListShardGroupCheckpoints` is called with a task ID
- **THEN** the manager SHALL return all shard group checkpoints for that task
- **AND** SHALL order them by shard group ID

### Requirement: Shard Group Checkpoint Schema

The checkpoint manager SHALL use a dedicated table for shard group checkpoints.

#### Scenario: Checkpoint table schema
- **GIVEN** the `shard_group_checkpoints` table
- **THEN** it SHALL have columns: `id`, `task_id`, `shard_group_id`, `start_time`, `end_time`, `last_completed_batch`, `last_timestamp`, `total_processed_rows`, `status`, `created_at`, `updated_at`
- **AND** SHALL have a unique constraint on `(task_id, shard_group_id)`

#### Scenario: Checkpoint uniqueness
- **WHEN** saving a checkpoint for a task and shard group that already exists
- **THEN** the existing record SHALL be updated (UPSERT behavior)

### Requirement: Checkpoint Recovery

The checkpoint manager SHALL support recovery scenarios.

#### Scenario: Recover from in-progress shard group
- **WHEN** a shard group has status `StatusInProgress`
- **THEN** the recovery process SHALL use `LastCompletedBatch` to skip completed batches
- **AND** SHALL resume from `LastCompletedBatch + 1`

#### Scenario: Recover from failed shard group
- **WHEN** a shard group has status `StatusFailed`
- **THEN** the recovery process SHALL treat it the same as `StatusInProgress`
- **AND** SHALL attempt to complete the shard group

#### Scenario: No checkpoint exists
- **WHEN** `LoadShardGroupCheckpoint` returns `nil`
- **THEN** the engine SHALL start from batch 0
- **AND** SHALL treat it as a fresh start

### Requirement: Backward Compatibility

The checkpoint manager SHALL maintain backward compatibility with existing task-level checkpoints.

#### Scenario: Existing task checkpoints unchanged
- **WHEN** using non-shard-group mode (single or batch)
- **THEN** the existing `Checkpoints` table SHALL be used
- **AND** shard group checkpoints SHALL NOT be created

#### Scenario: Dual checkpoint support
- **WHEN** a migration task uses shard-group mode
- **THEN** it SHALL use `shard_group_checkpoints` table
- **AND** when using single/batch mode, it SHALL use `checkpoints` table
