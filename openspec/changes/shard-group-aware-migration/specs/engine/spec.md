# Engine Spec: Shard Group-Aware Migration

## ADDED Requirements

### Requirement: Shard Group Mode Task Execution

The migration engine SHALL support shard group-aware query mode for InfluxDB migrations.

#### Scenario: Enable shard-group mode via config
- **WHEN** `influx_to_influx.query_mode` is set to `"shard-group"`
- **THEN** the engine SHALL use `DiscoverShardGroups` to get all shard groups
- **AND** SHALL filter to only shard groups that overlap with the query time range
- **AND** SHALL process each relevant shard group sequentially

#### Scenario: Shard group mode partitioning
- **WHEN** there are 3 relevant shard groups and `series_batch_size` is 50
- **THEN** the engine SHALL process each shard group one at a time
- **AND** within each shard group, SHALL discover series in batches of 50

#### Scenario: Single mode unchanged
- **WHEN** `influx_to_influx.query_mode` is `"single"` or `"batch"`
- **THEN** the engine SHALL use existing `QueryData` or `QueryDataBatch` methods
- **AND** SHALL NOT use shard group-aware logic

### Requirement: Shard Group Checkpoint

The engine SHALL handle shard-group-level checkpoint with at-least-once semantics.

#### Scenario: Update checkpoint after batch
- **WHEN** a batch completes successfully within a shard group
- **THEN** the engine SHALL save `ShardGroupCheckpoint` with `LastCompletedBatch` set to the batch index
- **AND** SHALL set `LastTimestamp` to the batch's max timestamp
- **AND** SHALL set status to `StatusInProgress`

#### Scenario: Checkpoint persistence on crash
- **WHEN** a batch completes but process crashes before next batch starts
- **THEN** on restart the engine SHALL load `ShardGroupCheckpoint`
- **AND** SHALL resume from `LastCompletedBatch + 1`
- **AND** SHALL use at-least-once semantics (may re-process this batch, but never lose data)

#### Scenario: Final shard group checkpoint on completion
- **WHEN** all batches in a shard group complete successfully
- **THEN** the engine SHALL update `ShardGroupCheckpoint` status to `StatusCompleted`

#### Scenario: Resume after partial shard group completion
- **GIVEN** Shard Group 1 is completed with status `StatusCompleted`
- **AND** Shard Group 2 has `LastCompletedBatch=3` and status `StatusInProgress`
- **WHEN** migration resumes
- **THEN** the engine SHALL skip Shard Group 1
- **AND** SHALL resume Shard Group 2 from batch 4

### Requirement: Time Range Calculation

The engine SHALL correctly calculate effective time ranges for shard groups.

#### Scenario: Query range boundaries align with shard group
- **GIVEN** query range `[2024-01-15, 2024-03-15)`
- **AND** shard group `[2024-01-01, 2024-02-01)`
- **WHEN** `EffectiveTimeRange` is calculated
- **THEN** the effective range SHALL be `[2024-01-15, 2024-02-01)` (intersection)

#### Scenario: Query before shard group start
- **GIVEN** query range `[2023-12-01, 2024-01-15)`
- **AND** shard group `[2024-01-01, 2024-02-01)`
- **WHEN** `EffectiveTimeRange` is calculated
- **THEN** the effective range SHALL be `[2024-01-01, 2024-01-15)` (shard group start limited)

#### Scenario: Query after shard group end
- **GIVEN** query range `[2024-02-15, 2024-06-01)`
- **AND** shard group `[2024-01-01, 2024-02-01)`
- **WHEN** `EffectiveTimeRange` is calculated
- **THEN** the effective range SHALL be `[2024-02-15, 2024-02-01)` (empty/invalid, shard group skipped)

### Requirement: Series Batch Processing

The engine SHALL process series in batches within each shard group.

#### Scenario: Skip already completed batches
- **WHEN** resuming a shard group with `LastCompletedBatch=5`
- **THEN** the engine SHALL skip batches 0 through 5
- **AND** SHALL start processing from batch 6

#### Scenario: Process remaining batches
- **WHEN** resuming with 10 total batches and `LastCompletedBatch=5`
- **THEN** the engine SHALL process batches 6, 7, 8, 9

#### Scenario: Batch iteration termination
- **WHEN** `SeriesIterator.NextBatch` returns `nil`
- **THEN** the engine SHALL mark the shard group as completed
- **AND** SHALL proceed to the next shard group

### Requirement: Memory Efficiency

The engine SHALL maintain bounded memory usage during shard group-aware migration.

#### Scenario: Memory bounded by series batch size
- **WHEN** migrating 100,000 series with `series_batch_size=50`
- **THEN** memory usage for series keys SHALL be O(50) not O(100,000)
- **AND** the engine SHALL NOT load all series into memory at once
