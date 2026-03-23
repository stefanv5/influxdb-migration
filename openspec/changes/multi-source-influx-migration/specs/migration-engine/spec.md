## ADDED Requirements

### Requirement: Migration Task Queue
The migration engine SHALL manage a queue of migration tasks.

#### Scenario: Task queue initialization
- **WHEN** migration starts
- **THEN** the engine SHALL create a task queue from config
- **AND** SHALL parse each mapping as a separate task

#### Scenario: Task execution
- **WHEN** a task is picked from queue
- **THEN** the engine SHALL execute SourceAdapter.QueryData
- **AND** SHALL pass records to TargetAdapter.WriteBatch
- **AND** SHALL update checkpoint on success

### Requirement: Parallel Task Execution
The engine SHALL support parallel execution of independent tasks.

#### Scenario: Parallel execution
- **WHEN** multiple tasks are queued
- **THEN** the engine SHALL execute up to parallel_tasks tasks concurrently
- **AND** SHALL NOT execute chunks of the same task in parallel
- **AND** SHALL respect max_series_parallel for InfluxDB

### Requirement: Chunk Processing
The engine SHALL process data in configurable chunk sizes.

#### Scenario: Chunk processing
- **WHEN** processing a large table
- **THEN** the engine SHALL split data into chunks of chunk_size rows
- **AND** SHALL process each chunk sequentially
- **AND** SHALL update checkpoint after each chunk

### Requirement: Retry Logic
The engine SHALL implement retry with exponential backoff.

#### Scenario: Retry on failure
- **WHEN** a chunk write fails with retryable error
- **THEN** the engine SHALL retry up to max_attempts times
- **AND** SHALL use exponential backoff (initial_delay * backoff_multiplier^attempt)
- **AND** SHALL fail the task after max_attempts exceeded

### Requirement: Rate Limiting
The engine SHALL enforce rate limiting during migration.

#### Scenario: Rate limiting
- **WHEN** processing chunks
- **THEN** the engine SHALL limit to points_per_second
- **AND** SHALL enforce source_protection limits
- **AND** SHALL use chunk_interval between batches

### Requirement: Incremental Sync
The engine SHALL support incremental data synchronization.

#### Scenario: Incremental sync
- **WHEN** incremental.enabled is true
- **THEN** the engine SHALL query only records where timestamp > last_checkpoint
- **AND** SHALL update last_timestamp in checkpoint
- **AND** SHALL support configurable interval_hours

### Requirement: Error Handling
The engine SHALL handle and report errors gracefully.

#### Scenario: Error handling
- **WHEN** a chunk fails with non-retryable error
- **THEN** the engine SHALL mark the chunk as failed
- **AND** SHALL log error details
- **AND** SHALL continue with next chunk
- **AND** SHALL update migration report with error count
