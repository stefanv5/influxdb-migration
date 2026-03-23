## ADDED Requirements

### Requirement: SQLite Checkpoint Store
The system SHALL use SQLite for checkpoint storage.

#### Scenario: Store initialization
- **WHEN** the migration tool starts
- **THEN** it SHALL initialize SQLite database at checkpoint_dir/*.db
- **AND** SHALL create checkpoints table if not exists

#### Scenario: Save checkpoint
- **WHEN** a chunk completes successfully
- **THEN** the system SHALL save checkpoint with task_id, source_table, last_id, last_timestamp, processed_rows
- **AND** SHALL update status to 'completed'

### Requirement: Checkpoint Recovery
The system SHALL support resuming from the last checkpoint.

#### Scenario: Recovery on restart
- **WHEN** a migration task restarts after failure
- **THEN** the system SHALL load checkpoint for each pending task
- **AND** SHALL continue from last_timestamp and last_id
- **AND** SHALL mark task status as 'in_progress'

#### Scenario: Failed task recovery
- **WHEN** a task has status='failed' or status='in_progress'
- **THEN** the system SHALL resume from the saved checkpoint
- **AND** SHALL skip already processed records

### Requirement: Checkpoint Status Tracking
The system SHALL track the status of each migration task.

#### Scenario: Status tracking
- **WHEN** a task starts
- **THEN** the system SHALL create checkpoint record with status='pending'
- **AND** SHALL update status to 'in_progress' when running
- **AND** SHALL update status to 'completed' or 'failed' when done

### Requirement: Multiple Task Support
The checkpoint manager SHALL support multiple concurrent tasks.

#### Scenario: Multiple tasks
- **WHEN** running multiple migration tasks
- **THEN** each task SHALL have independent checkpoint records
- **AND** tasks SHALL NOT interfere with each other
- **AND** the system SHALL query checkpoints by task_id

### Requirement: Checkpoint Persistence
The system SHALL persist checkpoints to disk regularly.

#### Scenario: Periodic persistence
- **WHEN** processing chunks
- **THEN** the system SHALL save checkpoint after each chunk
- **AND** SHALL ensure durability on crash
