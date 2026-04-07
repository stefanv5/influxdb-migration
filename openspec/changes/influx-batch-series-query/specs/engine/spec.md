## ADDED Requirements

### Requirement: Batch Mode Task Execution

The migration engine SHALL support batch series query mode for InfluxDB migrations.

#### Scenario: Enable batch mode via config
- **WHEN** `influx_to_influx.query_mode` is set to `"batch"`
- **THEN** the engine SHALL use `QueryDataBatch` instead of `QueryData`
- **AND** SHALL partition series into batches of `max_series_per_query` size

#### Scenario: Batch mode partitioning
- **WHEN** there are 250 series and `max_series_per_query` is 100
- **THEN** the engine SHALL create 3 batches: [100, 100, 50]
- **AND** SHALL process each batch sequentially

#### Scenario: Single mode unchanged
- **WHEN** `influx_to_influx.query_mode` is `"single"` or not set
- **THEN** the engine SHALL use existing `QueryData` with single series
- **AND** SHALL NOT use batch query logic

### Requirement: Batch Query Configuration

The engine SHALL respect `max_series_per_query` configuration.

#### Scenario: Configure batch size
- **WHEN** `influx_to_influx.max_series_per_query` is set to 200
- **THEN** each batch SHALL contain at most 200 series
- **AND** the default SHALL be 100 when not configured

### Requirement: Checkpoint for Batch Mode

The engine SHALL handle batch-level checkpoint in batch mode.

#### Scenario: Update checkpoint after batch
- **WHEN** a batch completes successfully
- **THEN** the engine SHALL save checkpoint with batch's max timestamp
- **AND** SHALL resume next batch from this checkpoint

### Requirement: Parallel Batch Processing

The engine SHALL control parallel batch processing via `max_series_parallel`.

#### Scenario: Parallel batch execution
- **WHEN** `max_series_parallel` is 4 and there are 20 batches
- **THEN** the engine SHALL process up to 4 batches concurrently
- **AND** SHALL maintain ordering within each batch
