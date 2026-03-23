## ADDED Requirements

### Requirement: Source Adapter Interface
The system SHALL provide a SourceAdapter interface that all source database adapters MUST implement.

#### Scenario: Adapter discovery
- **WHEN** the migration engine starts
- **THEN** it SHALL discover all registered source adapters via the plugin registry

#### Scenario: MySQL adapter
- **WHEN** connecting to MySQL source
- **THEN** the adapter SHALL use SHOW TABLES for table discovery
- **AND** SHALL use WHERE (ts, id) > (last_ts, last_id) LIMIT for data queries
- **AND** SHALL support SSL/TLS with skip_verify option

#### Scenario: TDengine adapter
- **WHEN** connecting to TDengine source
- **THEN** the adapter SHALL use SHOW TABLES for subtable discovery
- **AND** SHALL use WHERE tbname = 'xxx' AND ts BETWEEN for data queries
- **AND** SHALL support both 2.X and 3.X versions

#### Scenario: InfluxDB adapter
- **WHEN** connecting to InfluxDB source
- **THEN** the adapter SHALL use SHOW SERIES for series discovery
- **AND** SHALL use WHERE tag='value' AND time BETWEEN for data queries
- **AND** SHALL support both 1.X and 2.X versions

### Requirement: Connection Pool Management
The source adapter SHALL manage connection pools with configurable limits.

#### Scenario: Connection pool limits
- **WHEN** configuring a source adapter
- **THEN** the system SHALL support max_open_conns, max_idle_conns
- **AND** SHALL support conn_max_lifetime and conn_max_idle_time

### Requirement: Data Query with Checkpoint
The source adapter SHALL support resuming from the last checkpoint.

#### Scenario: Resume from checkpoint
- **WHEN** a migration task resumes after failure
- **THEN** the adapter SHALL accept a Checkpoint parameter
- **AND** SHALL query data starting from last_timestamp and last_id
- **AND** SHALL return updated checkpoint after batch processing

### Requirement: NULL Value Handling
The source adapter SHALL skip NULL values during data reading.

#### Scenario: NULL fields
- **WHEN** reading records with NULL field values
- **THEN** the adapter SHALL exclude NULL fields from the output Record
- **AND** SHALL NOT send NULL values to the target adapter
