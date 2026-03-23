## ADDED Requirements

### Requirement: Target Adapter Interface
The system SHALL provide a TargetAdapter interface that all target database adapters MUST implement.

#### Scenario: Adapter discovery
- **WHEN** the migration engine starts
- **THEN** it SHALL discover all registered target adapters via the plugin registry

#### Scenario: InfluxDB 1.X target
- **WHEN** writing to InfluxDB 1.X target
- **THEN** the adapter SHALL use Line Protocol for batch writes
- **AND** SHALL support HTTP API with configurable URL

#### Scenario: InfluxDB 2.X target
- **WHEN** writing to InfluxDB 2.X target
- **THEN** the adapter SHALL use HTTP API v2 with organization/bucket
- **AND** SHALL include authorization token

### Requirement: Batch Write
The target adapter SHALL write data in batches for efficiency.

#### Scenario: Batch write
- **WHEN** receiving a batch of records
- **THEN** the adapter SHALL write all records in a single transaction
- **AND** SHALL return error if any record fails
- **AND** SHALL support configurable batch size

### Requirement: SSL/TLS Support
The target adapter SHALL support secure connections.

#### Scenario: SSL connection
- **WHEN** configuring a target adapter with SSL enabled
- **THEN** the adapter SHALL use HTTPS for connections
- **AND** SHALL support insecure_skip_verify option for testing

### Requirement: Write Verification
The target adapter SHALL verify successful write.

#### Scenario: Write verification
- **WHEN** a batch write completes
- **THEN** the adapter SHALL verify the write was successful
- **AND** SHALL return WriteResult with rows_written count
- **OR** SHALL return error with details on failure
