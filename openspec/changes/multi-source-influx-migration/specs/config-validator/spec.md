## ADDED Requirements

### Requirement: Configuration Schema Validation
The system SHALL validate configuration against a defined schema.

#### Scenario: Required fields
- **WHEN** parsing config.yaml
- **THEN** the system SHALL verify required fields are present:
  - global.name
  - sources (at least one)
  - tasks (at least one)
  - migration.chunk_size

#### Scenario: Source adapter validation
- **WHEN** validating a source adapter config
- **THEN** the system SHALL verify:
  - name is unique
  - host and port are valid
  - required credentials are present or env var is set

### Requirement: Environment Variable Support
The system SHALL support environment variable substitution in config.

#### Scenario: Env var substitution
- **WHEN** config contains ${ENV_VAR}
- **THEN** the system SHALL replace with environment variable value
- **AND** SHALL fail if env var is not set and no default provided

### Requirement: SSL Configuration Validation
The system SHALL validate SSL/TLS configuration.

#### Scenario: SSL validation
- **WHEN** ssl.enabled is true
- **THEN** the system SHALL validate:
  - URL uses https for InfluxDB
  - skip_verify is boolean
  - ca_cert path exists if provided

### Requirement: Task Mapping Validation
The system SHALL validate migration task mappings.

#### Scenario: Mapping validation
- **WHEN** validating a task mapping
- **THEN** the system SHALL verify:
  - source exists in sources
  - target exists in targets
  - timestamp_column is specified
  - schema.tags and schema.fields are properly formatted

### Requirement: Default Values
The system SHALL provide sensible defaults for optional fields.

#### Scenario: Default values
- **WHEN** optional fields are not specified
- **THEN** the system SHALL use defaults:
  - migration.parallel_tasks = 4
  - migration.chunk_size = 10000
  - migration.chunk_interval = "100ms"
  - retry.max_attempts = 3
  - rate_limit.enabled = false
