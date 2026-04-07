## ADDED Requirements

### Requirement: Batch Query Configuration

The system SHALL provide configuration options for batch series query mode.

#### Scenario: Configuration structure
- **WHEN** configuring `influx_to_influx` section
- **THEN** the system SHALL support `query_mode` option
- **AND** SHALL support `max_series_per_query` option

### Requirement: Query Mode Options

The system SHALL support two query modes.

#### Scenario: Single mode (default)
- **WHEN** `query_mode` is `"single"` or not set
- **THEN** each series SHALL be queried individually
- **AND** this SHALL be the default behavior for backward compatibility

#### Scenario: Batch mode
- **WHEN** `query_mode` is `"batch"`
- **THEN** multiple series SHALL be queried in a single request
- **AND** the number per batch SHALL be controlled by `max_series_per_query`

### Requirement: Batch Size Configuration

The system SHALL allow configurable batch size.

#### Scenario: Configure batch size
- **WHEN** `max_series_per_query` is set to 150
- **THEN** each batch query SHALL contain at most 150 series
- **AND** the default SHALL be 100 when not explicitly configured

#### Scenario: Batch size limits
- **WHEN** `max_series_per_query` is less than 1
- **THEN** the system SHALL use default value of 100
- **WHEN** `max_series_per_query` is greater than 1000
- **THEN** the system SHALL cap at 1000

### Requirement: Configuration Validation

The system SHALL validate batch query configuration.

#### Scenario: Invalid query mode
- **WHEN** `query_mode` is set to anything other than `"single"` or `"batch"`
- **THEN** the validator SHALL reject the configuration
- **AND** SHALL provide clear error message

#### Scenario: Invalid batch size
- **WHEN** `max_series_per_query` is not a positive integer
- **THEN** the validator SHALL reject the configuration
- **AND** SHALL provide clear error message
