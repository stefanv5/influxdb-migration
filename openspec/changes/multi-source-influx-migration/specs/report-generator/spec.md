## ADDED Requirements

### Requirement: Migration Report Generation
The system SHALL generate detailed migration reports.

#### Scenario: Report generation
- **WHEN** a migration task completes (success or failure)
- **THEN** the system SHALL generate a migration report
- **AND** SHALL include migration_id, status, duration, row counts

### Requirement: Report Formats
The system SHALL support multiple report output formats.

#### Scenario: JSON format
- **WHEN** report format is 'json'
- **THEN** the system SHALL output machine-readable JSON
- **AND** SHALL include all migration details

#### Scenario: HTML format
- **WHEN** report format is 'html'
- **THEN** the system SHALL output human-readable HTML
- **AND** SHALL include tables and styling for visualization

#### Scenario: Markdown format
- **WHEN** report format is 'markdown'
- **THEN** the system SHALL output Markdown format
- **AND** SHALL be suitable for documentation

### Requirement: Report Content
The migration report SHALL include comprehensive details.

#### Scenario: Report content
- **WHEN** generating a report
- **THEN** the report SHALL include:
  - migration_id and task_name
  - status (completed/failed/in_progress)
  - started_at and completed_at
  - duration_seconds
  - summary (source, target, total_rows, transferred_rows, failed_rows)
  - errors (batch_id, timestamp, error, retryable)
  - checkpoints (last_id, last_timestamp, saved_at)

### Requirement: Report Storage
The system SHALL save reports to the specified report directory.

#### Scenario: Report storage
- **WHEN** a report is generated
- **THEN** the system SHALL save to report_dir/
- **AND** SHALL name file as {migration_id}_{timestamp}.{format}
- **AND** SHALL create directory if not exists
