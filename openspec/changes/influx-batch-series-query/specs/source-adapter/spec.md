## ADDED Requirements

### Requirement: Batch Series Query Interface

The source adapter SHALL provide a `QueryDataBatch` method for querying multiple series in a single request.

#### Scenario: Batch query interface
- **WHEN** batch query mode is enabled
- **THEN** the adapter SHALL implement `QueryDataBatch(measurement, series []string, ...)`
- **AND** SHALL accept series as full series keys (e.g., `measurement,tag1=value1,tag2=value2`)

#### Scenario: InfluxDB V1 batch query
- **WHEN** `QueryDataBatch` is called with V1 adapter
- **THEN** the adapter SHALL build SQL with OR拼接 for all series
- **AND** SHALL paginate through results until all records are fetched
- **AND** SHALL return batch checkpoint with max timestamp

#### Scenario: InfluxDB V2 batch query
- **WHEN** `QueryDataBatch` is called with V2 adapter
- **THEN** the adapter SHALL build Flux query with OR filter
- **AND** SHALL paginate through results using `limit()` and timestamp progression
- **AND** SHALL return batch checkpoint with max timestamp

### Requirement: Series Key Parsing

The adapter SHALL correctly parse series keys into measurement and tag components.

#### Scenario: Parse series key
- **WHEN** a series key like `cpu,host=server1,region=us` is provided
- **THEN** the adapter SHALL extract measurement as `cpu`
- **AND** SHALL extract tags as `{"host": "server1", "region": "us"}`

### Requirement: WHERE Clause Building

The adapter SHALL build a correct WHERE clause using AND/OR logic with proper escaping.

#### Scenario: Build WHERE clause
- **WHEN** 3 series are provided
- **THEN** the WHERE clause SHALL be `(tag1='v1' AND tag2='v2') OR (tag1='v3' AND tag2='v4') OR (tag1='v5' AND tag2='v6')`
- **AND** SHALL escape single quotes in tag values (replace ' with '')
- **AND** SHALL escape backslashes (replace \ with \\)
- **AND** SHALL include time filter in outer clause

#### Scenario: SQL injection prevention
- **WHEN** tag values contain special characters (e.g., `host=server'; DROP TABLE users;--`)
- **THEN** the adapter SHALL escape these characters
- **AND** SHALL prevent SQL injection attacks

### Requirement: Batch Checkpoint

The batch query SHALL return a checkpoint based on batch-level max timestamp.

#### Scenario: Return batch checkpoint
- **WHEN** a batch query completes
- **THEN** the checkpoint.LastTimestamp SHALL be the maximum timestamp across all records
- **AND** the checkpoint.ProcessedRows SHALL be the count of records in this batch

### Requirement: SSL/TLS Support

The InfluxDB adapter SHALL support SSL/TLS connections with configurable certificate verification.

#### Scenario: SSL enabled with skip_verify
- **WHEN** `ssl.enabled` is true and `ssl.skip_verify` is true
- **THEN** the adapter SHALL connect using HTTPS
- **AND** SHALL skip TLS certificate verification
- **AND** SHALL require `ALLOW_INSECURE_TLS=1` environment variable to be set

#### Scenario: SSL enabled with certificate verification
- **WHEN** `ssl.enabled` is true and `ssl.skip_verify` is false
- **THEN** the adapter SHALL connect using HTTPS
- **AND** SHALL verify TLS certificates (future: with custom CA cert support)
