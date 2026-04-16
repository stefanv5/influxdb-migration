# Source Adapter Spec: Shard Group-Aware Migration

## ADDED Requirements

### Requirement: Discover Shard Groups

The source adapter SHALL provide a method to discover InfluxDB shard groups.

#### Scenario: Discover shard groups from InfluxDB V1
- **WHEN** `DiscoverShardGroups` is called on a V1 adapter
- **THEN** the adapter SHALL execute `SHOW SHARDS` query
- **AND** SHALL parse the results to extract shard group ID, start time, and end time
- **AND** SHALL return a list of `ShardGroup` sorted by start time ascending

#### Scenario: Discover shard groups from InfluxDB V2
- **WHEN** `DiscoverShardGroups` is called on a V2 adapter
- **THEN** the adapter SHALL query the `/api/v2/shards` endpoint
- **AND** SHALL parse the JSON response to extract shard group information
- **AND** SHALL return a list of `ShardGroup` sorted by start time ascending

#### Scenario: Shard group contains no relevant series
- **WHEN** a shard group's time range does not overlap with the query time range
- **THEN** the shard group SHALL be excluded from processing
- **AND** no series discovery SHALL be attempted for that shard group

### Requirement: Paginated Series Discovery

The source adapter SHALL provide a paginated series discovery method to avoid loading all series into memory.

#### Scenario: Discover series with time range
- **WHEN** `DiscoverSeriesPaginated` is called with measurement, start time, end time, and batch size
- **THEN** the adapter SHALL return a `SeriesIterator`
- **AND** the iterator SHALL support `NextBatch` to retrieve series in batches
- **AND** each batch SHALL contain at most `batchSize` series keys

#### Scenario: Series iterator pagination
- **WHEN** `NextBatch` is called on a SeriesIterator
- **THEN** the iterator SHALL execute `SHOW SERIES FROM measurement WHERE time >= start AND time < end LIMIT batchSize OFFSET offset`
- **AND** SHALL increment offset by the number of series returned
- **AND** SHALL return `nil` when no more series are available

#### Scenario: Series iterator has no more series
- **WHEN** `NextBatch` returns fewer than `batchSize` series
- **THEN** `HasMore` SHALL return `false`
- **AND** subsequent calls to `NextBatch` SHALL return `nil`

### Requirement: Shard Group Time Range Intersection

The source adapter SHALL correctly calculate the effective time range for a shard group within a query range.

#### Scenario: Query range partially overlaps shard group
- **GIVEN** a shard group with time range `[2024-01-01, 2024-02-01)`
- **AND** query range `[2024-01-15, 2024-03-01)`
- **WHEN** `EffectiveTimeRange` is calculated
- **THEN** the result SHALL be `[2024-01-15, 2024-02-01)` (intersection)

#### Scenario: Query range fully contains shard group
- **GIVEN** a shard group with time range `[2024-01-01, 2024-02-01)`
- **AND** query range `[2023-01-01, 2025-01-01)`
- **WHEN** `EffectiveTimeRange` is calculated
- **THEN** the result SHALL be `[2024-01-01, 2024-02-01)` (full shard group)

#### Scenario: Query range outside shard group
- **GIVEN** a shard group with time range `[2024-01-01, 2024-02-01)`
- **AND** query range `[2025-01-01, 2025-06-01)`
- **WHEN** `Overlaps` is checked
- **THEN** the result SHALL be `false`

### Requirement: Checkpoint for Batch Mode

The batch query SHALL return a checkpoint based on batch-level max timestamp.

#### Scenario: Return batch checkpoint
- **WHEN** a batch query completes
- **THEN** the checkpoint.LastTimestamp SHALL be the maximum timestamp across all records
- **AND** the checkpoint.ProcessedRows SHALL be the count of records in this batch

### Requirement: SQL Injection Prevention in Series Discovery

The series discovery query SHALL properly escape special characters to prevent SQL injection.

#### Scenario: Tag value contains single quote
- **WHEN** a tag value contains a single quote (e.g., `host=server's-1`)
- **THEN** the value SHALL be escaped by doubling the quote (`''`)
- **AND** the resulting SQL SHALL be `WHERE tag='server''s-1'`

#### Scenario: Tag value contains backslash
- **WHEN** a tag value contains a backslash (e.g., `path=C:\Windows`)
- **THEN** the value SHALL be escaped (`\\`)
- **AND** the resulting SQL SHALL be `WHERE tag='C:\\Windows'`
