# Proposal: MySQL DiscoverSchema Interface

## Summary

Add a `DiscoverSchema` method to the `SourceAdapter` interface to allow MySQL (and other relational sources) to dynamically discover table schemas instead of hardcoding column names.

## Motivation

### Current State
- MySQL source hardcodes columns: `SELECT id, timestamp, host_id, cpu_usage, memory_usage`
- No way to migrate arbitrary MySQL tables
- Tight coupling between adapter and specific schema

### Problem
- MySQL adapter only works with specific monitoring schema
- Cannot migrate user-defined MySQL tables
- No schema discovery capability

### Use Cases
1. **Generic MySQL migration**: Discover any table's schema dynamically
2. **Schema evolution**: Handle tables where columns change over time
3. **Multi-table migration**: Migrate multiple tables with different schemas

## Proposed Solution

Add `DiscoverSchema` method to `SourceAdapter` interface:

```go
type Schema struct {
    TableName   string
    Columns     []Column
    PrimaryKey  string
    TimestampColumn string
}

type Column struct {
    Name     string
    Type     string
    Nullable bool
}

type SourceAdapter interface {
    DiscoverSchema(ctx context.Context, table string) (*Schema, error)
    QueryData(ctx context.Context, table string, lastCheckpoint *Checkpoint, batchFunc func([]Record) error, cfg *QueryConfig) (*Checkpoint, error)
}
```

MySQL implementation queries `INFORMATION_SCHEMA.COLUMNS`.

## Scope

### In Scope
- [x] Add `Schema` and `Column` types
- [x] Add `DiscoverSchema` method to `SourceAdapter` interface
- [x] Implement `DiscoverSchema` for MySQL
- [x] Update MySQL `QueryData` to use discovered schema
- [x] Add tests

### Out of Scope
- [ ] Other source adapters (InfluxDB/TDengine have different schema models)
- [ ] Schema evolution handling (future enhancement)

## Configuration

No new configuration needed.

## Success Criteria
- [ ] MySQL can discover schema for any table
- [ ] QueryData uses discovered columns dynamically
- [ ] Default schema fallback for backward compatibility
- [ ] Tests pass
