# Design: InfluxDB Batch Series Query

## 1. Overview

This design specifies a new **batch series query mode** for InfluxDB migrations. When enabled, the migration engine groups multiple series into batches and queries them together using OR拼接 in a single HTTP request.

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Migration Engine                                                │
│                                                                  │
│  ┌──────────────┐    ┌──────────────────────────────────────┐  │
│  │ Series       │    │ QueryDataBatch(series []string)     │  │
│  │ Discovery    │───▶│ - Partition into batches              │  │
│  └──────────────┘    │ - Build OR query                     │  │
│                      │ - Execute batch                        │  │
│                      │ - Return batch checkpoint              │  │
│                      └──────────────────────────────────────┘  │
│                                    │                            │
│                                    ▼                            │
│                      ┌──────────────────────────────────────┐  │
│                      │ batchFunc(records []Record)          │  │
│                      │ - TargetAdapter.WriteBatch()         │  │
│                      └──────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## 3. Configuration Schema

### 3.1 New Config Structure

```go
// pkg/types/config.go
type InfluxMigrationConfig struct {
    Enabled           bool   `mapstructure:"enabled"`
    QueryMode         string `mapstructure:"query_mode"`   // "single" | "batch"
    MaxSeriesPerQuery int    `mapstructure:"max_series_per_query"` // default: 100
}
```

### 3.2 Config File Example

```yaml
influx_to_influx:
  enabled: true
  query_mode: "batch"
  max_series_per_query: 100
```

### 3.3 Default Values

- `query_mode`: "single" (backward compatible default)
- `max_series_per_query`: 100

## 4. Interface Changes

### 4.1 SourceAdapter Interface

Add new method to `SourceAdapter`:

```go
// internal/adapter/adapter.go
type SourceAdapter interface {
    Name() string
    Connect(ctx context.Context, config map[string]interface{}) error
    Disconnect(ctx context.Context) error
    Ping(ctx context.Context) error
    DiscoverTables(ctx context.Context) ([]string, error)
    DiscoverSeries(ctx context.Context, measurement string) ([]string, error)
    DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error)

    // Existing single-series query
    QueryData(ctx context.Context, measurement string, lastCheckpoint *types.Checkpoint,
              batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)

    // NEW: Batch series query
    QueryDataBatch(ctx context.Context, measurement string, series []string,
                   lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error,
                   cfg *types.QueryConfig) (*types.Checkpoint, error)
}
```

### 4.2 QueryConfig Extension

Extend existing `QueryConfig` to support batch query params:

```go
// pkg/types/query_config.go
type QueryConfig struct {
    BatchSize int
    // Batch query specific
    MaxSeriesPerQuery int  // 0 means use default (100)
}
```

## 5. Series Partitioning Strategy

### 5.1 Partition Logic

```go
func PartitionSeries(series []string, maxPerBatch int) [][]string {
    var batches [][]string
    for i := 0; i < len(series); i += maxPerBatch {
        end := i + maxPerBatch
        if end > len(series) {
            end = len(series)
        }
        batches = append(batches, series[i:end])
    }
    return batches
}
```

### 5.2 Example

- Input: 250 series, maxPerBatch=100
- Output: 3 batches [100, 100, 50]

## 6. SQL Query Generation

### 6.1 InfluxDB V1 SQL Generation

For series with full tag definition:

```sql
SELECT * FROM measurement WHERE
  (tag1='value1' AND tag2='value2' AND time >= '...')
  OR (tag1='value3' AND tag2='value4' AND time >= '...')
  OR (tag1='value5' AND tag2='value6' AND time >= '...')
LIMIT batchSize
ORDER BY time
```

### 6.2 Series Key Parsing

InfluxDB series keys are formatted as: `measurement,tag1=value1,tag2=value2`

```go
func ParseSeriesKey(key string) (measurement string, tags map[string]string) {
    parts := strings.Split(key, ",")
    measurement = parts[0]
    tags = make(map[string]string)
    for _, part := range parts[1:] {
        kv := strings.SplitN(part, "=", 2)
        if len(kv) == 2 {
            tags[kv[0]] = kv[1]
        }
    }
    return
}
```

### 6.3 WHERE Clause Builder

```go
func BuildWhereClause(series []string) string {
    var conditions []string
    for _, s := range series {
        _, tags := ParseSeriesKey(s)
        var tagConditions []string
        for k, v := range tags {
            tagConditions = append(tagConditions, fmt.Sprintf("%s='%s'", k, v))
        }
        conditions = append(conditions, "("+strings.Join(tagConditions, " AND ")+")")
    }
    return strings.Join(conditions, " OR ")
}
```

## 7. Checkpoint Strategy

### 7.1 Batch-Level Checkpoint

- Track only the **maximum timestamp** across all records in batch
- Do NOT track per-series checkpoint (too expensive)
- On retry, entire batch is re-queried (InfluxDB supports reentrant queries)

```go
type Checkpoint struct {
    LastTimestamp int64  // max timestamp of last batch
    ProcessedRows int64  // total rows processed
}
```

### 7.2 Checkpoint Update

```go
for _, record := range records {
    if record.Time > checkpoint.LastTimestamp {
        checkpoint.LastTimestamp = record.Time
    }
}
```

## 8. Engine Integration

### 8.1 Engine Decision Logic

```go
func (e *MigrationEngine) runTask(ctx context.Context, task *MigrationTask) error {
    if e.config.InfluxMigration.QueryMode == "batch" {
        return e.runTaskBatchMode(ctx, task)
    }
    return e.runTaskSingleMode(ctx, task)
}
```

### 8.2 Batch Mode Flow

```go
func (e *MigrationEngine) runTaskBatchMode(ctx context.Context, task *MigrationTask) error {
    // 1. Discover all series for the measurement
    series, err := e.discoverSeriesForTask(ctx, task)
    if err != nil {
        return err
    }

    // 2. Partition into batches
    batchSize := e.config.InfluxMigration.MaxSeriesPerQuery
    if batchSize <= 0 {
        batchSize = 100
    }
    batches := PartitionSeries(series, batchSize)

    // 3. Process each batch
    for i, batch := range batches {
        checkpoint, err := e.sourceAdapter.QueryDataBatch(
            ctx,
            task.Mapping.SourceTable,
            batch,
            lastCheckpoint,
            func(records []types.Record) error {
                return e.processBatch(ctx, task.Mapping, records, task.TargetAdapter)
            },
            queryCfg,
        )
        // ... error handling
    }
}
```

## 9. Adapter Implementation

### 9.1 InfluxDB V1 QueryDataBatch

```go
func (a *InfluxDBV1Adapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

    whereClause := BuildWhereClause(series)
    query := fmt.Sprintf(`SELECT * FROM %s WHERE (%s) AND time >= '%s' LIMIT %d`,
        influxQuoteIdentifier(measurement),
        whereClause,
        startTime,
        batchSize,
    )

    records, err := a.executeSelectQuery(ctx, query)
    if err != nil {
        return nil, err
    }

    if err := batchFunc(records); err != nil {
        return nil, err
    }

    maxTS := getMaxTimestamp(records)
    return &types.Checkpoint{
        LastTimestamp: maxTS,
        ProcessedRows: int64(len(records)),
    }, nil
}
```

### 9.2 InfluxDB V2 QueryDataBatch

```go
func (a *InfluxDBV2Adapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

    // Build Flux filter expression with OR
    fluxFilter := BuildFluxFilter(series)
    fluxQuery := fmt.Sprintf(`
        from(bucket: "%s")
          |> range(start: %s)
          |> filter(fn: (r) => r._measurement == "%s" and (%s))
          |> limit(n: %d)
    `, a.config.Bucket, startTime, measurement, fluxFilter, batchSize)

    records, err := a.executeFluxSelect(ctx, fluxQuery)
    if err != nil {
        return nil, err
    }

    if err := batchFunc(records); err != nil {
        return nil, err
    }

    maxTS := getMaxTimestamp(records)
    return &types.Checkpoint{
        LastTimestamp: maxTS,
        ProcessedRows: int64(len(records)),
    }, nil
}
```

### 9.3 OpenGemini Target Adapter

OpenGemini supports similar InfluxDB line protocol, so batch records can be written directly without modification.

## 10. Error Handling

### 10.1 Retry Strategy

- Batch-level retry (entire batch on failure)
- Exponential backoff with max_attempts
- After max retries, mark batch as failed and continue

### 10.2 Partial Failure

- If batchFunc fails mid-batch, rollback is not possible
- Log partial progress and retry entire batch
- Target should handle idempotent writes (InfluxDB does)

## 11. Files to Modify

```
pkg/types/
├── query_config.go              # Add MaxSeriesPerQuery field

internal/adapter/
├── adapter.go                    # Add QueryDataBatch to interface

internal/adapter/source/
├── influxdb.go                   # Implement QueryDataBatch for V1 and V2

internal/engine/
├── migration.go                  # Add batch mode execution path
├── config.go                     # Add InfluxMigrationConfig

internal/config/
├── validator.go                   # Validate batch query config

config.yaml.example                # Add batch query config example
```

## 12. Files to Create

```
openspec/changes/influx-batch-series-query/
├── proposal.md
├── design.md
├── tasks.md
└── specs/
    ├── source-adapter/spec.md
    ├── engine/spec.md
    └── config/spec.md
```
