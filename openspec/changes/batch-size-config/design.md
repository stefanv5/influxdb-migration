# Design: Batch Size Configuration via QueryData Config

## 1. Overview

This design specifies adding an optional `QueryConfig` parameter to the `QueryData` method in the `SourceAdapter` interface, allowing the migration engine to pass configuration options like `batch_size` to source adapters.

## 2. Interface Change

### 2.1 Current Interface

```go
type SourceAdapter interface {
    Name() string
    DiscoverTables(ctx context.Context) ([]string, error)
    DiscoverSeries(ctx context.Context, measurement string) ([]string, error)
    QueryData(ctx context.Context, table string, lastCheckpoint *Checkpoint, batchFunc func([]Record) error) (*Checkpoint, error)
}
```

### 2.2 Proposed Interface

```go
type QueryConfig struct {
    BatchSize int
}

type SourceAdapter interface {
    Name() string
    DiscoverTables(ctx context.Context) ([]string, error)
    DiscoverSeries(ctx context.Context, measurement string) ([]string, error)
    QueryData(ctx context.Context, table string, lastCheckpoint *Checkpoint, batchFunc func([]Record) error, cfg *QueryConfig) (*Checkpoint, error)
}
```

## 3. QueryConfig Struct

```go
// pkg/types/query_config.go
package types

type QueryConfig struct {
    BatchSize int
}
```

## 4. Implementation Changes

### 4.1 InfluxDB V1 Source

**Current (influxdb.go:173):**
```go
batchSize := 10000
```

**Proposed:**
```go
batchSize := 10000
if cfg != nil && cfg.BatchSize > 0 {
    batchSize = cfg.BatchSize
}
```

### 4.2 InfluxDB V2 Source

Same change as V1.

### 4.3 MySQL Source

**Current (source/mysql.go:142):**
```go
batchSize := 10000
```

**Proposed:**
```go
batchSize := 10000
if cfg != nil && cfg.BatchSize > 0 {
    batchSize = cfg.BatchSize
}
```

### 4.4 TDengine Source

**Current (source/tdengine.go):**
Uses internal batching of 1000 records per batchFunc call.

**Proposed:**
```go
batchSize := 1000  // TDengine internal batch
if cfg != nil && cfg.BatchSize > 0 {
    batchSize = cfg.BatchSize
}
```

## 5. Migration Engine Changes

### 5.1 Current Call

```go
checkpoint, err := sourceAdapter.QueryData(ctx, sourceTable, lastCheckpoint, func(records []types.Record) error {
    return e.processBatch(ctx, task.Mapping, records, targetAdapter)
})
```

### 5.2 Proposed Call

```go
queryCfg := &types.QueryConfig{
    BatchSize: e.config.Migration.ChunkSize,
}
checkpoint, err := sourceAdapter.QueryData(ctx, sourceTable, lastCheckpoint, func(records []types.Record) error {
    return e.processBatch(ctx, task.Mapping, records, targetAdapter)
}, queryCfg)
```

## 6. Adapter Interface Changes

All adapter implementations need signature update:

```go
func (a *InfluxDBV1Adapter) QueryData(ctx context.Context, measurement string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)

func (a *InfluxDBV2Adapter) QueryData(ctx context.Context, measurement string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)

func (a *MySQLAdapter) QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)

func (a *TDengineAdapter) QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)
```

## 7. Files to Modify

```
pkg/types/
├── query_config.go          # NEW: QueryConfig struct

internal/adapter/source/
├── influxdb.go               # Update QueryData signature and batch size logic
├── mysql.go                 # Update QueryData signature and batch size logic
├── tdengine.go              # Update QueryData signature and batch size logic
├── registry_test.go         # Update mock signature

internal/engine/
├── migration.go             # Pass QueryConfig to QueryData
```
