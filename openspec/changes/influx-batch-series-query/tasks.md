# Tasks: InfluxDB Batch Series Query

## Overview

Implement batch series query mode that queries multiple InfluxDB series in a single request using OR拼接, reducing query count from N to N/max_series_per_query.

---

## Task 1: Add Batch Query Config

**Files:**
- Modify: `pkg/types/config.go`
- Modify: `pkg/types/query_config.go`

- [x] **Step 1: Add InfluxMigrationConfig to types**

Add to `pkg/types/config.go`:

```go
type InfluxMigrationConfig struct {
    Enabled           bool   `mapstructure:"enabled"`
    QueryMode         string `mapstructure:"query_mode"`   // "single" | "batch"
    MaxSeriesPerQuery int    `mapstructure:"max_series_per_query"`
}
```

- [x] **Step 2: Add MaxSeriesPerQuery to QueryConfig**

Modify `pkg/types/query_config.go`:

```go
type QueryConfig struct {
    BatchSize int
    MaxSeriesPerQuery int  // 0 means use default (100)
}
```

- [x] **Step 3: Commit**

```bash
git add pkg/types/config.go pkg/types/query_config.go
git commit -m "feat: add batch query configuration types"
```

---

## Task 2: Update Config Validator

**Files:**
- Modify: `internal/config/validator.go`

- [x] **Step 1: Add batch query validation**

Add validation for `query_mode` and `max_series_per_query`:

```go
// Validate query_mode
if cfg.QueryMode != "" && cfg.QueryMode != "single" && cfg.QueryMode != "batch" {
    v.errors = append(v.errors, fmt.Errorf(
        "influx_to_influx.query_mode must be 'single' or 'batch', got '%s'", cfg.QueryMode))
}

// Validate max_series_per_query
if cfg.MaxSeriesPerQuery < 0 {
    v.errors = append(v.errors, fmt.Errorf(
        "influx_to_influx.max_series_per_query must be non-negative"))
}
if cfg.MaxSeriesPerQuery > 1000 {
    cfg.MaxSeriesPerQuery = 1000 // cap at max
}
```

- [x] **Step 2: Run build to verify**

```bash
go build ./internal/config/...
```

- [x] **Step 3: Commit**

```bash
git add internal/config/validator.go
git commit -m "feat: validate batch query configuration"
```

---

## Task 3: Update SourceAdapter Interface

**Files:**
- Modify: `internal/adapter/adapter.go`

- [x] **Step 1: Add QueryDataBatch to interface**

```go
type SourceAdapter interface {
    // ... existing methods ...

    // NEW: Batch series query
    QueryDataBatch(ctx context.Context, measurement string, series []string,
                   lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error,
                   cfg *types.QueryConfig) (*types.Checkpoint, error)
}
```

- [x] **Step 2: Run build to verify**

```bash
go build ./internal/adapter/...
```

- [x] **Step 3: Commit**

```bash
git add internal/adapter/adapter.go
git commit -m "feat: add QueryDataBatch to SourceAdapter interface"
```

---

## Task 4: Implement QueryDataBatch for InfluxDB V1

**Files:**
- Modify: `internal/adapter/source/influxdb.go`

- [x] **Step 1: Add helper functions**

Add series parsing and WHERE clause building:

```go
// ParseSeriesKey parses "measurement,tag1=value1,tag2=value2" into components
func ParseSeriesKey(key string) (measurement string, tags map[string]string)

// BuildWhereClause builds "(tag1='v1' AND tag2='v2') OR (tag1='v3' AND tag2='v4')"
func BuildWhereClause(series []string) string
```

- [x] **Step 2: Implement QueryDataBatch**

```go
func (a *InfluxDBV1Adapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

    startTime := getStartTime(lastCheckpoint)
    batchSize := getBatchSize(cfg)

    whereClause := BuildWhereClause(series)
    query := fmt.Sprintf(`SELECT * FROM %s WHERE (%s) AND time >= '%s' LIMIT %d ORDER BY time`,
        influxQuoteIdentifier(measurement), whereClause, startTime, batchSize)

    records, err := a.executeSelectQuery(ctx, query)
    if err != nil {
        return nil, err
    }

    if len(records) > 0 {
        if err := batchFunc(records); err != nil {
            return nil, err
        }
    }

    maxTS := getMaxTimestamp(records)
    return &types.Checkpoint{
        LastTimestamp: maxTS,
        ProcessedRows: int64(len(records)),
    }, nil
}
```

- [x] **Step 3: Implement stub for V2 (NOT batch-capable yet)**

Add stub for OpenGemini source if needed.

- [x] **Step 4: Run build to verify**

```bash
go build ./internal/adapter/source/...
```

- [x] **Step 5: Commit**

```bash
git add internal/adapter/source/influxdb.go
git commit -m "feat: implement QueryDataBatch for InfluxDB V1"
```

---

## Task 5: Implement QueryDataBatch for InfluxDB V2

**Files:**
- Modify: `internal/adapter/source/influxdb.go`

- [x] **Step 1: Add BuildFluxFilter helper**

```go
// BuildFluxFilter builds Flux filter expression: (r.tag1 == "v1" and r.tag2 == "v2") or ...
func BuildFluxFilter(series []string) string
```

- [x] **Step 2: Implement QueryDataBatch for V2**

```go
func (a *InfluxDBV2Adapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

    startTime := getStartTime(lastCheckpoint)
    batchSize := getBatchSize(cfg)

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

    if len(records) > 0 {
        if err := batchFunc(records); err != nil {
            return nil, err
        }
    }

    maxTS := getMaxTimestamp(records)
    return &types.Checkpoint{
        LastTimestamp: maxTS,
        ProcessedRows: int64(len(records)),
    }, nil
}
```

- [x] **Step 3: Run build to verify**

```bash
go build ./internal/adapter/source/...
```

- [x] **Step 4: Commit**

```bash
git add internal/adapter/source/influxdb.go
git commit -m "feat: implement QueryDataBatch for InfluxDB V2"
```

---

## Task 6: Update Migration Engine for Batch Mode

**Files:**
- Modify: `internal/engine/migration.go`

- [x] **Step 1: Add series partitioning helper**

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

- [x] **Step 2: Add batch mode execution path**

```go
func (e *MigrationEngine) runTaskBatchMode(ctx context.Context, task *MigrationTask) error {
    // 1. Discover all series
    allSeries, err := e.discoverAllSeries(ctx, task)
    if err != nil {
        return err
    }

    // 2. Partition into batches
    batchSize := e.config.InfluxMigration.MaxSeriesPerQuery
    if batchSize <= 0 {
        batchSize = 100
    }
    batches := PartitionSeries(allSeries, batchSize)

    // 3. Process each batch
    for _, batch := range batches {
        checkpoint, err := e.sourceAdapter.QueryDataBatch(
            ctx, task.Mapping.SourceTable, batch, lastCheckpoint,
            func(records []types.Record) error {
                return e.processBatch(ctx, task.Mapping, records, task.TargetAdapter)
            }, queryCfg,
        )
        // ... handle checkpoint and errors
    }
}
```

- [x] **Step 3: Modify runTask to dispatch based on mode**

```go
func (e *MigrationEngine) runTask(ctx context.Context, task *MigrationTask) error {
    if e.config.InfluxMigration.QueryMode == "batch" {
        return e.runTaskBatchMode(ctx, task)
    }
    return e.runTaskSingleMode(ctx, task)
}
```

- [x] **Step 4: Run build to verify**

```bash
go build ./internal/engine/...
```

- [x] **Step 5: Commit**

```bash
git add internal/engine/migration.go
git commit -m "feat: add batch mode execution path to migration engine"
```

---

## Task 7: Update Mock Adapter for Tests

**Files:**
- Modify: `internal/adapter/registry_test.go`

- [x] **Step 1: Add QueryDataBatch mock**

```go
func (m *MockSourceAdapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
    return &types.Checkpoint{ProcessedRows: 100}, nil
}
```

- [x] **Step 2: Run tests**

```bash
go test ./internal/adapter/... -v
```

- [x] **Step 3: Commit**

```bash
git add internal/adapter/registry_test.go
git commit -m "test: add QueryDataBatch mock for tests"
```

---

## Task 8: Update Config Example

**Files:**
- Modify: `config.yaml.example`

- [x] **Step 1: Add batch query config example**

```yaml
influx_to_influx:
  enabled: true
  query_mode: "batch"           # "single" or "batch"
  max_series_per_query: 100    # default: 100, max: 1000
```

- [x] **Step 2: Commit**

```bash
git add config.yaml.example
git commit -m "docs: add batch query config example"
```

---

## Task 9: Integration Test

**Files:**
- Create: `internal/engine/migration_batch_test.go`

- [ ] **Step 1: Write integration test**

Test batch mode with mock InfluxDB server.

- [ ] **Step 2: Run tests**

```bash
go test ./internal/engine/... -v -run Batch
```

---

## Summary

| Task | Description | Files | Status |
|------|-------------|-------|--------|
| 1 | Batch Query Config Types | `pkg/types/config.go`, `query_config.go` | ✅ DONE |
| 2 | Config Validator | `internal/config/validator.go` | ✅ DONE |
| 3 | SourceAdapter Interface | `internal/adapter/adapter.go` | ✅ DONE |
| 4 | InfluxDB V1 QueryDataBatch | `internal/adapter/source/influxdb.go` | ✅ DONE |
| 5 | InfluxDB V2 QueryDataBatch | `internal/adapter/source/influxdb.go` | ✅ DONE |
| 6 | Migration Engine Batch Mode | `internal/engine/migration.go` | ✅ DONE |
| 7 | Mock Adapter for Tests | `internal/adapter/registry_test.go` | ✅ DONE |
| 8 | Config Example | `config.yaml.example` | ✅ DONE |
| 9 | Integration Test | `internal/engine/migration_batch_test.go` | ❌ NOT DONE |

## Verification

```bash
go build ./...     # Build succeeds
go test ./...     # All tests pass
```
