# InfluxDB Batch Series Query Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement batch series query mode that queries multiple InfluxDB series in a single request using OR拼接, reducing query count from N to N/max_series_per_query.

**Architecture:** Add `QueryDataBatch` interface method, new config section `influx_to_influx` with `query_mode` and `max_series_per_query`, and engine batch execution path. Only affects InfluxDB→InfluxDB and InfluxDB→OpenGemini migrations.

**Tech Stack:** Go, InfluxDB V1 SQL / V2 Flux, existing adapter pattern

---

## File Map

```
pkg/types/
├── config.go           # MODIFY: Add InfluxToInfluxConfig to MigrationConfig
├── query_config.go     # MODIFY: Add MaxSeriesPerQuery field

internal/adapter/
├── adapter.go          # MODIFY: Add QueryDataBatch to SourceAdapter interface
├── registry_test.go    # MODIFY: Add QueryDataBatch mock

internal/adapter/source/
├── influxdb.go         # MODIFY: Implement QueryDataBatch for V1 and V2

internal/engine/
├── migration.go       # MODIFY: Add batch mode execution path

internal/config/
├── validator.go       # MODIFY: Add batch query config validation

config.yaml.example    # MODIFY: Add batch query config example
```

---

## Task 1: Add InfluxToInfluxConfig Type

**Files:**
- Modify: `pkg/types/config.go:1-175`

- [ ] **Step 1: Read current MigrationConfig to find insertion point**

```bash
head -20 pkg/types/config.go
```

- [ ] **Step 2: Add InfluxToInfluxConfig struct after line 57**

After `type MigrationSettings struct { ... }`, add:

```go
type InfluxToInfluxConfig struct {
    Enabled           bool   `mapstructure:"enabled"`
    QueryMode         string `mapstructure:"query_mode"`   // "single" | "batch"
    MaxSeriesPerQuery int    `mapstructure:"max_series_per_query"`
}
```

- [ ] **Step 3: Add InfluxToInflux field to MigrationConfig**

In `MigrationConfig` struct (around line 10), add:

```go
type MigrationConfig struct {
    Global         GlobalConfig         `mapstructure:"global"`
    Logging        LoggingConfig        `mapstructure:"logging"`
    ConnectionPool ConnectionPoolConfig `mapstructure:"connection_pool"`
    RateLimit      RateLimitConfig      `mapstructure:"rate_limit"`
    Migration      MigrationSettings    `mapstructure:"migration"`
    Retry          RetryConfig          `mapstructure:"retry"`
    Incremental    IncrementalConfig    `mapstructure:"incremental"`
    Sources        []SourceConfig       `mapstructure:"sources"`
    Targets        []TargetConfig       `mapstructure:"targets"`
    Tasks          []TaskConfig         `mapstructure:"tasks"`
    InfluxToInflux InfluxToInfluxConfig `mapstructure:"influx_to_influx"` // NEW
}
```

- [ ] **Step 4: Run build to verify**

```bash
go build ./pkg/types/...
```

Expected: Build succeeds

- [ ] **Step 5: Commit**

```bash
git add pkg/types/config.go
git commit -m "feat: add InfluxToInfluxConfig type"
```

---

## Task 2: Add MaxSeriesPerQuery to QueryConfig

**Files:**
- Modify: `pkg/types/query_config.go:18-42`

- [ ] **Step 1: Read current QueryConfig**

```bash
cat pkg/types/query_config.go
```

- [ ] **Step 2: Add MaxSeriesPerQuery to QueryConfig struct**

Change from:
```go
type QueryConfig struct {
    BatchSize  int
    TimeWindow time.Duration
}
```

To:
```go
type QueryConfig struct {
    BatchSize  int
    TimeWindow time.Duration
    MaxSeriesPerQuery int  // 0 means use default (100)
}
```

- [ ] **Step 3: Add constants for MaxSeriesPerQuery**

Add after line 16:
```go
const (
    MinBatchSize     = 1
    MaxBatchSize     = 1000000
    DefaultBatchSize = 10000

    MinSeriesPerQuery     = 1
    MaxSeriesPerQuery     = 1000
    DefaultSeriesPerQuery = 100

    MinTimeWindow     = 1 * time.Hour
    MaxTimeWindow     = 30 * 24 * time.Hour
    DefaultTimeWindow = 168 * time.Hour
)
```

- [ ] **Step 4: Add validation for MaxSeriesPerQuery**

Update `Validate()` method:
```go
func (c *QueryConfig) Validate() error {
    if c.BatchSize < MinBatchSize || c.BatchSize > MaxBatchSize {
        return fmt.Errorf("batch_size must be between %d and %d, got %d", MinBatchSize, MaxBatchSize, c.BatchSize)
    }

    if c.TimeWindow < MinTimeWindow || c.TimeWindow > MaxTimeWindow {
        return fmt.Errorf("time_window must be between %v and %v, got %v", MinTimeWindow, MaxTimeWindow, c.TimeWindow)
    }

    if c.MaxSeriesPerQuery != 0 && (c.MaxSeriesPerQuery < MinSeriesPerQuery || c.MaxSeriesPerQuery > MaxSeriesPerQuery) {
        return fmt.Errorf("max_series_per_query must be between %d and %d, got %d", MinSeriesPerQuery, MaxSeriesPerQuery, c.MaxSeriesPerQuery)
    }

    return nil
}
```

- [ ] **Step 5: Update ApplyDefaults**

Update `ApplyDefaults()` method:
```go
func (c *QueryConfig) ApplyDefaults() {
    if c.BatchSize == 0 {
        c.BatchSize = DefaultBatchSize
    }
    if c.TimeWindow == 0 {
        c.TimeWindow = DefaultTimeWindow
    }
    if c.MaxSeriesPerQuery == 0 {
        c.MaxSeriesPerQuery = DefaultSeriesPerQuery
    }
}
```

- [ ] **Step 6: Run build to verify**

```bash
go build ./pkg/types/...
```

Expected: Build succeeds

- [ ] **Step 7: Commit**

```bash
git add pkg/types/query_config.go
git commit -m "feat: add MaxSeriesPerQuery to QueryConfig"
```

---

## Task 3: Update SourceAdapter Interface

**Files:**
- Modify: `internal/adapter/adapter.go:1-50`

- [ ] **Step 1: Read current adapter.go**

```bash
cat internal/adapter/adapter.go
```

- [ ] **Step 2: Add QueryDataBatch to SourceAdapter interface**

Find the `SourceAdapter` interface definition and add the new method:

```go
type SourceAdapter interface {
    Name() string
    Connect(ctx context.Context, config map[string]interface{}) error
    Disconnect(ctx context.Context) error
    Ping(ctx context.Context) error
    DiscoverTables(ctx context.Context) ([]string, error)
    DiscoverSeries(ctx context.Context, measurement string) ([]string, error)
    DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error)
    QueryData(ctx context.Context, measurement string, lastCheckpoint *types.Checkpoint,
              batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)

    // NEW: Batch series query
    QueryDataBatch(ctx context.Context, measurement string, series []string,
                   lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error,
                   cfg *types.QueryConfig) (*types.Checkpoint, error)
}
```

- [ ] **Step 3: Run build to verify**

```bash
go build ./internal/adapter/...
```

Expected: Build fails with "missing method QueryDataBatch" (expected, we will fix in Task 5)

- [ ] **Step 4: Commit**

```bash
git add internal/adapter/adapter.go
git commit -m "feat: add QueryDataBatch to SourceAdapter interface"
```

---

## Task 4: Implement QueryDataBatch Helper Functions

**Files:**
- Modify: `internal/adapter/source/influxdb.go`

- [ ] **Step 1: Add helper functions at end of influxdb.go (before closing package bracket)**

Add these helper functions:

```go
// ParseSeriesKey parses "measurement,tag1=value1,tag2=value2" into components
func ParseSeriesKey(key string) (tags map[string]string) {
    tags = make(map[string]string)
    parts := strings.Split(key, ",")
    // First part is measurement, skip it
    for _, part := range parts[1:] {
        kv := strings.SplitN(part, "=", 2)
        if len(kv) == 2 {
            tags[kv[0]] = kv[1]
        }
    }
    return
}

// BuildWhereClause builds "(tag1='v1' AND tag2='v2') OR (tag1='v3' AND tag2='v4')"
func BuildWhereClause(series []string) string {
    var conditions []string
    for _, s := range series {
        tags := ParseSeriesKey(s)
        var tagConditions []string
        for k, v := range tags {
            tagConditions = append(tagConditions, fmt.Sprintf("%s='%s'", influxQuoteIdentifier(k), v))
        }
        if len(tagConditions) > 0 {
            conditions = append(conditions, "("+strings.Join(tagConditions, " AND ")+")")
        }
    }
    return strings.Join(conditions, " OR ")
}

// BuildFluxFilter builds Flux filter expression: (r.tag1 == "v1" and r.tag2 == "v2") or ...
func BuildFluxFilter(series []string) string {
    var conditions []string
    for _, s := range series {
        tags := ParseSeriesKey(s)
        var tagConditions []string
        for k, v := range tags {
            tagConditions = append(tagConditions, fmt.Sprintf(`r.%s == "%s"`, influxQuoteIdentifier(k), v))
        }
        if len(tagConditions) > 0 {
            conditions = append(conditions, "("+strings.Join(tagConditions, " and ")+")")
        }
    }
    return strings.Join(conditions, " or ")
}

// getBatchSize returns batch size from config, defaulting to DefaultBatchSize
func getBatchSize(cfg *types.QueryConfig) int {
    batchSize := types.DefaultBatchSize
    if cfg != nil && cfg.BatchSize > 0 {
        batchSize = cfg.BatchSize
    }
    return batchSize
}

// getMaxSeriesPerQuery returns max series per query from config, defaulting to DefaultSeriesPerQuery
func getMaxSeriesPerQuery(cfg *types.QueryConfig) int {
    maxSeries := types.DefaultSeriesPerQuery
    if cfg != nil && cfg.MaxSeriesPerQuery > 0 {
        maxSeries = cfg.MaxSeriesPerQuery
    }
    return maxSeries
}
```

- [ ] **Step 2: Run build to verify helpers compile**

```bash
go build ./internal/adapter/source/...
```

Expected: Build succeeds (no error for helpers)

- [ ] **Step 3: Commit**

```bash
git add internal/adapter/source/influxdb.go
git commit -m "feat: add ParseSeriesKey, BuildWhereClause, BuildFluxFilter helpers"
```

---

## Task 5: Implement QueryDataBatch for InfluxDB V1

**Files:**
- Modify: `internal/adapter/source/influxdb.go`

- [ ] **Step 1: Find where InfluxDBV1Adapter QueryData method ends (around line 257)**

```bash
grep -n "func (a \*InfluxDBV1Adapter) QueryData" internal/adapter/source/influxdb.go
```

- [ ] **Step 2: Add QueryDataBatch method for InfluxDBV1Adapter after QueryData method**

Add this method before `executeSelectQuery`:

```go
func (a *InfluxDBV1Adapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

    var lastTS int64
    if lastCheckpoint != nil {
        lastTS = lastCheckpoint.LastTimestamp
    }

    var startTime string
    if lastTS == 0 {
        startTime = "1970-01-01T00:00:00Z"
    } else {
        startTime = time.Unix(0, lastTS).Format(time.RFC3339Nano)
    }

    batchSize := getBatchSize(cfg)

    whereClause := BuildWhereClause(series)
    query := fmt.Sprintf(`SELECT * FROM %s WHERE (%s) AND time >= '%s' LIMIT %d ORDER BY time`,
        influxQuoteIdentifier(measurement), whereClause, startTime, batchSize)

    logger.Debug("executing batch query for InfluxDB V1",
        zap.String("measurement", measurement),
        zap.Int("series_count", len(series)),
        zap.String("query", query))

    records, err := a.executeSelectQuery(ctx, query)
    if err != nil {
        return nil, fmt.Errorf("batch query failed: %w", err)
    }

    if len(records) > 0 {
        if err := batchFunc(records); err != nil {
            return nil, fmt.Errorf("batch func failed: %w", err)
        }
    }

    var maxTS int64
    for _, record := range records {
        if record.Time > maxTS {
            maxTS = record.Time
        }
    }

    logger.Info("completed batch query for InfluxDB V1",
        zap.String("measurement", measurement),
        zap.Int("series_count", len(series)),
        zap.Int("records", len(records)),
        zap.Int64("max_timestamp", maxTS))

    return &types.Checkpoint{
        LastTimestamp: maxTS,
        ProcessedRows: int64(len(records)),
    }, nil
}
```

- [ ] **Step 3: Run build to verify**

```bash
go build ./internal/adapter/source/...
```

Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add internal/adapter/source/influxdb.go
git commit -m "feat: implement QueryDataBatch for InfluxDB V1"
```

---

## Task 6: Implement QueryDataBatch for InfluxDB V2

**Files:**
- Modify: `internal/adapter/source/influxdb.go`

- [ ] **Step 1: Find where InfluxDBV2Adapter QueryData method ends (around line 653)**

```bash
grep -n "func (a \*InfluxDBV2Adapter) QueryData" internal/adapter/source/influxdb.go
```

- [ ] **Step 2: Add QueryDataBatch method for InfluxDBV2Adapter after V2 QueryData method**

Add this method before `executeFluxSelect`:

```go
func (a *InfluxDBV2Adapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

    var startTime string
    var lastTS int64

    if lastCheckpoint != nil && lastCheckpoint.LastTimestamp != 0 {
        startTime = fmt.Sprintf("%d", lastCheckpoint.LastTimestamp)
        lastTS = lastCheckpoint.LastTimestamp
    } else {
        startTime = "1970-01-01T00:00:00Z"
    }

    batchSize := getBatchSize(cfg)

    fluxFilter := BuildFluxFilter(series)
    fluxQuery := fmt.Sprintf(`
        from(bucket: "%s")
          |> range(start: %s)
          |> filter(fn: (r) => r._measurement == "%s" and (%s))
          |> limit(n: %d)
    `, a.config.Bucket, startTime, measurement, fluxFilter, batchSize)

    logger.Debug("executing batch query for InfluxDB V2",
        zap.String("measurement", measurement),
        zap.Int("series_count", len(series)),
        zap.String("query", fluxQuery))

    records, err := a.executeFluxSelect(ctx, fluxQuery)
    if err != nil {
        return nil, fmt.Errorf("batch query failed: %w", err)
    }

    if len(records) > 0 {
        if err := batchFunc(records); err != nil {
            return nil, fmt.Errorf("batch func failed: %w", err)
        }
    }

    var maxTS int64
    for _, record := range records {
        if record.Time > maxTS {
            maxTS = record.Time
        }
    }

    logger.Info("completed batch query for InfluxDB V2",
        zap.String("measurement", measurement),
        zap.Int("series_count", len(series)),
        zap.Int("records", len(records)),
        zap.Int64("max_timestamp", maxTS))

    return &types.Checkpoint{
        LastTimestamp: maxTS,
        ProcessedRows: int64(len(records)),
    }, nil
}
```

- [ ] **Step 3: Run build to verify**

```bash
go build ./internal/adapter/source/...
```

Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add internal/adapter/source/influxdb.go
git commit -m "feat: implement QueryDataBatch for InfluxDB V2"
```

---

## Task 7: Add Mock QueryDataBatch for Tests

**Files:**
- Modify: `internal/adapter/registry_test.go`

- [ ] **Step 1: Read registry_test.go to find MockSourceAdapter**

```bash
cat internal/adapter/registry_test.go
```

- [ ] **Step 2: Add QueryDataBatch method to MockSourceAdapter**

Find the `MockSourceAdapter` struct and add:

```go
func (m *MockSourceAdapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
    if batchFunc != nil && len(series) > 0 {
        // Return mock records
        mockRecords := make([]types.Record, 10)
        for i := range mockRecords {
            mockRecords[i] = *types.NewRecord()
            mockRecords[i].Time = time.Now().UnixNano()
            mockRecords[i].AddField("mock_field", float64(i))
        }
        if err := batchFunc(mockRecords); err != nil {
            return nil, err
        }
    }
    return &types.Checkpoint{
        LastTimestamp: time.Now().UnixNano(),
        ProcessedRows:  int64(len(series) * 10),
    }, nil
}
```

- [ ] **Step 3: Run tests to verify**

```bash
go test ./internal/adapter/... -v -run Mock
```

Expected: Tests pass

- [ ] **Step 4: Commit**

```bash
git add internal/adapter/registry_test.go
git commit -m "test: add QueryDataBatch mock for tests"
```

---

## Task 8: Update Config Validator

**Files:**
- Modify: `internal/config/validator.go`

- [ ] **Step 1: Read validator.go to find where to add validation**

```bash
grep -n "func.*Validate" internal/config/validator.go | head -5
```

- [ ] **Step 2: Find the validation function that handles MigrationConfig**

```bash
grep -n "Migration\|Influx" internal/config/validator.go
```

- [ ] **Step 3: Add validation for InfluxToInfluxConfig**

Find the validation function that processes `MigrationConfig` and add:

```go
// Validate InfluxToInflux config
if cfg.InfluxToInflux.Enabled {
    if cfg.InfluxToInflux.QueryMode != "" &&
       cfg.InfluxToInflux.QueryMode != "single" &&
       cfg.InfluxToInflux.QueryMode != "batch" {
        v.errors = append(v.errors, fmt.Errorf(
            "influx_to_influx.query_mode must be 'single' or 'batch', got '%s'",
            cfg.InfluxToInflux.QueryMode))
    }

    if cfg.InfluxToInflux.MaxSeriesPerQuery < 0 {
        v.errors = append(v.errors, fmt.Errorf(
            "influx_to_influx.max_series_per_query must be non-negative, got %d",
            cfg.InfluxToInflux.MaxSeriesPerQuery))
    }

    if cfg.InfluxToInflux.MaxSeriesPerQuery > 1000 {
        cfg.InfluxToInflux.MaxSeriesPerQuery = 1000
    }
}
```

- [ ] **Step 4: Run build to verify**

```bash
go build ./internal/config/...
```

Expected: Build succeeds

- [ ] **Step 5: Commit**

```bash
git add internal/config/validator.go
git commit -m "feat: validate InfluxToInflux batch query configuration"
```

---

## Task 9: Update Migration Engine for Batch Mode

**Files:**
- Modify: `internal/engine/migration.go`

- [ ] **Step 1: Read migration.go to understand runTask structure**

```bash
grep -n "func (e \*MigrationEngine) runTask" internal/engine/migration.go
```

- [ ] **Step 2: Add PartitionSeries helper function**

Add this function to migration.go (after imports or near other helper functions):

```go
// PartitionSeries splits a slice of series into batches of maxPerBatch size
func PartitionSeries(series []string, maxPerBatch int) [][]string {
    if maxPerBatch <= 0 {
        maxPerBatch = 100
    }
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

- [ ] **Step 3: Find where runTask is defined and add batch mode dispatch**

Modify `runTask` to dispatch based on query mode:

```go
func (e *MigrationEngine) runTask(ctx context.Context, task *MigrationTask) error {
    // Check if batch mode is enabled for InfluxToInflux
    if e.config.InfluxToInflux.Enabled && e.config.InfluxToInflux.QueryMode == "batch" {
        return e.runTaskBatchMode(ctx, task)
    }
    return e.runTaskSingleMode(ctx, task)
}
```

- [ ] **Step 4: Add runTaskBatchMode method**

Add this method after runTask:

```go
func (e *MigrationEngine) runTaskBatchMode(ctx context.Context, task *MigrationTask) error {
    logger.Info("starting batch mode task",
        zap.String("task_id", task.ID),
        zap.String("source_table", task.Mapping.SourceTable),
        zap.String("target_measurement", task.Mapping.TargetMeasurement))

    // Discover all series for the measurement
    series, err := e.sourceAdapter.DiscoverSeries(ctx, task.Mapping.SourceTable)
    if err != nil {
        logger.Warn("DiscoverSeries not supported or failed, treating as single table",
            zap.String("table", task.Mapping.SourceTable),
            zap.Error(err))
        series = []string{task.Mapping.SourceTable}
    }

    // Apply tag filters if any
    if len(task.Mapping.TagFilters) > 0 {
        filteredSeries := make([]string, 0)
        for _, s := range series {
            if e.matchTagFilters(s, task.Mapping.TagFilters) {
                filteredSeries = append(filteredSeries, s)
            }
        }
        series = filteredSeries
    }

    if len(series) == 0 {
        logger.Info("no series to migrate in batch mode",
            zap.String("task_id", task.ID))
        return nil
    }

    // Partition into batches
    batchSize := e.config.InfluxToInflux.MaxSeriesPerQuery
    if batchSize <= 0 {
        batchSize = 100
    }
    batches := PartitionSeries(series, batchSize)

    logger.Info("batch mode: partitioned series into batches",
        zap.String("task_id", task.ID),
        zap.Int("total_series", len(series)),
        zap.Int("batch_size", batchSize),
        zap.Int("total_batches", len(batches)))

    // Process each batch
    queryCfg := &types.QueryConfig{
        BatchSize:        e.config.Migration.ChunkSize,
        MaxSeriesPerQuery: batchSize,
    }

    var lastCheckpoint *types.Checkpoint
    for i, batch := range batches {
        logger.Debug("processing batch",
            zap.String("task_id", task.ID),
            zap.Int("batch_index", i+1),
            zap.Int("batch_size", len(batch)))

        checkpoint, err := e.sourceAdapter.QueryDataBatch(
            ctx,
            task.Mapping.SourceTable,
            batch,
            lastCheckpoint,
            func(records []types.Record) error {
                if len(records) == 0 {
                    return nil
                }
                return e.processBatch(ctx, task.Mapping, records, task.TargetAdapter)
            },
            queryCfg,
        )

        if err != nil {
            return fmt.Errorf("batch %d/%d failed: %w", i+1, len(batches), err)
        }

        lastCheckpoint = checkpoint
    }

    logger.Info("completed batch mode task",
        zap.String("task_id", task.ID),
        zap.Int("total_batches", len(batches)))

    return nil
}
```

- [ ] **Step 5: Run build to verify**

```bash
go build ./internal/engine/...
```

Expected: Build succeeds

- [ ] **Step 6: Commit**

```bash
git add internal/engine/migration.go
git commit -m "feat: add batch mode execution path to migration engine"
```

---

## Task 10: Update Config Example

**Files:**
- Modify: `config.yaml.example`

- [ ] **Step 1: Read config.yaml.example to find where to add**

```bash
cat config.yaml.example
```

- [ ] **Step 2: Add influx_to_influx section**

Add after the migration section:

```yaml
# InfluxDB to InfluxDB / OpenGemini batch query mode
influx_to_influx:
  enabled: true
  query_mode: "batch"           # "single" or "batch"
  max_series_per_query: 100     # default: 100, max: 1000
```

- [ ] **Step 3: Commit**

```bash
git add config.yaml.example
git commit -m "docs: add influx_to_influx batch query config example"
```

---

## Task 11: Write Unit Tests for Helpers

**Files:**
- Create: `internal/adapter/source/influxdb_batch_test.go`

- [ ] **Step 1: Write tests for ParseSeriesKey**

```go
package source

import "testing"

func TestParseSeriesKey(t *testing.T) {
    tests := []struct {
        name     string
        key      string
        expected map[string]string
    }{
        {
            name: "single tag",
            key:  "cpu,host=server1",
            expected: map[string]string{"host": "server1"},
        },
        {
            name: "multiple tags",
            key:  "cpu,host=server1,region=us",
            expected: map[string]string{"host": "server1", "region": "us"},
        },
        {
            name: "no tags",
            key:  "cpu",
            expected: map[string]string{},
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := ParseSeriesKey(tt.key)
            if len(result) != len(tt.expected) {
                t.Errorf("expected %d tags, got %d", len(tt.expected), len(result))
            }
            for k, v := range tt.expected {
                if result[k] != v {
                    t.Errorf("expected %s=%s, got %s=%s", k, v, k, result[k])
                }
            }
        })
    }
}
```

- [ ] **Step 2: Write tests for BuildWhereClause**

```go
func TestBuildWhereClause(t *testing.T) {
    tests := []struct {
        name     string
        series   []string
        contains string
    }{
        {
            name:     "single series",
            series:   []string{"cpu,host=server1"},
            contains: "host='server1'",
        },
        {
            name:     "multiple series",
            series:   []string{"cpu,host=server1", "cpu,host=server2"},
            contains: " OR ",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := BuildWhereClause(tt.series)
            if tt.contains != "" && !contains(result, tt.contains) {
                t.Errorf("expected result to contain %q, got %q", tt.contains, result)
            }
        })
    }
}
```

- [ ] **Step 3: Write tests for PartitionSeries**

```go
func TestPartitionSeries(t *testing.T) {
    tests := []struct {
        name       string
        series     []string
        maxPerBatch int
        expectedBatches int
        expectedLastLen int
    }{
        {
            name:       "exact batches",
            series:     []string{"s1", "s2", "s3", "s4"},
            maxPerBatch: 2,
            expectedBatches: 2,
            expectedLastLen: 2,
        },
        {
            name:       "partial last batch",
            series:     []string{"s1", "s2", "s3"},
            maxPerBatch: 2,
            expectedBatches: 2,
            expectedLastLen: 1,
        },
        {
            name:       "smaller than batch",
            series:     []string{"s1", "s2"},
            maxPerBatch: 10,
            expectedBatches: 1,
            expectedLastLen: 2,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result := PartitionSeries(tt.series, tt.maxPerBatch)
            if len(result) != tt.expectedBatches {
                t.Errorf("expected %d batches, got %d", tt.expectedBatches, len(result))
            }
            lastBatchLen := len(result[len(result)-1])
            if lastBatchLen != tt.expectedLastLen {
                t.Errorf("expected last batch length %d, got %d", tt.expectedLastLen, lastBatchLen)
            }
        })
    }
}
```

- [ ] **Step 4: Run tests**

```bash
go test ./internal/adapter/source/... -v -run "Parse|BuildWhere|Partition"
```

Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add internal/adapter/source/influxdb_batch_test.go
git commit -m "test: add unit tests for batch query helpers"
```

---

## Summary

| Task | Description | Files | Status |
|------|-------------|-------|--------|
| 1 | InfluxToInfluxConfig Type | `pkg/types/config.go` | - [ ] |
| 2 | MaxSeriesPerQuery in QueryConfig | `pkg/types/query_config.go` | - [ ] |
| 3 | QueryDataBatch Interface | `internal/adapter/adapter.go` | - [ ] |
| 4 | Helper Functions | `internal/adapter/source/influxdb.go` | - [ ] |
| 5 | InfluxDB V1 QueryDataBatch | `internal/adapter/source/influxdb.go` | - [ ] |
| 6 | InfluxDB V2 QueryDataBatch | `internal/adapter/source/influxdb.go` | - [ ] |
| 7 | Mock for Tests | `internal/adapter/registry_test.go` | - [ ] |
| 8 | Config Validator | `internal/config/validator.go` | - [ ] |
| 9 | Engine Batch Mode | `internal/engine/migration.go` | - [ ] |
| 10 | Config Example | `config.yaml.example` | - [ ] |
| 11 | Unit Tests | `internal/adapter/source/influxdb_batch_test.go` | - [ ] |
