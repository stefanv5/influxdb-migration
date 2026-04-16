# Design: Shard Group-Aware Migration for InfluxDB

## 1. Overview

This design specifies a shard group-aware migration optimization that:

1. Queries InfluxDB for shard group metadata to understand time boundaries
2. Discovers series **per shard group per time window** (not all at once)
3. Uses time window-based splitting to avoid OOM without using LIMIT/OFFSET
4. Tracks checkpoint **per shard group and time window** for precise crash recovery

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Shard Group-Aware Migration Flow                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. Discover Shard Groups                                                │
│     └── ShowShardGroups() → [SG1, SG2, SG3, ...]                        │
│              │                                                           │
│              ▼                                                           │
│  2. For each Shard Group (filtered by query time range):                │
│     │                                                                    │
│     ├── 2a. Calculate effective time range                                │
│     │       └── SG.time_range ∩ query.time_range                        │
│     │                                                                    │
│     ├── 2b. Split into time windows (configurable, default=SG length)    │
│     │       └── Window 1: [start~start+tw), Window 2: [start+tw~...),   │
│     │                                                                    │
│     └── 3. For each Time Window:                                         │
│         │                                                                │
│         ├── 3a. Discover Series (time-range limited)                      │
│         │       └── SHOW SERIES WHERE time >= window_start               │
│         │           AND time < window_end                                │
│         │       → Returns series with data in this window (bounded)     │
│         │                                                                │
│         ├── 3b. Partition into batches (in memory)                        │
│         │       └── Partition(series, max_series_per_query)              │
│         │                                                                │
│         ├── 3c. For each batch:                                          │
│         │       ├── QueryDataBatch(series_batch, startTime, endTime)    │
│         │       │       └── WHERE (series_tags) AND time >= start      │
│         │       │           AND time < endTime                          │
│         │       │                                                        │
│         │       ├── WriteBatch to Target                                  │
│         │       │                                                        │
│         │       └── SaveWindowCheckpoint(window_idx, batch_idx, ts)      │
│         │                                                                │
│         └── 3d. Mark window completed                                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## 3. Configuration Schema

### 3.1 Config Structure

```go
// pkg/types/config.go
type InfluxMigrationConfig struct {
    Enabled           bool                      `mapstructure:"enabled"`
    QueryMode         string                    `mapstructure:"query_mode"`   // "single" | "batch" | "shard-group"
    MaxSeriesPerQuery int                       `mapstructure:"max_series_per_query"`
    ShardGroupConfig  *ShardGroupConfig         `mapstructure:"shard_group_config"`
}

type ShardGroupConfig struct {
    Enabled          bool           `mapstructure:"enabled"`
    SeriesBatchSize int            `mapstructure:"series_batch_size"`   // default: 50
    ShardParallelism int           `mapstructure:"shard_parallelism"`    // default: 1
    TimeWindow       time.Duration `mapstructure:"time_window"`          // default: 0 (use shard group length)
}
```

### 3.2 Config File Example

```yaml
influx_to_influx:
  enabled: true
  query_mode: "shard-group"        # "single" | "batch" | "shard-group"
  max_series_per_query: 100
  shard_group_config:
    enabled: true
    series_batch_size: 50           # series per batch within time window
    shard_parallelism: 1            # process shard groups sequentially
    time_window: 0                  # 0 = use shard group length; e.g., 168h for weekly
```

## 4. Interface Changes

### 4.1 SourceAdapter Interface - New Methods

```go
// internal/adapter/adapter.go
type SourceAdapter interface {
    // ... existing methods ...

    // NEW: Get shard group information
    DiscoverShardGroups(ctx context.Context) ([]*ShardGroup, error)

    // NEW: Discover series within a time window (returns all series at once)
    DiscoverSeriesInTimeWindow(ctx context.Context, measurement string,
        startTime, endTime time.Time) ([]string, error)

    // MODIFIED: QueryDataBatch now requires startTime and endTime bounds
    QueryDataBatch(ctx context.Context, measurement string, series []string,
        startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
        batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)
}

// ShardGroup represents a time-bounded shard group
type ShardGroup struct {
    ID        int
    StartTime time.Time
    EndTime   time.Time
}
```

### 4.2 Checkpoint - New Type

```go
// pkg/types/checkpoint.go
type CheckpointType string

const (
    CheckpointTypeTask        CheckpointType = "task"
    CheckpointTypeShardGroup  CheckpointType = "shard_group"
)

// ShardGroupCheckpoint tracks migration progress per shard group and time window
type ShardGroupCheckpoint struct {
    ShardGroupID        string           `json:"shard_group_id"`
    WindowStart         int64            `json:"window_start"`          // Unix nano - start of time window
    WindowEnd           int64            `json:"window_end"`            // Unix nano - end of time window
    LastCompletedBatch  int              `json:"last_completed_batch"`  // 0-indexed batch within window
    LastTimestamp       int64            `json:"last_timestamp"`        // max timestamp of last batch
    TotalProcessedRows  int64            `json:"total_processed_rows"`
    Status              CheckpointStatus `json:"status"`
}

type Checkpoint struct {
    // ... existing fields ...

    CheckpointType CheckpointType `json:"checkpoint_type"` // "task" | "shard_group"

    // For shard group checkpoint
    ShardGroupCheckpoint *ShardGroupCheckpoint `json:"shard_group_checkpoint,omitempty"`
}
```

## 5. Implementation Details

### 5.1 InfluxDB V1 - DiscoverShardGroups

```sql
SHOW SHARDS
-- Returns: id, database, retention_policy, shard_group, start_time, end_time, owners
```

Parse results to derive shard groups (group by shard_group ID):

```go
func (a *InfluxDBV1Adapter) DiscoverShardGroups(ctx context.Context) ([]*ShardGroup, error) {
    query := "SHOW SHARDS"
    results, err := a.executeQuery(ctx, query)
    if err != nil {
        return nil, err
    }

    // Group by shard_group ID, extract time range
    shardGroups := make(map[int]*ShardGroup)
    for _, result := range results {
        for _, values := range result.Values {
            if len(values) < 6 {
                continue
            }
            // Parse: id, database, retention_policy, shard_group, start_time, end_time
            shardGroupID := parseInt(values[3])
            startTime := parseTime(values[4])
            endTime := parseTime(values[5])

            if _, exists := shardGroups[shardGroupID]; !exists {
                shardGroups[shardGroupID] = &ShardGroup{
                    ID: shardGroupID,
                    StartTime: startTime,
                    EndTime: endTime,
                }
            }
        }
    }

    var result []*ShardGroup
    for _, sg := range shardGroups {
        result = append(result, sg)
    }
    sort.Slice(result, func(i, j int) bool {
        return result[i].StartTime.Before(result[j].StartTime)
    })
    return result, nil
}
```

### 5.2 InfluxDB V2 - DiscoverShardGroups

```go
// Use the shards API endpoint
func (a *InfluxDBV2Adapter) DiscoverShardGroups(ctx context.Context) ([]*ShardGroup, error) {
    req, _ := http.NewRequestWithContext(ctx, "GET", a.config.URL+"/api/v2/shards", nil)
    req.Header.Set("Authorization", "Token "+a.config.Token)

    resp, err := a.client.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    var result struct {
        Shards []struct {
            ID        int    `json:"id"`
            StartTime int64  `json:"startTime"`
            EndTime   int64  `json:"endTime"`
        } `json:"shards"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return nil, err
    }

    var shardGroups []*ShardGroup
    for _, s := range result.Shards {
        shardGroups = append(shardGroups, &ShardGroup{
            ID: s.ID,
            StartTime: time.Unix(0, s.StartTime),
            EndTime: time.Unix(0, s.EndTime),
        })
    }
    return shardGroups, nil
}
```

### 5.3 Series Discovery with Time Window

```go
// DiscoverSeriesInTimeWindow returns all series that have data in the given time window
// No LIMIT/OFFSET - relies on time window to bound the result set
func (a *InfluxDBV1Adapter) DiscoverSeriesInTimeWindow(ctx context.Context,
    measurement string, startTime, endTime time.Time) ([]string, error) {

    query := fmt.Sprintf(`
        SHOW SERIES FROM %s
        WHERE time >= '%s' AND time < '%s'`,
        influxQuoteIdentifier(measurement),
        startTime.Format(time.RFC3339Nano),
        endTime.Format(time.RFC3339Nano))

    series, err := a.executeShowSeries(ctx, query)
    if err != nil {
        return nil, err
    }

    return series, nil
}

// For InfluxDB V2, use SHOW SERIES CARDINALITY or similar approach
func (a *InfluxDBV2Adapter) DiscoverSeriesInTimeWindow(ctx context.Context,
    measurement string, startTime, endTime time.Time) ([]string, error) {
    // InfluxDB 2.x: Use SHOW SERIES with time filter
    // Note: V2 may not support time filter on SHOW SERIES directly
    // Alternative: query _series bucket or use tag values approach
    // This needs verification with actual InfluxDB 2.x implementation
    ...
}
```

### 5.4 Time Window Splitting

```go
// SplitTimeWindows splits a time range into windows of the given duration
func SplitTimeWindows(start, end time.Time, windowDuration time.Duration) []TimeWindow {
    var windows []TimeWindow
    for windowStart := start; windowStart.Before(end); {
        windowEnd := windowStart.Add(windowDuration)
        if windowEnd.After(end) {
            windowEnd = end
        }
        windows = append(windows, TimeWindow{
            Start: windowStart,
            End:   windowEnd,
        })
        windowStart = windowEnd
    }
    return windows
}

type TimeWindow struct {
    Start time.Time
    End   time.Time
}
```

### 5.5 Time Range Intersection

```go
// EffectiveTimeRange returns the intersection of shard group and query time range
func (sg *ShardGroup) EffectiveTimeRange(queryStart, queryEnd time.Time) (start, end time.Time) {
    start = queryStart
    end = queryEnd

    if start.Before(sg.StartTime) {
        start = sg.StartTime
    }
    if end.After(sg.EndTime) {
        end = sg.EndTime
    }
    return start, end
}

// Overlaps returns true if shard group overlaps with query range
func (sg *ShardGroup) Overlaps(queryStart, queryEnd time.Time) bool {
    return !queryEnd.Before(sg.StartTime) && !queryStart.After(sg.EndTime)
}
```

### 5.6 Migration Engine - Shard Group Mode

```go
func (e *MigrationEngine) runTaskShardGroupMode(ctx context.Context, task *MigrationTask) error {
    // 1. Discover shard groups
    shardGroups, err := e.sourceAdapter.DiscoverShardGroups(ctx)
    if err != nil {
        return fmt.Errorf("failed to discover shard groups: %w", err)
    }

    // 2. Filter shard groups by query time range (if specified)
    queryStart := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
    queryEnd := time.Now()
    if task.Mapping.TimeRange.Start != "" {
        queryStart, _ = time.Parse(time.RFC3339, task.Mapping.TimeRange.Start)
    }
    if task.Mapping.TimeRange.End != "" {
        queryEnd, _ = time.Parse(time.RFC3339, task.Mapping.TimeRange.End)
    }

    var relevantGroups []*ShardGroup
    for _, sg := range shardGroups {
        if sg.Overlaps(queryStart, queryEnd) {
            relevantGroups = append(relevantGroups, sg)
        }
    }

    if len(relevantGroups) == 0 {
        logger.Info("no relevant shard groups found",
            zap.String("task_id", task.ID))
        return nil
    }

    // 3. Process each shard group
    for _, sg := range relevantGroups {
        if err := e.migrateShardGroup(ctx, task, sg, queryStart, queryEnd); err != nil {
            return fmt.Errorf("shard group %d migration failed: %w", sg.ID, err)
        }
    }

    return nil
}

func (e *MigrationEngine) migrateShardGroup(ctx context.Context,
    task *MigrationTask, sg *ShardGroup, queryStart, queryEnd time.Time) error {

    // Calculate effective time range
    start, end := sg.EffectiveTimeRange(queryStart, queryEnd)

    logger.Info("migrating shard group",
        zap.Int("shard_id", sg.ID),
        zap.String("start", start.Format(time.RFC3339)),
        zap.String("end", end.Format(time.RFC3339)))

    // Determine time window duration
    timeWindow := e.config.InfluxToInflux.ShardGroupConfig.TimeWindow
    if timeWindow == 0 {
        // Use shard group length as default
        timeWindow = end.Sub(start)
    }

    // Split into time windows
    windows := SplitTimeWindows(start, end, timeWindow)

    // Process each time window
    for _, window := range windows {
        if err := e.migrateTimeWindow(ctx, task, sg, window); err != nil {
            return fmt.Errorf("time window [%s, %s) migration failed: %w",
                window.Start.Format(time.RFC3339), window.End.Format(time.RFC3339), err)
        }
    }

    // Mark shard group as completed
    if err := e.checkpointMgr.MarkShardGroupCompleted(ctx, task.ID, sg.ID); err != nil {
        logger.Warn("failed to mark shard group completed", zap.Error(err))
    }

    logger.Info("shard group migration completed",
        zap.Int("shard_id", sg.ID))

    return nil
}

func (e *MigrationEngine) migrateTimeWindow(ctx context.Context,
    task *MigrationTask, sg *ShardGroup, window TimeWindow) error {

    // Load existing checkpoint for this window
    cp, err := e.checkpointMgr.LoadShardGroupCheckpoint(ctx, task.ID, sg.ID)
    if err != nil {
        return fmt.Errorf("failed to load shard group checkpoint: %w", err)
    }

    var lastBatchIdx int
    var lastTimestamp int64
    if cp != nil && cp.Status == types.StatusInProgress {
        // Check if checkpoint is for this window
        if cp.WindowStart == window.Start.UnixNano() && cp.WindowEnd == window.End.UnixNano() {
            lastBatchIdx = cp.LastCompletedBatch
            lastTimestamp = cp.LastTimestamp
        }
    }

    // Discover series in this time window (bounded by window, no LIMIT/OFFSET)
    series, err := e.sourceAdapter.DiscoverSeriesInTimeWindow(ctx,
        task.Mapping.SourceTable, window.Start, window.End)
    if err != nil {
        return fmt.Errorf("failed to discover series: %w", err)
    }

    if len(series) == 0 {
        logger.Debug("no series found in time window",
            zap.Int("shard_id", sg.ID),
            zap.String("window_start", window.Start.Format(time.RFC3339)))
        return nil
    }

    // Partition into batches
    batchSize := e.config.InfluxToInflux.MaxSeriesPerQuery
    if batchSize <= 0 {
        batchSize = 100
    }
    batches := PartitionSeries(series, batchSize)

    logger.Info("processing time window",
        zap.Int("shard_id", sg.ID),
        zap.String("window_start", window.Start.Format(time.RFC3339)),
        zap.String("window_end", window.End.Format(time.RFC3339)),
        zap.Int("total_series", len(series)),
        zap.Int("total_batches", len(batches)))

    // Process each batch
    for batchIdx, batch := range batches {
        if batchIdx <= lastBatchIdx {
            logger.Debug("skipping completed batch",
                zap.Int("batch_idx", batchIdx),
                zap.Int("last_completed", lastBatchIdx))
            continue
        }

        // Query data for this batch with time bounds
        checkpoint, err := e.sourceAdapter.QueryDataBatch(ctx,
            task.Mapping.SourceTable,
            batch,
            window.Start,
            window.End,
            &types.Checkpoint{LastTimestamp: lastTimestamp},
            func(records []types.Record) error {
                return e.processBatch(ctx, task.Mapping, records, task.TargetAdapter)
            },
            &types.QueryConfig{BatchSize: e.config.Migration.ChunkSize},
        )
        if err != nil {
            return fmt.Errorf("batch %d failed: %w", batchIdx, err)
        }

        if checkpoint != nil {
            lastTimestamp = checkpoint.LastTimestamp
        }

        // Save checkpoint after each batch (at-least-once)
        sgCP := &types.ShardGroupCheckpoint{
            ShardGroupID:        fmt.Sprintf("%d", sg.ID),
            WindowStart:         window.Start.UnixNano(),
            WindowEnd:           window.End.UnixNano(),
            LastCompletedBatch:  batchIdx,
            LastTimestamp:      lastTimestamp,
            TotalProcessedRows: checkpoint.ProcessedRows,
            Status:             types.StatusInProgress,
        }
        if err := e.checkpointMgr.SaveShardGroupCheckpoint(ctx, task.ID, sg.ID, sgCP); err != nil {
            logger.Warn("failed to save shard group checkpoint", zap.Error(err))
        }
    }

    return nil
}
```

## 6. QueryDataBatch with Time Bounds

### 6.1 Modified Interface

```go
// QueryDataBatch queries multiple series within a time range
// startTime: inclusive lower bound
// endTime: exclusive upper bound (time < endTime)
func QueryDataBatch(ctx context.Context, measurement string, series []string,
    startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)
```

### 6.2 InfluxDB V1 Implementation

```go
func (a *InfluxDBV1Adapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

    var lastTS int64
    if lastCheckpoint != nil {
        lastTS = lastCheckpoint.LastTimestamp
    }

    // Determine query start time
    queryStart := startTime
    if lastTS > 0 && lastTS > startTime.UnixNano() {
        queryStart = time.Unix(0, lastTS)
    }

    batchSize := getBatchSize(cfg)
    whereClause := BuildWhereClause(series)

    var totalRecords int
    var maxTS int64

    for {
        // Query with time bounds: startTime (or resume point) AND endTime
        query := fmt.Sprintf(`
            SELECT * FROM %s
            WHERE (%s) AND time >= '%s' AND time < '%s'
            LIMIT %d ORDER BY time`,
            influxQuoteIdentifier(measurement),
            whereClause,
            queryStart.Format(time.RFC3339Nano),
            endTime.Format(time.RFC3339Nano),
            batchSize)

        records, err := a.executeSelectQuery(ctx, query)
        if err != nil {
            return nil, fmt.Errorf("batch query failed: %w", err)
        }

        if len(records) == 0 {
            break
        }

        if err := batchFunc(records); err != nil {
            return nil, fmt.Errorf("batch func failed: %w", err)
        }

        totalRecords += len(records)

        // Update max timestamp
        for _, record := range records {
            if record.Time > maxTS {
                maxTS = record.Time
            }
        }

        // If we got fewer records than batch size, we're done
        if len(records) < batchSize {
            break
        }

        // Move to next batch within time bounds
        if maxTS < math.MaxInt64 {
            queryStart = time.Unix(0, maxTS+1)
        }

        // Don't query beyond window end
        if queryStart.UnixNano() >= endTime.UnixNano() {
            break
        }

        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(100 * time.Millisecond):
        }
    }

    return &types.Checkpoint{
        LastTimestamp: maxTS,
        ProcessedRows: int64(totalRecords),
    }, nil
}
```

### 6.3 InfluxDB V2 Implementation

```go
func (a *InfluxDBV2Adapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

    var lastTS int64
    if lastCheckpoint != nil && lastCheckpoint.LastTimestamp != 0 {
        lastTS = lastCheckpoint.LastTimestamp
    }

    queryStart := startTime
    if lastTS > 0 && lastTS > startTime.UnixNano() {
        queryStart = time.Unix(0, lastTS)
    }

    batchSize := getBatchSize(cfg)
    fluxFilter := BuildFluxFilter(series)

    var totalRecords int
    var maxTS int64

    for {
        fluxQuery := fmt.Sprintf(`
            from(bucket: "%s")
              |> range(start: %s, stop: %s)
              |> filter(fn: (r) => r._measurement == "%s" and (%s))
              |> limit(n: %d)
        `, a.config.Bucket,
            queryStart.Format(time.RFC3339Nano),
            endTime.Format(time.RFC3339Nano),
            measurement, fluxFilter, batchSize)

        records, err := a.executeFluxSelect(ctx, fluxQuery)
        if err != nil {
            return nil, fmt.Errorf("batch query failed: %w", err)
        }

        if len(records) == 0 {
            break
        }

        if err := batchFunc(records); err != nil {
            return nil, fmt.Errorf("batch func failed: %w", err)
        }

        totalRecords += len(records)

        for _, record := range records {
            if record.Time > maxTS {
                maxTS = record.Time
            }
        }

        if len(records) < batchSize {
            break
        }

        if maxTS < math.MaxInt64 {
            queryStart = time.Unix(0, maxTS+1)
        }

        if queryStart.UnixNano() >= endTime.UnixNano() {
            break
        }

        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(100 * time.Millisecond):
        }
    }

    return &types.Checkpoint{
        LastTimestamp: maxTS,
        ProcessedRows: int64(totalRecords),
    }, nil
}
```

## 7. Checkpoint Storage

### 7.1 Schema Change

```sql
-- New table for shard group checkpoints
CREATE TABLE IF NOT EXISTS shard_group_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    shard_group_id TEXT NOT NULL,
    window_start INTEGER NOT NULL,
    window_end INTEGER NOT NULL,
    last_completed_batch INTEGER NOT NULL DEFAULT 0,
    last_timestamp INTEGER NOT NULL DEFAULT 0,
    total_processed_rows INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(task_id, shard_group_id, window_start)
);
```

### 7.2 Manager Methods

```go
func (m *Manager) SaveShardGroupCheckpoint(ctx context.Context,
    taskID string, shardGroupID int, cp *ShardGroupCheckpoint) error

func (m *Manager) LoadShardGroupCheckpoint(ctx context.Context,
    taskID string, shardGroupID int) (*ShardGroupCheckpoint, error)

func (m *Manager) MarkShardGroupCompleted(ctx context.Context,
    taskID string, shardGroupID int) error

func (m *Manager) ListShardGroupCheckpoints(ctx context.Context,
    taskID string) ([]*ShardGroupCheckpoint, error)

// LoadShardGroupCheckpointForWindow loads checkpoint for specific time window
func (m *Manager) LoadShardGroupCheckpointForWindow(ctx context.Context,
    taskID string, shardGroupID int, windowStart, windowEnd int64) (*ShardGroupCheckpoint, error)
```

## 8. Crash Recovery Scenarios

### 8.1 Scenario: Crash During Batch Processing in Time Window

```
Shard Group 1 [2024-01-01 ~ 2024-02-01), Window [01-01~01-08):
  Batch 0: completed, checkpoint saved
  Batch 1: completed, checkpoint saved
  Batch 2: in progress... CRASH

Recovery:
  1. Load checkpoint for window [01-01~01-08): LastCompletedBatch=1, LastTimestamp=T_batch1
  2. Resume from batch index 2
  3. Query: WHERE series_batch[2] AND time >= T_batch1+1 AND time < 01-08
  4. Re-process Batch 2, 3, 4...
```

### 8.2 Scenario: Crash Between Time Windows

```
Shard Group 1:
  Window [01-01~01-08): completed
  Window [01-08~01-15): Batch 0-3 completed, Batch 4 in progress... CRASH

Recovery:
  1. Window [01-01~01-08): skip (completed)
  2. Window [01-08~01-15): resume from batch 4
  3. Continue with remaining windows
```

### 8.3 Scenario: Shard Group Partially Completed

```
Shard Group 1: completed
Shard Group 2:
  Window [01-01~01-08): completed
  Window [01-08~01-15): in progress... CRASH
Shard Group 3: not started

Recovery:
  1. SG1: skip (completed)
  2. SG2: resume from window [01-08~01-15)
  3. SG3: start from beginning
```

## 9. Files to Modify/Create

### Modified Files

```
pkg/types/
├── config.go                      # Add ShardGroupConfig with TimeWindow
├── checkpoint.go                  # Add ShardGroupCheckpoint with window fields

internal/adapter/
├── adapter.go                     # Add DiscoverShardGroups, DiscoverSeriesInTimeWindow
                                   # Modify QueryDataBatch signature

internal/adapter/source/
├── influxdb.go                    # Implement DiscoverShardGroups, DiscoverSeriesInTimeWindow
                                   # Update QueryDataBatch with time bounds

internal/checkpoint/
├── manager.go                     # Add shard group checkpoint methods
├── store.go                       # Add shard group checkpoint storage

internal/engine/
├── migration.go                   # Add shard-group mode execution

internal/config/
├── validator.go                   # Validate shard-group config including TimeWindow
```

### New Files

```
openspec/changes/shard-group-aware-migration/
├── proposal.md
├── design.md                      # This file (updated)
├── tasks.md
└── specs/
    ├── source-adapter/spec.md
    ├── engine/spec.md
    ├── checkpoint-manager/spec.md
    └── config/spec.md
```

## 10. Testing Strategy

### Unit Tests

1. `ShardGroup.Overlaps()` - time range intersection
2. `ShardGroup.EffectiveTimeRange()` - boundary calculation
3. `SplitTimeWindows()` - window splitting correctness
4. `PartitionSeries()` - batch partitioning
5. `BuildWhereClause()` - SQL injection prevention
6. `QueryDataBatch` with time bounds - respects endTime

### Integration Tests

1. Mock InfluxDB with shard groups, verify correct queries
2. Verify time window boundaries are respected
3. Simulate crash after batch 2 in window, verify resume
4. Verify at-least-once: data not lost after crash
5. Memory test: large series count should not OOM

### Performance Tests

1. Compare memory usage: old vs new approach
2. Compare execution time for large series counts
3. Verify checkpoint frequency doesn't impact performance
