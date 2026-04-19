# Bugfixes Design Document

**Date**: 2026-04-19
**Project**: InfluxDB Migration Tool
**Related Spec**: spec.md

---

## P0-1: Batch Mode Checkpoint Resume

### Problem Analysis
The batch mode migration processes series in batches, but upon resume, does not track which batches have been completed. The `lastCheckpoint` is passed to `QueryDataBatch` but never used to skip completed batches.

### Root Cause
```go
// Current code in runTaskBatchMode (lines 566-604):
for i, batch := range batches {
    checkpoint, err := sourceAdapter.QueryDataBatch(
        ctx,
        task.Mapping.SourceTable,
        batch,
        startTime,
        endTime,
        lastCheckpoint,  // ← Never used to skip completed batches
        ...
    )
}
```

The checkpoint stores `ProcessedRows` as total count, not the batch index. Even if we had the batch index, there's no logic to skip batches where `i <= lastCompletedBatch`.

### Solution Design

**Checkpoint Enhancement**:
```go
// Add to Checkpoint type or use ProcessedRows differently
// ProcessedRows will store: last completed batch index (for batch mode)

// In runTaskBatchMode:
lastCheckpoint, err := e.checkpointMgr.LoadCheckpoint(ctx, task.ID, task.Mapping.SourceTable)
if err != nil {
    return fmt.Errorf("failed to load checkpoint: %w", err)
}

lastCompletedBatch := -1
if lastCheckpoint != nil && lastCheckpoint.Status == types.StatusInProgress {
    // ProcessedRows stores batch index in batch mode
    lastCompletedBatch = int(lastCheckpoint.ProcessedRows) - 1
}

for i, batch := range batches {
    if i <= lastCompletedBatch {
        logger.Info("skipping already completed batch", zap.Int("batch_index", i))
        continue
    }
    // ... process batch
}

// Save checkpoint with batch index
if err := e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
    0, checkpoint.LastTimestamp, int64(i+1), types.StatusInProgress); err != nil {
    // ...
}
```

**Checkpoint Type Clarification**:
- Single mode: `ProcessedRows` = total records processed
- Batch mode: `ProcessedRows` = last completed batch index

This is backward compatible as batch index is just a counter, and for single mode we continue storing actual row count.

---

## P0-2: Timer Resource Leak

### Problem Analysis
In `queryWithTimeRange`, a timer is created in each loop iteration. The `select` statement only stops the timer in the `ctx.Done()` case. If the loop continues, a new timer is created each time. If context is cancelled in the gap between iterations, accumulated timers may leak.

### Root Cause
```go
for windowStart := startTime; windowStart.Before(endTime); {
    timer := time.NewTimer(e.config.Migration.ChunkInterval)  // New timer each iteration
    select {
    case <-ctx.Done():
        timer.Stop()  // Only stops on cancel
        return nil, ctx.Err()
    case <-timer.C:
        // timer.Stop() is unnecessary here but not harmful
    }
    // If we exit loop here (windowStart >= endTime), timer leaks if not stopped
}
```

### Solution Design
```go
// Solution: Use defer to ensure timer cleanup, and check ctx before creating timer
timer := time.NewTimer(e.config.Migration.ChunkInterval)
defer timer.Stop()  // Always stop when scope exits

select {
case <-ctx.Done():
    return nil, ctx.Err()
case <-timer.C:
    // Continue processing
}
```

However, we need to be careful not to have unbounded timer accumulation. The fix should:
1. Always defer Stop() for each timer
2. Check ctx before creating new timer in next iteration

---

## P1-1: V2 Source Tag/Field Parsing

### Problem Analysis
`parseV1Values` treats all string values as tags. But when V2 source uses V1 compatibility API, string columns in query results could be either tags or fields.

### Solution Design

**Option A**: Use DiscoverTagKeys to build a set of known tag keys
```go
// In QueryDataBatch for V2 adapter:
tagKeys, err := a.DiscoverTagKeys(ctx, measurement)
tagKeySet := make(map[string]bool)
for _, k := range tagKeys {
    tagKeySet[k] = true
}

records, err := a.executeV1SelectQuery(ctx, query, tagKeySet)
```

**Option B**: Check if column name matches known tag pattern

We choose Option A as it's more robust and leverages existing DiscoverTagKeys method.

**Modified parseV1ValuesWithTagKeys**:
```go
func parseV1ValuesWithTagKeys(columns []string, values []interface{}, tagKeySet map[string]bool) *types.Record {
    record := types.NewRecord()
    for i, col := range columns {
        if i >= len(values) { continue }
        val := values[i]
        if val == nil { continue }

        switch col {
        case "time":
            // existing time parsing logic
        default:
            if tagKeySet[col] {
                record.AddTag(col, fmt.Sprintf("%v", val))
            } else {
                // Field - use existing type inference
                switch v := val.(type) {
                case float64:
                    record.AddField(col, v)
                case string:
                    record.AddField(col, v)  // String fields preserved
                case bool:
                    record.AddField(col, v)
                case int64:
                    record.AddField(col, v)
                // ... other types
                }
            }
        }
    }
    return record
}
```

---

## P1-2: Target basic_auth Configuration

### Problem Analysis
`targetConfigToMap` for InfluxDB targets doesn't include authentication info:
```go
case "influxdb":
    m["influxdb"] = map[string]interface{}{
        "url":     tgt.InfluxDB.URL,
        "token":   tgt.InfluxDB.Token,
        // Missing: username, password, basic_auth
    }
```

### Solution Design
```go
case "influxdb-v1":
    influxCfg := map[string]interface{}{
        "url":     tgt.InfluxDB.URL,
        "version": tgt.InfluxDB.Version,
    }
    // Include retention_policy for V1
    if tgt.InfluxDB.RetentionPolicy != "" {
        influxCfg["retention_policy"] = tgt.InfluxDB.RetentionPolicy
    }
    // V1 uses basic_auth with username/password
    if tgt.User != "" {
        influxCfg["basic_auth"] = map[string]interface{}{
            "username": tgt.User,
            "password": tgt.Password,
        }
    }
    m["influxdb"] = influxCfg

case "influxdb-v2":
    influxCfg := map[string]interface{}{
        "url":     tgt.InfluxDB.URL,
        "version": tgt.InfluxDB.Version,
        "token":   tgt.InfluxDB.Token,
        "org":     tgt.InfluxDB.Org,
        "bucket":  tgt.InfluxDB.Bucket,
    }
    m["influxdb"] = influxCfg
```

---

## P2-1: Shard-Group Parallelism

### Problem Analysis
`ShardParallelism` config is read but never used. Shards are processed in a simple for loop:
```go
for _, sg := range relevantGroups {
    if err := e.migrateShardGroup(...); err != nil {  // Serial
```

### Solution Design
```go
func (e *MigrationEngine) runTaskShardGroupMode(ctx context.Context, task *MigrationTask) error {
    // ... existing setup ...

    parallelism := 1
    if e.config.InfluxToInflux.ShardGroupConfig != nil {
        parallelism = e.config.InfluxToInflux.ShardGroupConfig.ShardParallelism
    }
    if parallelism < 1 {
        parallelism = 1
    }

    // For parallelism=1, use existing serial logic
    if parallelism == 1 {
        for _, sg := range relevantGroups {
            if err := e.migrateShardGroup(ctx, task, sg, sourceAdapter, targetAdapter, queryStart, queryEnd); err != nil {
                return err
            }
        }
        return nil
    }

    // For parallelism>1, use goroutines with semaphore
    semaphore := make(chan struct{}, parallelism)
    var wg sync.WaitGroup
    var firstErr error
    var errMu sync.Mutex

    for _, sg := range relevantGroups {
        wg.Add(1)
        go func(shardGroup *adapter.ShardGroup) {
            defer wg.Done()

            // Acquire semaphore slot
            semaphore <- struct{}{}
            defer func() { <-semaphore }()

            // Check context before processing
            select {
            case <-ctx.Done():
                return
            default:
            }

            if err := e.migrateShardGroup(ctx, task, shardGroup, sourceAdapter, targetAdapter, queryStart, queryEnd); err != nil {
                errMu.Lock()
                if firstErr == nil {
                    firstErr = err
                }
                errMu.Unlock()
            }
        }(sg)
    }

    wg.Wait()
    return firstErr
}
```

---

## P2-2: Retry With Jitter

### Problem Analysis
Fixed exponential backoff without jitter causes thundering herd:
```
Attempt 1 fails at T+0
All instances retry at T+1, T+2, T+4, T+8... simultaneously
```

### Solution Design
```go
import (
    "math/rand"
   "time"
)

func (e *MigrationEngine) writeWithRetry(...) error {
    maxAttempts := e.config.Retry.MaxAttempts
    baseDelay := e.config.Retry.InitialDelay
    if baseDelay == 0 {
        baseDelay = 1 * time.Second
    }
    maxDelay := e.config.Retry.MaxDelay
    if maxDelay == 0 {
        maxDelay = 60 * time.Second
    }
    backoffMultiplier := e.config.Retry.BackoffMultiplier
    if backoffMultiplier == 0 {
        backoffMultiplier = 2.0
    }

    var lastErr error
    delay := baseDelay

    for attempt := 1; attempt <= maxAttempts; attempt++ {
        err := targetAdapter.WriteBatch(ctx, measurement, records)
        if err == nil {
            return nil
        }

        lastErr = err
        logger.Warn("write batch failed, will retry",
            zap.Int("attempt", attempt),
            zap.Duration("delay", delay),
            zap.Error(err))

        if attempt < maxAttempts {
            // Calculate jittered delay: base_delay * (0.5 + random[0,1]) * multiplier^(attempt-1)
            // This gives range [0.5x, 1.5x] of exponential delay
            jitterFactor := 0.5 + rand.Float64()  // 0.5 to 1.5
            sleepDuration := time.Duration(float64(delay) * jitterFactor)

            timer := time.NewTimer(sleepDuration)
            select {
            case <-ctx.Done():
                timer.Stop()
                return ctx.Err()
            case <-timer.C:
                // Continue to retry
            }

            // Calculate next delay
            delay = time.Duration(float64(delay) * backoffMultiplier)
            if delay > maxDelay {
                delay = maxDelay
            }
        }
    }

    return fmt.Errorf("write batch failed after %d attempts: %w", maxAttempts, lastErr)
}
```

**Note**: Add `math/rand` seed or use `rand.New(rand.NewSource(time.Now().UnixNano()))` for thread safety if needed.

---

## P2-3: DiscoverSeries Pagination

### Problem Analysis
`DiscoverSeries` returns all series without limit. Large deployments can have 100K+ series causing OOM.

### Solution Design
```go
func (a *InfluxDBV1Adapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
    var allSeries []string
    var lastKey string
    batchSize := 10000  // Configurable batch size

    for {
        var query string
        if lastKey == "" {
            query = fmt.Sprintf("SHOW SERIES FROM %s LIMIT %d",
                influxQuoteIdentifier(measurement), batchSize)
        } else {
            // InfluxDB 1.7+ supports WHERE series_key >
            query = fmt.Sprintf("SHOW SERIES FROM %s WHERE series_key > '%s' LIMIT %d",
                influxQuoteIdentifier(measurement), lastKey, batchSize)
        }

        results, err := a.executeQuery(ctx, query)
        if err != nil {
            return nil, err
        }

        batchCount := 0
        for _, result := range results {
            for _, values := range result.Values {
                if len(values) > 0 {
                    if key, ok := values[0].(string); ok {
                        allSeries = append(allSeries, key)
                        lastKey = key
                        batchCount++
                    }
                }
            }
        }

        // If returned fewer than batch size, we're done
        if batchCount < batchSize {
            break
        }

        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        default:
        }
    }

    return allSeries, nil
}
```

**Fallback for older InfluxDB**: If `WHERE series_key >` syntax fails, fall back to collecting all results (original behavior) but with a warning log.

---

## P3-1: Code Deduplication

### Problem Analysis
Two identical functions exist:
1. `TransformEngine.FilterNulls` in `transform.go`
2. `filterNilValues` (local function) in `migration.go`

### Solution Design
```go
// transform.go - keep this method
func (t *TransformEngine) FilterNulls(record *types.Record) *types.Record {
    // existing implementation
}

// migration.go - replace with call to transformer
import "github.com/migration-tools/influx-migrator/internal/engine"

// In processBatch:
filtered := e.transformer.FilterNulls(&records[i])
```

---

## P3-2: Checkpoint Save Failure Configuration

### Problem Analysis
Checkpoint save failures only log warnings. Production systems may want fail-fast behavior.

### Solution Design
```go
// In types/config.go
type MigrationSettings struct {
    // ... existing fields ...
    FailOnCheckpointError bool `mapstructure:"fail_on_checkpoint_error"`
}

// In config/validator.go - add to ApplyDefaults
if newCfg.Migration.FailOnCheckpointError == false {
    newCfg.Migration.FailOnCheckpointError = false  // Default is warn-only
}

// In engine/migration.go - modify checkpoint save error handling
if err := e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
    0, saveTimestamp, saveProcessed, types.StatusInProgress); err != nil {
    if e.config.Migration.FailOnCheckpointError {
        return fmt.Errorf("failed to save checkpoint: %w", err)
    }
    logger.Error("failed to save checkpoint, data loss risk on crash", zap.Error(err))
}
```

---

## P3-3: Config Comment Fix

### Solution Design
```go
type InfluxDBConfig struct {
    Version         string `mapstructure:"version"`
    URL             string `mapstructure:"url"`
    // Token is used for V2 native API authentication
    // For V2 source using V1 compatibility API, use Username/Password instead
    Token           string `mapstructure:"token"`
    // Org is used for V2 native API (not needed for V1 compatibility API)
    Org             string `mapstructure:"org"`
    Bucket          string `mapstructure:"bucket"`
    // Username/Password are used for:
    // 1. V1 source authentication
    // 2. V2 source using V1 compatibility API
    Username        string `mapstructure:"username"`
    Password        string `mapstructure:"password"`
    // RetentionPolicy is used for V2 source using V1 compatibility API
    // to specify which retention policy to use
    RetentionPolicy string `mapstructure:"retention_policy"`
}
```

---

## Testing Strategy

### Unit Tests
1. **P0-1**: Create test that simulates interruption and verify no duplicate processing
2. **P0-2**: Verify no timer leaks after context cancellation
3. **P1-1**: Test with mocked tag keys, verify string fields preserved
4. **P2-1**: Test with parallelism>1, verify concurrent execution
5. **P2-2**: Verify jitter randomization in retry loop

### Integration Tests
1. Resume interrupted batch migration, verify no duplicates
2. Long-running migration with context cancellation, verify clean shutdown

---

## Rollback Plan

If any fix causes regression:
1. Revert to previous commit
2. Fix identified issue
3. Re-run tests
4. Re-release

All changes are backward compatible, so rollback should not break existing configurations.
