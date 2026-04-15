# Tasks: Time Precision Fix - Nanosecond Timestamp Support

## Overview

This document contains implementation tasks for changing `Record.Time` from `time.Time` to `int64` (Unix nanoseconds).

---

## Task 1: Update Record Type and Add Helper Functions

**Files:**
- Modify: `pkg/types/record.go`
- Modify: `pkg/types/record_test.go`

- [x] **Step 1: Add helper functions for time conversion**

Add to `pkg/types/record.go`:

```go
// TimeToUnixNano converts time.Time to Unix nanoseconds
func TimeToUnixNano(t time.Time) int64 {
    return t.UnixNano()
}

// UnixNanoToTime converts Unix nanoseconds to time.Time
func UnixNanoToTime(ns int64) time.Time {
    return time.Unix(0, ns)
}

// NowNano returns current time as Unix nanoseconds
func NowNano() int64 {
    return time.Now().UnixNano()
}

// IsZeroTime checks if nanosecond timestamp is zero (epoch)
func IsZeroTime(ns int64) bool {
    return ns == 0
}
```

- [x] **Step 2: Change Record.Time from time.Time to int64**

Modify `Record` struct in `pkg/types/record.go`:

```go
type Record struct {
    Time    int64                  // Changed from time.Time to Unix nanoseconds
    Tags    map[string]string
    Fields  map[string]interface{}
}
```

- [x] **Step 3: Run build to check for compilation errors**

Run: `go build ./pkg/types/...`
Expected: Should fail with errors about Time being int64 now

- [x] **Step 4: Update record_test.go**

Update all test cases that use `time.Time` to use `int64` nanoseconds:

```go
func TestNewRecord(t *testing.T) {
    r := NewRecord()
    assert.Equal(t, int64(0), r.Time)  // Changed from time.Time{}
    assert.NotNil(t, r.Tags)
    assert.NotNil(t, r.Fields)
}
```

- [x] **Step 5: Run tests to verify**

Run: `go test ./pkg/types/... -v`
Expected: All tests pass

- [x] **Step 6: Commit**

```bash
git add pkg/types/record.go pkg/types/record_test.go
git commit -m "feat: change Record.Time to int64 nanoseconds"
```

---

## Task 2: Update Checkpoint Type

**Files:**
- Modify: `pkg/types/checkpoint.go`

- [x] **Step 1: Change LastTimestamp from time.Time to int64**

Modify `Checkpoint` struct in `pkg/types/checkpoint.go`:

```go
type Checkpoint struct {
    // ... existing fields ...
    LastTimestamp int64  // Changed from time.Time
    // ... existing fields ...
}
```

- [x] **Step 2: Run build to check**

Run: `go build ./pkg/types/...`
Expected: Should fail in checkpoint package

- [x] **Step 3: Commit**

```bash
git add pkg/types/checkpoint.go
git commit -m "feat: checkpoint LastTimestamp to int64"
```

---

## Task 3: Update Checkpoint Store

**Files:**
- Modify: `internal/checkpoint/store.go`

- [x] **Step 1: Update LoadCheckpoint to handle int64 timestamps**

Modify `LoadCheckpoint` in `internal/checkpoint/store.go`:

```go
if lastTS.Valid {
    // Try RFC3339Nano first for nanosecond precision
    if t, err := time.Parse(time.RFC3339Nano, lastTS.String); err == nil {
        cp.LastTimestamp = t.UnixNano()
    } else if t, err := time.Parse(time.RFC3339, lastTS.String); err == nil {
        // Backward compatibility for existing checkpoints
        cp.LastTimestamp = t.UnixNano()
    }
}
```

- [x] **Step 2: Update SaveCheckpoint to store RFC3339Nano**

Modify `SaveCheckpoint`:

```go
ts := ""
if cp.LastTimestamp != 0 {
    ts = time.Unix(0, cp.LastTimestamp).Format(time.RFC3339Nano)
}
```

- [x] **Step 3: Update ListCheckpoints (same as LoadCheckpoint)**

- [x] **Step 4: Update GetTasksByStatus (same as LoadCheckpoint)**

- [x] **Step 5: Run build to verify**

Run: `go build ./internal/checkpoint/...`
Expected: Build succeeds

- [x] **Step 6: Commit**

```bash
git add internal/checkpoint/store.go
git commit -m "feat: checkpoint store handles int64 timestamps"
```

---

## Task 4: Update InfluxDB V1 Source Adapter

**Files:**
- Modify: `internal/adapter/source/influxdb.go`

- [x] **Step 1: Update parseValues timestamp parsing**

Find and modify the timestamp parsing in `parseValues` method (around line 289):

```go
case "time":
    if ts, ok := val.(string); ok {
        // Try RFC3339Nano first
        if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
            record.Time = t.UnixNano()
        } else if t, err := time.Parse(time.RFC3339, ts); err == nil {
            // Fallback to RFC3339 for backward compatibility
            record.Time = t.UnixNano()
            logger.Warn("timestamp parsed with reduced precision",
                zap.String("timestamp", ts))
        } else {
            logger.Warn("failed to parse timestamp",
                zap.String("timestamp_string", ts),
                zap.Error(err))
        }
    }
```

- [x] **Step 2: Update QueryData callback timestamp tracking**

Find where `lastRecord.Time` is used and ensure it's int64:

```go
if len(records) > 0 {
    lastRecord := records[len(records)-1]
    totalProcessed += int64(len(records))
    lastTimestamp = lastRecord.Time  // Now int64
    e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
        0, lastTimestamp, totalProcessed, types.StatusInProgress)
}
```

Note: `SaveCheckpoint` signature may need updating - see Task 6.

- [x] **Step 3: Run build to check**

Run: `go build ./internal/adapter/source/...`
Expected: May have errors due to SaveCheckpoint signature

- [x] **Step 4: Commit**

```bash
git add internal/adapter/source/influxdb.go
git commit -m "feat: influxdb source parses timestamps to int64"
```

---

## Task 5: Update TDengine Source Adapter

**Files:**
- Modify: `internal/adapter/source/tdengine.go`

- [x] **Step 1: Update QueryData timestamp parsing**

Find and modify the timestamp parsing (around line 182):

```go
if ts, ok := val.(string); ok {
    if t, err := time.Parse("2006-01-02 15:04:05.000", ts); err == nil {
        record.Time = t.UnixNano()
        if t.UnixNano() > maxTS {
            maxTS = t.UnixNano()
        }
    } else {
        logger.Warn("failed to parse TDengine timestamp",
            zap.String("timestamp_string", ts),
            zap.Error(err))
    }
}
```

- [x] **Step 2: Update maxTS tracking**

Note that `maxTS` is still `time.Time` for comparison purposes, but when assigned to `record.Time`, convert to nanoseconds:

```go
record.Time = t.UnixNano()
if t.After(maxTS) {
    maxTS = t
}
```

- [x] **Step 3: Update return checkpoint**

```go
return &types.Checkpoint{
    LastTimestamp: maxTS.UnixNano(),  // Convert to int64
    ProcessedRows: totalProcessed,
}, nil
```

- [x] **Step 4: Run build to check**

Run: `go build ./internal/adapter/source/...`
Expected: Build succeeds

- [x] **Step 5: Commit**

```bash
git add internal/adapter/source/tdengine.go
git commit -m "feat: tdengine source parses timestamps to int64"
```

---

## Task 6: Update Manager SaveCheckpoint Signature

**Files:**
- Modify: `internal/checkpoint/manager.go`

- [x] **Step 1: Update SaveCheckpoint to accept int64 instead of time.Time**

Current signature:
```go
func (m *Manager) SaveCheckpoint(ctx context.Context, taskID, sourceTable string, lastID int64, lastTS time.Time, processedRows int64, status types.CheckpointStatus) error
```

New signature:
```go
func (m *Manager) SaveCheckpoint(ctx context.Context, taskID, sourceTable string, lastID int64, lastTS int64, processedRows int64, status types.CheckpointStatus) error
```

Update the implementation to not convert time:

```go
cp.LastTimestamp = lastTS  // Already int64
```

- [x] **Step 2: Run build to find all callers**

Run: `go build ./...`
Expected: Errors in files calling SaveCheckpoint

- [x] **Step 3: Update all callers in migration.go**

Find all calls to SaveCheckpoint and update:

```go
// Before
e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
    0, lastTimestamp, totalProcessed, types.StatusInProgress)

// After (lastTimestamp is now int64)
e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
    0, lastTimestamp, totalProcessed, types.StatusInProgress)
```

- [x] **Step 4: Run build to verify**

Run: `go build ./...`
Expected: Build succeeds

- [x] **Step 5: Commit**

```bash
git add internal/checkpoint/manager.go internal/engine/migration.go
git commit -m "refactor: SaveCheckpoint uses int64 timestamp"
```

---

## Task 7: Update MySQL Source Adapter

**Files:**
- Modify: `internal/adapter/source/mysql.go`

- [x] **Step 1: Find timestamp parsing code**

Look for code parsing timestamp columns (around line 179):

```go
if col.Name == "timestamp" || col.Name == "time" {
    if t, ok := val.([]byte); ok {
        if parsed, err := time.Parse("2006-01-02 15:04:05.000", string(t)); err == nil {
            record.Time = parsed.UnixNano()  // Convert to int64
        }
    }
}
```

- [x] **Step 2: Run build**

Run: `go build ./internal/adapter/source/...`
Expected: Build succeeds

- [x] **Step 3: Commit**

```bash
git add internal/adapter/source/mysql.go
git commit -m "feat: mysql source parses timestamps to int64"
```

---

## Task 8: Update Target Adapters

**Files:**
- Modify: `internal/adapter/target/mysql.go`
- Modify: `internal/adapter/target/tdengine.go`
- Modify: `internal/adapter/target/influxdb.go`

### MySQL Target

- [x] **Step 1: Update recordToValues timestamp formatting**

Find where timestamp is formatted for MySQL:

```go
// In recordToValues method
if col == "timestamp" {
    // Convert nanoseconds to MySQL DATETIME(3) format
    t := time.Unix(0, record.Time)
    values = append(values, t.Format("2006-01-02 15:04:05.000"))
}
```

### TDengine Target

- [x] **Step 2: Update timestamp usage**

Find and update:

```go
// Before
timestamp := record.Time.UnixNano()

// After (already nanoseconds)
timestamp := record.Time
```

### InfluxDB Target

- [x] **Step 3: Update line protocol timestamp**

```go
// Before
timestamp := record.Time.UnixNano()

// After (already nanoseconds)
timestamp := record.Time
```

- [x] **Step 4: Run build for all targets**

Run: `go build ./internal/adapter/target/...`
Expected: Build succeeds

- [x] **Step 5: Commit**

```bash
git add internal/adapter/target/mysql.go internal/adapter/target/tdengine.go internal/adapter/target/influxdb.go
git commit -m "feat: target adapters handle int64 timestamps"
```

---

## Task 9: Update Transform Engine

**Files:**
- Modify: `internal/engine/transform.go`

- [x] **Step 1: Find and update time comparisons**

Search for `record.Time.Before`, `record.Time.After`, `record.Time.IsZero`:

Replace patterns:
- `record.Time.IsZero()` → `record.Time == 0`
- `record.Time.Before(t)` → `record.Time < t.UnixNano()`
- `record.Time.After(t)` → `record.Time > t.UnixNano()`
- `record.Time.Equal(t)` → `record.Time == t.UnixNano()`

- [x] **Step 2: Run build**

Run: `go build ./internal/engine/...`
Expected: Build succeeds

- [x] **Step 3: Commit**

```bash
git add internal/engine/transform.go
git commit -m "feat: transform engine handles int64 timestamps"
```

---

## Task 10: Update Tests

**Files:**
- Modify: All `*_test.go` files that use Record.Time

- [x] **Step 1: Find all test files using record.Time**

Run: `grep -r "record.Time\|Record{.*Time:" --include="*_test.go" .`

- [x] **Step 2: Update each test file**

For each occurrence, update:
```go
// Before
record := types.Record{Time: time.Now()}

// After
record := types.Record{Time: time.Now().UnixNano()}
```

- [x] **Step 3: Run all tests**

Run: `go test ./... -v 2>&1 | head -200`
Expected: All tests pass (or fail with clear errors to fix)

- [x] **Step 4: Commit**

```bash
git add ./*_test.go
git commit -m "test: update tests for int64 timestamps"
```

---

## Task 11: Final Verification

- [x] **Step 1: Run full build**

Run: `go build ./...`
Expected: Build succeeds

- [x] **Step 2: Run all tests**

Run: `go test ./...`
Expected: All tests pass

- [x] **Step 3: Run race detector**

Run: `go test -race ./...`
Expected: No race conditions detected

- [x] **Step 4: Final commit**

```bash
git add -A
git commit -m "feat: complete time precision fix - nanosecond support"
```

---

## Summary

| Task | Description | Files | Status |
|------|-------------|-------|--------|
| 1 | Record type + helpers | `pkg/types/record.go` | ✅ DONE |
| 2 | Checkpoint type | `pkg/types/checkpoint.go` | ✅ DONE |
| 3 | Checkpoint store | `internal/checkpoint/store.go` | ✅ DONE |
| 4 | InfluxDB source | `internal/adapter/source/influxdb.go` | ✅ DONE |
| 5 | TDengine source | `internal/adapter/source/tdengine.go` | ✅ DONE |
| 6 | Manager signature | `internal/checkpoint/manager.go`, `migration.go` | ✅ DONE |
| 7 | MySQL source | `internal/adapter/source/mysql.go` | ✅ DONE |
| 8 | Target adapters | `internal/adapter/target/*.go` | ✅ DONE |
| 9 | Transform engine | `internal/engine/transform.go` | ✅ DONE |
| 10 | Update tests | All `*_test.go` | ✅ DONE |
| 11 | Final verification | - | ✅ DONE |

## Verification

```bash
go build ./...      # Build succeeds
go test ./...       # All tests pass
go test -race ./... # No race conditions
```
