# Tasks: TDengine TimeWindow Configuration

## Overview

Update TDengine source to use configurable TimeWindow from QueryConfig instead of hardcoded 7 days.

---

## Task 1: Update QueryConfig with TimeWindow

**Files:**
- Modify: `pkg/types/query_config.go`

- [x] **Step 1: Add TimeWindow field**

Update `pkg/types/query_config.go`:

```go
package types

import "time"

type QueryConfig struct {
    BatchSize  int
    TimeWindow time.Duration
}
```

- [x] **Step 2: Commit**

```bash
git add pkg/types/query_config.go
git commit -m "feat: add TimeWindow to QueryConfig"
```

---

## Task 2: Update TDengine Source

**Files:**
- Modify: `internal/adapter/source/tdengine.go`

- [x] **Step 1: Update TDengine QueryData signature**

The signature already accepts `cfg *types.QueryConfig` from batch size task.

- [x] **Step 2: Update time window logic**

Change:
```go
startTime := lastTS.Format("2006-01-02 15:04:05.000")
endTime := lastTS.Add(7 * 24 * time.Hour).Format("2006-01-02 15:04:05.000")
```

To:
```go
timeWindow := 7 * 24 * time.Hour  // Default 7 days
if cfg != nil && cfg.TimeWindow > 0 {
    timeWindow = cfg.TimeWindow
}

startTime := lastTS.Format("2006-01-02 15:04:05.000")
endTime := lastTS.Add(timeWindow).Format("2006-01-02 15:04:05.000")
```

Note: `lastTS` is now `int64` (nanoseconds), so need to convert:
```go
timeWindow := 7 * 24 * time.Hour
if cfg != nil && cfg.TimeWindow > 0 {
    timeWindow = cfg.TimeWindow
}

var startTime, endTime string
if lastTS == 0 {
    startTime = "1970-01-01 00:00:00.000"
    endTime = time.Unix(0, 0).Add(timeWindow).Format("2006-01-02 15:04:05.000")
} else {
    startTime = time.Unix(0, lastTS).Format("2006-01-02 15:04:05.000")
    endTime = time.Unix(0, lastTS).Add(timeWindow).Format("2006-01-02 15:04:05.000")
}
```

- [x] **Step 3: Run build to verify**

Run: `go build ./internal/adapter/source/...`
Expected: Build succeeds

- [x] **Step 4: Commit**

```bash
git add internal/adapter/source/tdengine.go
git commit -m "feat: tdengine source uses TimeWindow from config"
```

---

## Task 3: Update Migration Engine

**Files:**
- Modify: `internal/engine/migration.go`

- [x] **Step 1: Parse TimeWindow from mapping**

In `runTask`, when creating QueryConfig:

```go
timeWindow := 168 * time.Hour  // Default
if task.Mapping.TimeWindow != "" {
    if tw, err := time.ParseDuration(task.Mapping.TimeWindow); err == nil {
        timeWindow = tw
    }
}

queryCfg := &types.QueryConfig{
    BatchSize:  e.config.Migration.ChunkSize,
    TimeWindow: timeWindow,
}
```

- [x] **Step 2: Pass queryCfg to TDengine QueryData**

Update the call site for `sourceAdapter.QueryData` to pass `queryCfg`.

Note: For non-TDengine sources, `TimeWindow` will be ignored (they don't use it).

- [x] **Step 3: Run build to verify**

Run: `go build ./internal/engine/...`
Expected: Build succeeds

- [x] **Step 4: Commit**

```bash
git add internal/engine/migration.go
git commit -m "feat: engine passes TimeWindow to source adapters"
```

---

## Summary

| Task | Description | Files | Status |
|------|-------------|-------|--------|
| 1 | Add TimeWindow to QueryConfig | `pkg/types/query_config.go` | ✅ DONE |
| 2 | TDengine Source | `internal/adapter/source/tdengine.go` | ✅ DONE |
| 3 | Migration Engine | `internal/engine/migration.go` | ✅ DONE |

## Verification

```bash
go build ./...     # Build succeeds
go test ./...     # All tests pass
```
