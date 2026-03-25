# Design: TDengine TimeWindow Configuration

## 1. Overview

This design specifies updating the TDengine source adapter to use a configurable `TimeWindow` from `QueryConfig` instead of the hardcoded 7-day window.

## 2. QueryConfig Update

### 2.1 Current QueryConfig

```go
type QueryConfig struct {
    BatchSize int
}
```

### 2.2 Proposed QueryConfig

```go
type QueryConfig struct {
    BatchSize  int
    TimeWindow time.Duration
}
```

## 3. TDengine Source Changes

### 3.1 Current Implementation (source/tdengine.go:154-155)

```go
startTime := lastTS.Format("2006-01-02 15:04:05.000")
endTime := lastTS.Add(7 * 24 * time.Hour).Format("2006-01-02 15:04:05.000")
```

### 3.2 Proposed Implementation

```go
timeWindow := 7 * 24 * time.Hour  // Default 7 days
if cfg != nil && cfg.TimeWindow > 0 {
    timeWindow = cfg.TimeWindow
}

startTime := lastTS.Format("2006-01-02 15:04:05.000")
endTime := lastTS.Add(timeWindow).Format("2006-01-02 15:04:05.000")
```

## 4. Migration Engine Changes

The migration engine already has access to `mapping.TimeWindow`. Update the `QueryConfig` creation:

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

## 5. Files to Modify

```
pkg/types/
├── query_config.go              # Add TimeWindow field

internal/adapter/source/
├── tdengine.go                   # Use TimeWindow from config

internal/engine/
├── migration.go                  # Pass TimeWindow in QueryConfig
```
