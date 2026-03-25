# Tasks: Batch Size Configuration via QueryData Config

## Overview

Add `QueryConfig` parameter to `QueryData` interface to allow passing batch_size configuration.

---

## Task 1: Add QueryConfig struct

**Files:**
- Create: `pkg/types/query_config.go`

- [ ] **Step 1: Create QueryConfig struct**

Create `pkg/types/query_config.go`:

```go
package types

type QueryConfig struct {
    BatchSize int
}
```

- [ ] **Step 2: Commit**

```bash
git add pkg/types/query_config.go
git commit -m "feat: add QueryConfig struct for batch size"
```

---

## Task 2: Update InfluxDB V1 Source

**Files:**
- Modify: `internal/adapter/source/influxdb.go`

- [ ] **Step 1: Update QueryData signature**

Find `func (a *InfluxDBV1Adapter) QueryData` and add `cfg *types.QueryConfig` parameter.

- [ ] **Step 2: Update batch size logic**

Change:
```go
batchSize := 10000
```

To:
```go
batchSize := 10000
if cfg != nil && cfg.BatchSize > 0 {
    batchSize = cfg.BatchSize
}
```

- [ ] **Step 3: Run build to verify**

Run: `go build ./internal/adapter/source/...`
Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add internal/adapter/source/influxdb.go
git commit -m "feat: influxdb v1 source accepts QueryConfig for batch size"
```

---

## Task 3: Update InfluxDB V2 Source

**Files:**
- Modify: `internal/adapter/source/influxdb.go`

- [ ] **Step 1: Update QueryData signature**

Find `func (a *InfluxDBV2Adapter) QueryData` and add `cfg *types.QueryConfig` parameter.

- [ ] **Step 2: Update batch size logic**

Same change as V1 source.

- [ ] **Step 3: Run build to verify**

Run: `go build ./internal/adapter/source/...`
Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add internal/adapter/source/influxdb.go
git commit -m "feat: influxdb v2 source accepts QueryConfig for batch size"
```

---

## Task 4: Update MySQL Source

**Files:**
- Modify: `internal/adapter/source/mysql.go`

- [ ] **Step 1: Update QueryData signature**

Find `func (a *MySQLAdapter) QueryData` and add `cfg *types.QueryConfig` parameter.

- [ ] **Step 2: Update batch size logic**

```go
batchSize := 10000
if cfg != nil && cfg.BatchSize > 0 {
    batchSize = cfg.BatchSize
}
```

- [ ] **Step 3: Run build to verify**

Run: `go build ./internal/adapter/source/...`
Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add internal/adapter/source/mysql.go
git commit -m "feat: mysql source accepts QueryConfig for batch size"
```

---

## Task 5: Update TDengine Source

**Files:**
- Modify: `internal/adapter/source/tdengine.go`

- [ ] **Step 1: Update QueryData signature**

Find `func (a *TDengineAdapter) QueryData` and add `cfg *types.QueryConfig` parameter.

- [ ] **Step 2: Update batch size logic**

TDengine uses internal batching of 1000 for batchFunc calls:
```go
internalBatchSize := 1000
if cfg != nil && cfg.BatchSize > 0 {
    internalBatchSize = cfg.BatchSize
}
```

And update the condition:
```go
if len(records) >= internalBatchSize {
```

- [ ] **Step 3: Run build to verify**

Run: `go build ./internal/adapter/source/...`
Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add internal/adapter/source/tdengine.go
git commit -m "feat: tdengine source accepts QueryConfig for batch size"
```

---

## Task 6: Update Migration Engine

**Files:**
- Modify: `internal/engine/migration.go`

- [ ] **Step 1: Add QueryConfig usage**

Find all calls to `sourceAdapter.QueryData` and add `queryCfg`:

```go
queryCfg := &types.QueryConfig{
    BatchSize: e.config.Migration.ChunkSize,
}
checkpoint, err := sourceAdapter.QueryData(ctx, sourceTable, lastCheckpoint, func(records []types.Record) error {
    return e.processBatch(ctx, task.Mapping, records, targetAdapter)
}, queryCfg)
```

- [ ] **Step 2: Run build to verify**

Run: `go build ./internal/engine/...`
Expected: Build succeeds

- [ ] **Step 3: Commit**

```bash
git add internal/engine/migration.go
git commit -m "feat: engine passes QueryConfig to source adapters"
```

---

## Task 7: Update Mock Adapter for Tests

**Files:**
- Modify: `internal/adapter/registry_test.go`

- [ ] **Step 1: Update mock QueryData signature**

Find the `MockSourceAdapter` and update its `QueryData` method signature to include `cfg *types.QueryConfig`.

- [ ] **Step 2: Run tests**

Run: `go test ./internal/adapter/... -v`
Expected: Tests pass

- [ ] **Step 3: Commit**

```bash
git add internal/adapter/registry_test.go
git commit -m "test: update mock adapter for QueryConfig"
```

---

## Summary

| Task | Description | Files | Status |
|------|-------------|-------|--------|
| 1 | QueryConfig struct | `pkg/types/query_config.go` | - [ ] |
| 2 | InfluxDB V1 | `internal/adapter/source/influxdb.go` | - [ ] |
| 3 | InfluxDB V2 | `internal/adapter/source/influxdb.go` | - [ ] |
| 4 | MySQL | `internal/adapter/source/mysql.go` | - [ ] |
| 5 | TDengine | `internal/adapter/source/tdengine.go` | - [ ] |
| 6 | Migration Engine | `internal/engine/migration.go` | - [ ] |
| 7 | Mock Adapter Tests | `internal/adapter/registry_test.go` | - [ ] |
