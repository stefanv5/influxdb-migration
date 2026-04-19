# Bugfixes Task List

**Date**: 2026-04-19
**Project**: InfluxDB Migration Tool
**Related Spec**: spec.md
**Related Design**: design.md

---

## Task Checklist

### P0 Bugs

#### TASK-BUG-001: Fix Batch Mode Checkpoint Resume
**Issue**: BUG-001
**Files**: `internal/engine/migration.go`
**Estimated Time**: 2 hours

- [ ] **TASK-BUG-001-1**: Load checkpoint before batch loop in `runTaskBatchMode`
- [ ] **TASK-BUG-001-2**: Track `lastCompletedBatch` index from checkpoint
- [ ] **TASK-BUG-001-3**: Add skip logic for completed batches (`if i <= lastCompletedBatch`)
- [ ] **TASK-BUG-001-4**: Save checkpoint with batch index after each batch completes
- [ ] **TASK-BUG-001-5**: Add unit test for batch resume scenario
- [ ] **TASK-BUG-001-6**: Run existing tests to verify no regression

#### TASK-BUG-002: Fix Timer Resource Leak
**Issue**: BUG-002
**Files**: `internal/engine/migration.go`
**Estimated Time**: 1 hour

- [ ] **TASK-BUG-002-1**: Add `defer timer.Stop()` before select statement
- [ ] **TASK-BUG-002-2**: Check ctx before creating timer in loop
- [ ] **TASK-BUG-002-3**: Add test to verify no timer leaks after cancellation
- [ ] **TASK-BUG-002-4**: Run existing tests to verify no regression

---

### P1 Bugs

#### TASK-BUG-003: Fix V2 Source Tag/Field Parsing
**Issue**: BUG-003
**Files**: `internal/adapter/source/influxdb.go`
**Estimated Time**: 3 hours

- [ ] **TASK-BUG-003-1**: Add `parseV1ValuesWithTagKeys` function that accepts tagKeySet
- [ ] **TASK-BUG-003-2**: Modify `executeV1SelectQuery` to accept and pass tagKeys
- [ ] **TASK-BUG-003-3**: Update `QueryDataBatch` for V2 adapter to fetch and pass tagKeys
- [ ] **TASK-BUG-003-4**: Add unit test with mocked tagKeys
- [ ] **TASK-BUG-003-5**: Run existing tests to verify no regression

#### TASK-BUG-004: Fix Target Adapter basic_auth
**Issue**: BUG-004
**Files**: `internal/engine/migration.go`
**Estimated Time**: 1 hour

- [ ] **TASK-BUG-004-1**: Update `targetConfigToMap` to include basic_auth for V1 targets
- [ ] **TASK-BUG-004-2**: Include retention_policy in V1 target config
- [ ] **TASK-BUG-004-3**: Test V1 target connection with basic_auth
- [ ] **TASK-BUG-004-4**: Run existing tests to verify no regression

---

### P2 Bugs

#### TASK-BUG-005: Implement Shard-Group Parallelism
**Issue**: BUG-005
**Files**: `internal/engine/migration.go`
**Estimated Time**: 2 hours

- [ ] **TASK-BUG-005-1**: Add semaphore-based concurrency in `runTaskShardGroupMode`
- [ ] **TASK-BUG-005-2**: Implement goroutine-per-shard with ShardParallelism limit
- [ ] **TASK-BUG-005-3**: Add error collection and first-error-return logic
- [ ] **TASK-BUG-005-4**: Add unit test for parallel execution
- [ ] **TASK-BUG-005-5**: Run existing tests to verify no regression

#### TASK-BUG-006: Add Jitter to Retry Logic
**Issue**: BUG-006
**Files**: `internal/engine/migration.go`
**Estimated Time**: 1 hour

- [ ] **TASK-BUG-006-1**: Add `math/rand` import
- [ ] **TASK-BUG-006-2**: Implement jitter calculation in `writeWithRetry`
- [ ] **TASK-BUG-006-3**: Add unit test to verify jitter range
- [ ] **TASK-BUG-006-4**: Run existing tests to verify no regression

#### TASK-BUG-007: Add Pagination to DiscoverSeries
**Issue**: BUG-007
**Files**: `internal/adapter/source/influxdb.go`
**Estimated Time**: 2 hours

- [ ] **TASK-BUG-007-1**: Implement paginated `DiscoverSeries` for V1 adapter
- [ ] **TASK-BUG-007-2**: Implement paginated `DiscoverSeries` for V2 adapter
- [ ] **TASK-BUG-007-3**: Add fallback for older InfluxDB without series_key filter
- [ ] **TASK-BUG-007-4**: Add unit test with mocked pagination
- [ ] **TASK-BUG-007-5**: Run existing tests to verify no regression

---

### P3 Issues

#### TASK-CODE-001: Remove Duplicate Null Filtering Code
**Issue**: CODE-001
**Files**: `internal/engine/transform.go`, `internal/engine/migration.go`
**Estimated Time**: 30 minutes

- [ ] **TASK-CODE-001-1**: Verify `TransformEngine.FilterNulls` implementation
- [ ] **TASK-CODE-001-2**: Replace `filterNilValues` calls in migration.go with `e.transformer.FilterNulls`
- [ ] **TASK-CODE-001-3**: Run existing tests to verify no regression

#### TASK-CODE-002: Add FailOnCheckpointError Config
**Issue**: CODE-002
**Files**: `pkg/types/config.go`, `internal/config/validator.go`, `internal/engine/migration.go`
**Estimated Time**: 1 hour

- [ ] **TASK-CODE-002-1**: Add `FailOnCheckpointError` field to `MigrationSettings`
- [ ] **TASK-CODE-002-2**: Add default value in `ApplyDefaults`
- [ ] **TASK-CODE-002-3**: Implement fail-fast logic in checkpoint save error handling
- [ ] **TASK-CODE-002-4**: Run existing tests to verify no regression

#### TASK-DOC-001: Fix Config Comments
**Issue**: DOC-001
**Files**: `pkg/types/config.go`
**Estimated Time**: 15 minutes

- [ ] **TASK-DOC-001-1**: Update `InfluxDBConfig` field comments
- [ ] **TASK-DOC-001-2**: Verify comment accuracy

---

## Execution Order

**Phase 1: P0 Bugs (Critical)**
1. TASK-BUG-001: Batch Mode Checkpoint Resume
2. TASK-BUG-002: Timer Resource Leak

**Phase 2: P1 Bugs (High)**
3. TASK-BUG-003: V2 Source Tag/Field Parsing
4. TASK-BUG-004: Target Adapter basic_auth

**Phase 3: P2 Bugs (Medium)**
5. TASK-BUG-005: Shard-Group Parallelism
6. TASK-BUG-006: Retry With Jitter
7. TASK-BUG-007: DiscoverSeries Pagination

**Phase 4: P3 Issues (Low)**
8. TASK-CODE-001: Remove Duplicate Code
9. TASK-CODE-002: Add FailOnCheckpointError Config
10. TASK-DOC-001: Fix Config Comments

---

## Verification Commands

```bash
# Run all tests
go test ./... -v

# Run with race detector
go test -race ./...

# Run specific package tests
go test ./internal/engine/... -v
go test ./internal/adapter/... -v

# Build to verify no compilation errors
go build -o migrate ./cmd/migrate
```

---

## Estimated Total Time

| Phase | Tasks | Estimated Time |
|-------|-------|----------------|
| P0 | 2 | 3 hours |
| P1 | 2 | 4 hours |
| P2 | 3 | 5 hours |
| P3 | 3 | 2 hours |
| **Total** | **10** | **14 hours** |

---

## Dependencies

- TASK-BUG-003 requires no other tasks
- TASK-BUG-004 requires no other tasks
- All other tasks can proceed independently
- All tasks require passing existing test suite after changes
