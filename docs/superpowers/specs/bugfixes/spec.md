# Bugfixes Specification

**Date**: 2026-04-19
**Project**: InfluxDB Migration Tool
**Status**: Draft

## Overview

This document specifies the bug fixes for critical and high-priority issues discovered in the migration tool. All fixes follow the principle of maintaining backward compatibility while addressing the identified defects.

---

## P0 Bugs

### P0-1: Batch Mode Checkpoint Resume Failure

**Issue ID**: BUG-001
**Severity**: Critical
**File**: `internal/engine/migration.go`
**Lines**: 566-604

**Description**:
In batch mode, when a migration is interrupted and resumed, the system does not correctly skip already processed batches. The `lastCheckpoint` variable is passed to `QueryDataBatch` but never used to determine which batches have been completed. This results in data duplication when resuming.

**Expected Behavior**:
- When resuming an interrupted batch mode migration, already completed batches should be skipped
- Checkpoint should track the last completed batch index, not just timestamp

**Acceptance Criteria**:
- [ ] Resume from interruption point, not from beginning
- [ ] No duplicate data after resume
- [ ] Checkpoint correctly stores and loads batch progress

---

### P0-2: Timer Resource Leak

**Issue ID**: BUG-002
**Severity**: Critical
**File**: `internal/engine/migration.go`
**Lines**: 1009-1016

**Description**:
In `queryWithTimeRange`, a new `time.Timer` is created in each loop iteration without proper cleanup in all code paths. When context is cancelled between loop iterations, the timer may leak.

**Expected Behavior**:
- Timer should be properly stopped in all code paths
- Long-running migrations should not accumulate leaked timers

**Acceptance Criteria**:
- [ ] No timer leaks during extended migration
- [ ] All timers are stopped when context is cancelled
- [ ] Memory usage remains stable during long migrations

---

## P1 Bugs

### P1-1: V2 Source Tag/Field Parsing Error

**Issue ID**: BUG-003
**Severity**: High
**File**: `internal/adapter/source/influxdb.go`
**Lines**: 579-586 (parseV1Values function)

**Description**:
When using V2 source with V1 compatibility API, all string values are incorrectly treated as tags. However, string values from query results may be field values, not tags. The code lacks distinction between tag keys and field keys.

**Expected Behavior**:
- String values that correspond to tag keys should be stored as tags
- String values that correspond to field keys should be stored as fields
- Tag/field distinction should use `DiscoverTagKeys` result

**Acceptance Criteria**:
- [ ] Tags correctly identified using DiscoverTagKeys
- [ ] String field values preserved as fields, not tags
- [ ] V2 source data integrity maintained

---

### P1-2: Target Adapter Missing basic_auth

**Issue ID**: BUG-004
**Severity**: High
**File**: `internal/engine/migration.go`
**Lines**: 1202-1222

**Description**:
The `targetConfigToMap` function does not include `basic_auth` configuration for InfluxDB V1 target adapter. This causes authentication failures when the target requires basic auth.

**Expected Behavior**:
- V1 target adapter should receive username/password via basic_auth
- Authentication should work with both token and basic_auth configurations

**Acceptance Criteria**:
- [ ] V1 target accepts basic_auth configuration
- [ ] V2 target accepts token configuration
- [ ] Target adapter connects successfully with provided credentials

---

## P2 Bugs

### P2-1: Shard-Group Parallelism Configuration Ignored

**Issue ID**: BUG-005
**Severity**: Medium
**File**: `internal/engine/migration.go`
**Lines**: 706-709

**Description**:
The `ShardParallelism` configuration option in shard-group mode is ignored. All shard groups are processed serially regardless of the configured parallelism value.

**Expected Behavior**:
- Shard groups should be processed in parallel according to `ShardParallelism` config
- Multiple shards should be migrated simultaneously when configured

**Acceptance Criteria**:
- [ ] ShardParallelism=1 processes shards serially
- [ ] ShardParallelism>1 processes shards in parallel
- [ ] No race conditions when processing shards in parallel

---

### P2-2: Retry Without Jitter

**Issue ID**: BUG-006
**Severity**: Medium
**File**: `internal/engine/migration.go`
**Lines**: 1074-1113

**Description**:
When retrying failed operations, the code uses fixed exponential backoff without jitter. Multiple instances failing simultaneously will retry at exactly the same time, causing thundering herd problem.

**Expected Behavior**:
- Each retry should have random jitter to spread load
- Multiple instances should not retry at exactly the same moment

**Acceptance Criteria**:
- [ ] Jitter value is random within 50-150% of base delay
- [ ] No thundering herd on recovery
- [ ] Retry timing follows exponential backoff with jitter (EBOJ)

---

### P2-3: DiscoverSeries Without Pagination

**Issue ID**: BUG-007
**Severity**: Medium
**Files**:
- `internal/adapter/source/influxdb.go` (V1 adapter, lines 152-171)
- `internal/adapter/source/influxdb.go` (V2 adapter, lines 940-960)

**Description**:
The `DiscoverSeries` method returns all series without pagination. For databases with large numbers of series (100K+), this can cause OOM errors.

**Expected Behavior**:
- Series discovery should use pagination for large result sets
- Memory usage should remain bounded regardless of series count

**Acceptance Criteria**:
- [ ] Discovery uses LIMIT and key-based pagination
- [ ] Memory usage is O(batch_size), not O(total_series)
- [ ] Backward compatible with older InfluxDB versions

---

## P3 Issues

### P3-1: Duplicate Null Filtering Code

**Issue ID**: CODE-001
**Severity**: Low
**Files**:
- `internal/engine/transform.go` (lines 177-194)
- `internal/engine/migration.go` (lines 1120-1137)

**Description**:
`TransformEngine.FilterNulls` and `filterNilValues` perform identical functionality but are implemented separately.

**Expected Behavior**:
- Single implementation of null filtering
- No code duplication

**Acceptance Criteria**:
- [ ] Single FilterNulls method in TransformEngine
- [ ] migration.go uses TransformEngine.FilterNulls

---

### P3-2: Checkpoint Save Failure Handling

**Issue ID**: CODE-002
**Severity**: Low
**File**: `internal/engine/migration.go`
**Lines**: 432-435

**Description**:
When checkpoint save fails, only a warning is logged and execution continues. This can lead to data loss on crash. No configuration option exists to fail fast.

**Expected Behavior**:
- Checkpoint save failures should be configurable
- Production deployments may want fail-fast behavior

**Acceptance Criteria**:
- [ ] New config option `FailOnCheckpointError`
- [ ] Default behavior unchanged (warn and continue)
- [ ] When enabled, migration fails on checkpoint error

---

### P3-3: Config Comment Errors

**Issue ID**: DOC-001
**Severity**: Low
**File**: `pkg/types/config.go`
**Lines**: 139-148

**Description**:
Comments for `InfluxDBConfig` fields are misleading. Token is marked deprecated for V2 source, but V2 source using V1 API needs username/password, not token.

**Expected Behavior**:
- Comments accurately describe field usage

**Acceptance Criteria**:
- [ ] Comments clarify V1 vs V2 API usage
- [ ] No misleading "deprecated" markers

---

## Non-Functional Requirements

### Backward Compatibility
All fixes MUST maintain backward compatibility with existing configurations and workflows.

### Testing
- Unit tests for each fix
- Integration tests for checkpoint resume behavior
- No regression in existing test coverage

### Performance
- Timer fix should not introduce measurable overhead
- Pagination should improve memory usage for large deployments

---

## Out of Scope

- Changes to the adapter interface
- Database schema migrations for checkpoint store
- Configuration file format changes
- CLI command changes
