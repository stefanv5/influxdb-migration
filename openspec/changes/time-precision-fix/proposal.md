# Proposal: Time Precision Fix - Nanosecond Timestamp Support

## Summary

Fix the time precision issue in the migration tool by changing `Record.Time` from `time.Time` (RFC3339, millisecond precision) to `int64` (Unix nanoseconds). This ensures nanosecond-level precision is preserved throughout the migration pipeline, particularly important for TDengine and high-frequency InfluxDB data.

## Motivation

### Current State
- `Record.Time` is typed as `time.Time` (Go standard library)
- Source adapters parse timestamps from database responses as strings
- InfluxDB stores timestamps as int64 nanoseconds internally
- TDengine supports nanosecond precision timestamps

### Problem
- RFC3339 format only supports millisecond precision (e.g., `2024-01-15T10:30:00.123Z`)
- Parsing and reformatting through RFC3339 loses nanosecond precision
- High-frequency data (sub-millisecond intervals) cannot be accurately migrated
- TDengine's nanosecond capability is underutilized

### Use Cases
1. **High-frequency IoT data**: Sensors emitting data at >1000Hz
2. **Financial tick data**: Market data with microsecond requirements
3. **Scientific instrumentation**: Laboratory measurements at high sampling rates

## Proposed Solution

### Core Change
```go
// Before
type Record struct {
    Time time.Time
}

// After
type Record struct {
    Time int64  // Unix timestamp in nanoseconds
}
```

### Rationale
1. **Precision preserved**: int64 nanoseconds have full range for any database timestamp
2. **Database native format**: All databases (InfluxDB, TDengine, MySQL) use int64 internally
3. **Performance**: Avoid string parsing/formatting overhead
4. **Consistency**: Single time representation throughout the pipeline

## Impact Analysis

### Affected Components

| Component | Files | Change Type |
|-----------|-------|-------------|
| Record type | `pkg/types/record.go` | Type change |
| Source adapters | `internal/adapter/source/*.go` | Time parsing |
| Target adapters | `internal/adapter/target/*.go` | Time formatting |
| Transform engine | `internal/engine/transform.go` | Time operations |
| Checkpoint | `pkg/types/checkpoint.go` | Timestamp field |
| Tests | All *_test.go files | Update assertions |

### Backward Compatibility
- **Breaking change**: Any code accessing `record.Time` directly will break
- Migration path: Update all usages to handle int64 instead of time.Time

## Scope

### In Scope
- [x] Change Record.Time to int64
- [x] Update all source adapters to parse to int64 nanoseconds
- [x] Update all target adapters to format from int64
- [x] Update checkpoint timestamp handling
- [x] Update transform engine time operations
- [x] Add unit tests for time handling
- [x] Update existing tests

### Out of Scope
- [ ] Time zone handling (UTC internal representation)
- [ ] Batch size configuration (separate issue)
- [ ] TimeWindow configuration (separate issue)

## Configuration Changes

No configuration changes required - this is an internal type change.

## Success Criteria
- [ ] All source adapters correctly parse timestamps to int64 nanoseconds
- [ ] All target adapters correctly format int64 to database timestamp
- [ ] Existing tests pass or updated
- [ ] No precision loss in migration pipeline
- [ ] TDengine nanosecond timestamps preserved

## Risks

| Risk | Mitigation |
|------|------------|
| Breaking change to Record interface | Clear documentation, migration guide |
| Performance regression | Benchmark before/after |
| Time zone handling complexity | Use UTC internally, convert at boundaries |

## Dependencies

None - this is a pure refactoring with no new external dependencies.

## Timeline

Single iteration, estimated 2-4 hours for implementation and testing.
