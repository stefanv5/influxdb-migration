# Proposal: TDengine TimeWindow Configuration

## Summary

Fix the TDengine source adapter's fixed 7-day window by accepting TimeWindow configuration via the QueryConfig parameter added in the batch size fix.

## Motivation

### Current State
- TDengine source uses hardcoded 7-day window: `endTime = lastTS.Add(7 * 24 * time.Hour)`
- Config has `mappings[].time_window` setting but it's not used
- No way to tune the time window size

### Problem
- Fixed 7-day window is too large for high-frequency data (memory issues)
- Fixed 7-day window is too small for low-frequency data (inefficient)
- No adaptation to different data patterns

### Use Cases
1. **High-frequency IoT data**: 1-hour windows to control memory
2. **Daily metrics**: 24-hour windows for efficiency
3. **Long-term historical data**: 7-day windows for bulk migration

## Proposed Solution

Extend `QueryConfig` struct to include TimeWindow:

```go
type QueryConfig struct {
    BatchSize   int
    TimeWindow  time.Duration
}
```

TDengine source will use `TimeWindow` instead of hardcoded 7 days.

## Scope

### In Scope
- [x] Add `TimeWindow` field to `QueryConfig`
- [x] Update TDengine source to use TimeWindow from config
- [x] Update migration engine to pass TimeWindow
- [x] Update tests

### Out of Scope
- [ ] Other source adapters (they don't use time windows)

## Configuration

Uses existing `mappings[].time_window` configuration.

## Success Criteria
- [ ] TDengine source uses TimeWindow from config
- [ ] Default 7 days when TimeWindow is 0
- [ ] Tests pass
