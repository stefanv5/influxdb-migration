# Proposal: Batch Size Configuration via QueryData Config

## Summary

Add an optional `QueryConfig` parameter to the `QueryData` interface to allow passing configuration options like `batch_size` from the migration engine to source adapters. Currently, source adapters hardcode `batchSize := 10000`.

## Motivation

### Current State
- Source adapters hardcode `batchSize := 10000`
- Config has `migration.chunk_size` setting but it's not used by adapters
- No way to tune batch sizes per source type

### Problem
- High-latency sources may need smaller batches
- Low-latency sources could benefit from larger batches
- Memory usage can't be controlled per adapter

### Use Cases
1. **Tune batch size per source type**: MySQL could use 5000, TDengine 15000
2. **Control memory usage**: Large batches can cause OOM on constrained systems
3. **Adapt to network conditions**: High-latency links benefit from smaller batches

## Proposed Solution

### Interface Change

Add optional `QueryConfig` parameter to `QueryData`:

```go
type QueryConfig struct {
    BatchSize int
}

func (a *MySQLAdapter) QueryData(ctx context.Context, table string, lastCheckpoint *Checkpoint, batchFunc func([]Record) error, cfg *QueryConfig) (*Checkpoint, error)
```

### Backward Compatibility
- Make `cfg` parameter optional (nil is valid)
- Use default `batchSize := 10000` when `cfg == nil` or `cfg.BatchSize == 0`

## Scope

### In Scope
- [x] Add `QueryConfig` struct to `pkg/types`
- [x] Update `SourceAdapter` interface
- [x] Update all source adapter implementations
- [x] Update migration engine to pass config
- [x] Update tests

### Out of Scope
- [ ] Per-mapping batch size config (separate issue)
- [ ] Dynamic batch size adjustment

## Configuration

No new configuration needed - uses existing `migration.chunk_size`.

## Success Criteria
- [ ] All source adapters accept QueryConfig parameter
- [ ] Default batch size works when config is nil
- [ ] Engine passes configured chunk_size to adapters
- [ ] Tests pass
