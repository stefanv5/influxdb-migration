# Proposal: Shard Group-Aware Migration for InfluxDB

## Summary

Add **shard group-aware migration** optimization that discovers and queries series per shard group instead of loading all series into memory at once. This prevents OOM issues on large-scale InfluxDB migrations with high series cardinality.

## Motivation

### Current State

- `DiscoverSeries` loads **all series** into memory at once
- For InfluxDB instances with 100k+ series, this causes OOM
- `SHOW SERIES` query across all shard groups is expensive and slow
- Memory usage is O(series_count), unbounded

### Problem

```
Original Batch Mode Flow:
  1. DiscoverSeries(measurement) → loads ALL series into memory
     └── 100,000 series × ~100 bytes/series = ~10MB+ memory spike
  2. PartitionSeries(all, 100) → 1000 batches
  3. For each batch: QueryDataBatch + WriteBatch
  4. Checkpoint per task (not per batch progress)
```

**Problems**:
1. OOM on large series count
2. `SHOW SERIES` across all time ranges is slow
3. If `SHOW SERIES` succeeds but memory is exhausted during partitioning, partial state
4. Checkpoint doesn't track per-batch progress

### Use Cases

1. **Large-scale IoT data**: 50k+ devices, each with multiple measurements
2. **Multi-tenant SaaS**: Shared InfluxDB with 10k+ customer series
3. **Long retention periods**: Series scattered across many shard groups

## Proposed Solution

### Core Idea: Per-Shard-Group Series Discovery + Streaming

```
Instead of:
  DiscoverSeries(all) → partition → query

Use:
  For each ShardGroup:
    DiscoverSeries(shard_group_time_range) → batch query → checkpoint
```

### Key Changes

1. **Add `ShowShardGroups()` method** to query InfluxDB shard group metadata
2. **Modify `DiscoverSeries` to accept time range** for targeted discovery
3. **Add streaming batch iterator** to process series in chunks without full memory load
4. **Add ShardGroup-level Checkpoint** to track progress per shard group

## Scope

### In Scope

- [ ] Add `ShowShardGroups()` to InfluxDB V1/V2 adapters
- [ ] Add time-range parameter to `DiscoverSeries()`
- [ ] Create `ShardGroupBatchIterator` for streaming series processing
- [ ] Implement shard-group-aware migration engine path
- [ ] Add `ShardGroupCheckpoint` type and storage
- [ ] Handle shard group time range intersection with query range
- [ ] Update checkpoint manager to support shard group checkpoints

### Out of Scope

- [ ] Changes to non-InfluxDB source adapters (MySQL, TDengine)
- [ ] Dynamic shard group detection during migration (use initial discovery)
- [ ] Shard group rebalancing handling (assume static during migration)

## Backward Compatibility

- `DiscoverSeries()` without time range still works (returns all series for backward compatibility)
- Existing batch mode continues to work for small/medium series counts
- New shard-group-aware mode is opt-in via configuration

## Configuration

```yaml
influx_to_influx:
  enabled: true
  query_mode: "batch"           # "single" | "batch" | "shard-group"
  max_series_per_query: 100
  shard_group_mode:
    enabled: true
    series_batch_size: 50       # series per batch within shard group
```

## Success Criteria

- [ ] Migration of 100k+ series without OOM
- [ ] `SHOW SERIES` only executed per relevant shard group (not all time)
- [ ] Memory usage bounded by `series_batch_size`, not total series count
- [ ] Crash recovery resumes at correct shard group and batch
- [ ] At-least-once semantics maintained (no data loss)

## Open Questions

1. **Shard group discovery frequency**: Once at start or periodically during migration?
2. **Series migration order**: Within a shard group, what order to process batches?
3. **Cross-shard series handling**: A series may have data in multiple shard groups - how to handle time range splits?
