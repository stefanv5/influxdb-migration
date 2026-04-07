# Proposal: InfluxDB Batch Series Query

## Summary

Add a new **batch series query mode** for InfluxDB migrations that queries multiple time series (series) in a single request using OR拼接. This reduces the number of query requests from N (number of series) to N/100, significantly improving migration performance.

## Motivation

### Current State

- Each series is queried individually via `QueryData(series)`
- For migrations with thousands of series, this results in thousands of HTTP requests
- `max_series_parallel` controls concurrency but still requires N queries

### Problem

- High latency due to many sequential/round-robin queries
- Inefficient use of network bandwidth
- Long migration times for large-scale time series data

### Use Cases

1. **InfluxDB → InfluxDB migration**: Query 100 series at a time instead of 1
2. **InfluxDB → OpenGemini migration**: Same batch query optimization
3. **Large-scale IoT data migration**: 10,000 series × 100 per batch = 100 queries instead of 10,000

## Proposed Solution

### New Query Mode: `batch`

Add a new `query_mode` configuration that enables batch series querying:

```yaml
influx_to_influx:
  enabled: true
  query_mode: "batch"        # "single" | "batch"
  max_series_per_query: 100  # configurable, not hardcoded
```

### Query Strategy

Instead of:
```go
for _, series := range allSeries {
    QueryData(series)  // N requests
}
```

Use:
```go
batches := PartitionSeries(allSeries, max_series_per_query)
for _, batch := range batches {
    QueryDataBatch(batch)  // N/max_series_per_query requests
}
```

### InfluxDB SQL Example

```sql
-- Single series query (current)
SELECT * FROM measurement WHERE tag='series1' AND time >= '...'

-- Batch series query (new)
SELECT * FROM measurement WHERE
  (tag='series1' AND time >= '...')
  OR (tag='series2' AND time >= '...')
  OR (tag='series3' AND time >= '...')
LIMIT 10000
```

## Scope

### In Scope

- [x] Add `query_mode` config: "single" | "batch"
- [x] Add `max_series_per_query` config (default 100)
- [x] Add `QueryDataBatch` interface method
- [x] Implement `QueryDataBatch` for InfluxDB V1 adapter
- [x] Implement `QueryDataBatch` for InfluxDB V2 adapter
- [x] Engine changes to support batch mode
- [x] Checkpoint handling for batch mode (batch-level, not per-series)

### Out of Scope

- [ ] MySQL source batch query (different SQL syntax)
- [ ] TDengine batch query (different API)
- [ ] Dynamic batch size adjustment
- [ ] Per-series checkpoint tracking

## Backward Compatibility

- `query_mode: "single"` (default) preserves existing behavior
- `QueryDataBatch` is a new method, existing adapters remain functional
- Non-InfluxDB migrations unaffected

## Configuration

```yaml
influx_to_influx:
  enabled: true
  query_mode: "batch"        # "single" | "batch", default: "single"
  max_series_per_query: 100  # 1-1000, default: 100
```

## Success Criteria

- [ ] Batch mode queries multiple series in single request
- [ ] `max_series_per_query` is configurable, not hardcoded
- [ ] Checkpoint tracks batch-level progress (max timestamp)
- [ ] Single mode unchanged and default
- [ ] Only InfluxDB → InfluxDB and InfluxDB → OpenGemini supported
