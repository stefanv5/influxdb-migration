# Design: V2 Source Tag Preservation & Series Discovery Fix

## 1. Overview

修复 InfluxDB V2 Source Adapter 的三个严重问题：
1. `executeFluxSelect` 丢失 Tags
2. `DiscoverSeries` 返回 Tag Keys 而非 Series Keys
3. `DiscoverSeriesInTimeWindow` 是占位实现

## 2. Architecture

### 2.1 V2 Series Discovery 流程

```
┌─────────────────────────────────────────────────────────────────────┐
│                   V2 Series Discovery                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. DiscoverTagKeys(measurement)                                     │
│     └── schema.tagKeys(bucket, measurement)                         │
│         → ["host", "region"]                                        │
│                           │                                          │
│                           ▼                                          │
│  2. For each tag key, DiscoverTagValues                             │
│     └── schema.tagValues(bucket, tag, measurement)                  │
│         → host: ["server1", "server2"]                             │
│         → region: ["us-west", "us-east"]                            │
│                           │                                          │
│                           ▼                                          │
│  3. Cartesian Product → Full Series Keys                             │
│     → "cpu,host=server1,region=us-west"                             │
│     → "cpu,host=server1,region=us-east"                             │
│     → "cpu,host=server2,region=us-west"                             │
│     → "cpu,host=server2,region=us-east"                             │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Tag Preservation 流程

```
┌─────────────────────────────────────────────────────────────────────┐
│                   Tag Preservation Flow                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. migrateTimeWindow 开始时:                                         │
│     tagKeys = DiscoverTagKeys(measurement)                           │
│                           │                                          │
│                           ▼                                          │
│  2. QueryDataBatch 调用时:                                            │
│     executeFluxSelect(query, tagKeys)                                │
│                           │                                          │
│                           ▼                                          │
│  3. executeFluxSelect 解析:                                           │
│     - 解析 JSON 到 map[string]interface{}                             │
│     - 构建 tagKeys 集合: {"host": true, "region": true}              │
│     - 遍历字段:                                                       │
│       if tagKeys[fieldName] → record.AddTag(fieldName, value)        │
│       else → record.AddField(fieldName, value)                       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## 3. Interface Changes

### 3.1 QueryConfig

```go
// pkg/types/query_config.go
type QueryConfig struct {
    BatchSize         int
    TimeWindow        time.Duration
    MaxSeriesPerQuery int
    TagKeys           []string  // NEW
}
```

### 3.2 SourceAdapter Interface

```go
// internal/adapter/adapter.go
type SourceAdapter interface {
    // ... existing methods ...

    // NEW: DiscoverTagKeys returns all tag key names for a measurement
    // Used by executeFluxSelect to distinguish tags from fields
    DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error)
}
```

## 4. Implementation Details

### 4.1 DiscoverTagKeys (V2)

```go
func (a *InfluxDBV2Adapter) DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error) {
    fluxQuery := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.tagKeys(bucket: "%s", measurement: "%s")`, a.config.Bucket, measurement)

    results, err := a.executeFluxQuery(ctx, fluxQuery)
    if err != nil {
        return nil, err
    }

    var tagKeys []string
    for _, r := range results {
        for _, v := range r {
            if arr, ok := v.([]interface{}); ok && len(arr) > 0 {
                if key, ok := arr[0].(string); ok {
                    tagKeys = append(tagKeys, key)
                }
            }
        }
    }
    return tagKeys, nil
}
```

### 4.2 DiscoverSeries (V2 - 重写)

```go
func (a *InfluxDBV2Adapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
    // Step 1: Get all tag keys
    tagKeys, err := a.DiscoverTagKeys(ctx, measurement)
    if err != nil {
        return nil, fmt.Errorf("failed to get tag keys: %w", err)
    }

    if len(tagKeys) == 0 {
        return []string{measurement}, nil
    }

    // Step 2: Get values for each tag key
    tagValues := make(map[string][]string)
    for _, key := range tagKeys {
        valuesQuery := fmt.Sprintf(`import "influxdata/influxdb/schema"
schema.tagValues(bucket: "%s", tag: "%s", measurement: "%s")`,
            a.config.Bucket, key, measurement)

        results, err := a.executeFluxQuery(ctx, valuesQuery)
        if err != nil {
            logger.Warn("failed to get tag values, skipping", zap.String("tag", key), zap.Error(err))
            continue
        }

        for _, r := range results {
            for _, v := range r {
                if arr, ok := v.([]interface{}); ok && len(arr) > 0 {
                    if val, ok := arr[0].(string); ok {
                        tagValues[key] = append(tagValues[key], val)
                    }
                }
            }
        }
    }

    // Step 3: Cartesian product
    var seriesKeys []string
    var generate func(idx int, parts []string)
    generate = func(idx int, parts []string) {
        if idx == len(tagKeys) {
            seriesKeys = append(seriesKeys, measurement+","+strings.Join(parts, ","))
            return
        }
        key := tagKeys[idx]
        values := tagValues[key]
        if len(values) == 0 {
            generate(idx+1, parts)
            return
        }
        for _, v := range values {
            newParts := append(parts, fmt.Sprintf("%s=%s", key, v))
            generate(idx+1, newParts)
        }
    }
    generate(0, []string{})

    return seriesKeys, nil
}
```

### 4.3 executeFluxSelect (修改)

```go
func (a *InfluxDBV2Adapter) executeFluxSelect(ctx context.Context, query string, tagKeys []string) ([]types.Record, error) {
    params, err := json.Marshal(map[string]string{"query": query})
    if err != nil {
        return nil, fmt.Errorf("failed to marshal query: %w", err)
    }

    u, err := url.Parse(a.config.URL)
    if err != nil {
        return nil, fmt.Errorf("invalid URL: %w", err)
    }
    u.Path = "/api/v2/query"

    req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(params))
    if err != nil {
        return nil, err
    }

    req.Header.Set("Authorization", "Token "+a.config.Token)
    req.Header.Set("Content-Type", "application/json")

    resp, err := a.client.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("flux query failed with status %d: %s", resp.StatusCode, string(body))
    }

    // Decode into generic map to capture all fields including tags
    var rawResults []map[string]interface{}
    if err := json.NewDecoder(resp.Body).Decode(&rawResults); err != nil {
        return nil, fmt.Errorf("failed to decode flux result: %w", err)
    }

    // Build tagKeys set for O(1) lookup
    tagKeySet := make(map[string]bool)
    for _, k := range tagKeys {
        tagKeySet[k] = true
    }

    var records []types.Record
    for _, r := range rawResults {
        record := types.NewRecord()

        // Extract _time
        if t, ok := r["_time"].(string); ok {
            if tm, err := time.Parse(time.RFC3339Nano, t); err == nil {
                record.Time = tm.UnixNano()
            }
        }

        // Extract _measurement as tag
        if m, ok := r["_measurement"].(string); ok {
            record.AddTag("_measurement", m)
        }

        // Extract _field and _value as field
        if f, ok := r["_field"].(string); ok {
            if v, ok := r["_value"]; ok {
                record.AddField(f, v)
            }
        }

        // Distinguish tags from fields
        // System fields: _time, _measurement, _field, _value
        // Tag fields: any field whose name is in tagKeys
        // Other fields: regular fields (float, int, bool)
        for k, v := range r {
            switch k {
            case "_time", "_measurement", "_field", "_value":
                continue
            }
            if tagKeySet[k] {
                // This is a tag - convert value to string
                record.AddTag(k, fmt.Sprintf("%v", v))
            } else if k != "_field" && k != "_value" {
                // This is a regular field
                record.AddField(k, v)
            }
        }

        records = append(records, *record)
    }

    return records, nil
}
```

### 4.4 QueryDataBatch 修改

`QueryDataBatch` 需要接收 `TagKeys` 并传递给 `executeFluxSelect`：

```go
func (a *InfluxDBV2Adapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
    batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {

    // ... existing code for startTime handling ...

    batchSize := getBatchSize(cfg)
    fluxFilter := BuildFluxFilter(series)

    var totalRecords int
    var maxTS int64

    for {
        fluxQuery := fmt.Sprintf(`
            from(bucket: "%s")
              |> range(start: %s, stop: %s)
              |> filter(fn: (r) => r._measurement == "%s" and (%s))
              |> limit(n: %d)
        `, a.config.Bucket,
            queryStart.Format(time.RFC3339Nano),
            endTime.Format(time.RFC3339Nano),
            measurement, fluxFilter, batchSize)

        // Pass tagKeys to executeFluxSelect
        records, err := a.executeFluxSelectWithContext(ctx, fluxQuery, cfg.TagKeys)
        if err != nil {
            return nil, fmt.Errorf("batch query failed: %w", err)
        }

        // ... rest of function ...
    }
}

// executeFluxSelectWithContext wraps executeFluxSelect with context support
func (a *InfluxDBV2Adapter) executeFluxSelectWithContext(ctx context.Context, query string, tagKeys []string) ([]types.Record, error) {
    // Reuse existing executeFluxSelect logic
    return a.executeFluxSelect(query, tagKeys)
}
```

## 5. Migration Engine Changes

### 5.1 migrateShardGroup - 在 shard group 级别获取 tag keys

Tag keys 按 **shard group 级别获取一次**，不是每个 time window 获取一次：

```go
func (e *MigrationEngine) migrateShardGroup(ctx context.Context, task *MigrationTask,
    sg *adapter.ShardGroup, sourceAdapter adapter.SourceAdapter,
    targetAdapter adapter.TargetAdapter, queryStart, queryEnd time.Time) error {

    start, end := ShardGroupEffectiveTimeRange(sg, queryStart, queryEnd)

    // 获取 tag keys（按 shard group 级别获取一次）
    var tagKeys []string
    if v2Adapter, ok := sourceAdapter.(*adapter.InfluxDBV2Adapter); ok {
        keys, err := v2Adapter.DiscoverTagKeys(ctx, task.Mapping.SourceTable)
        if err != nil {
            logger.Warn("failed to discover tag keys",
                zap.Int("shard_id", sg.ID),
                zap.Error(err))
        } else {
            tagKeys = keys
            logger.Info("discovered tag keys for shard group",
                zap.Int("shard_id", sg.ID),
                zap.Int("tag_key_count", len(tagKeys)))
        }
    }

    // Split into time windows
    timeWindow := ...
    windows := SplitTimeWindows(start, end, timeWindow)

    // Process each time window - 传递 tagKeys
    for _, window := range windows {
        if err := e.migrateTimeWindow(ctx, task, sg, window, sourceAdapter,
            targetAdapter, queryStart, queryEnd, tagKeys); err != nil {
            return err
        }
    }
    // ...
}
```

### 5.2 migrateTimeWindow - 接收 tagKeys 参数

```go
func (e *MigrationEngine) migrateTimeWindow(ctx context.Context, task *MigrationTask,
    sg *adapter.ShardGroup, window TimeWindow,
    sourceAdapter adapter.SourceAdapter, targetAdapter adapter.TargetAdapter,
    queryStart, queryEnd time.Time, tagKeys []string) error {  // NEW: tagKeys param

    // ... existing checkpoint resume logic ...

    // Discover series in this time window
    series, err := sourceAdapter.DiscoverSeriesInTimeWindow(ctx,
        task.Mapping.SourceTable, window.Start, window.End)
    // ...

    // Process each batch - 传递 tagKeys 到 QueryConfig
    for batchIdx, batch := range batches {
        // ...
        batchCheckpoint, err := sourceAdapter.QueryDataBatch(ctx,
            task.Mapping.SourceTable, batch, window.Start, window.End,
            &types.Checkpoint{LastTimestamp: lastTimestamp},
            func(records []types.Record) error {
                return e.processBatch(ctx, task.Mapping, records, targetAdapter)
            },
            &types.QueryConfig{
                BatchSize: e.config.Migration.ChunkSize,
                TagKeys:   tagKeys,  // Pass tag keys
            },
        )
        // ...
    }
}
```

**关键点**: 同一个 shard group 内的所有 time window 共享相同的 tag keys，避免重复查询。

## 6. V1 Adapter Impact

V1 adapter 不需要 `DiscoverTagKeys` 方法。可以：
1. 在接口中定义为可选方法
2. 或让 V1 adapter 返回 `nil` 作为"不支持"标志

**推荐**: 接口保持简洁，V2 实现 `DiscoverTagKeys`，V1 返回空 slice。调用方检查返回空 slice 时跳过 tag 相关逻辑。

## 7. Error Handling

### 7.1 Tag Discovery Failure

如果 `DiscoverTagKeys` 失败：
- 记录 warning 日志
- 继续处理，tagKeys 为空 slice
- 所有 string 类型字段会被当作 field（可能不是预期行为，但不阻塞迁移）

### 7.2 Tag Values Discovery Failure

如果某个 tag key 的 values 查询失败：
- 跳过该 tag key
- 不阻塞整个 series discovery

## 8. Performance Considerations

### 8.1 Series Discovery Cardinality

完整笛卡尔积可能产生大量 series keys：
- 3 个 tag keys，每个 1000 个 values → 10亿 series keys

**缓解措施**:
1. 在配置中添加 `MaxTagValuesPerKey` 限制
2. 警告日志当 series keys 数量超过阈值

### 8.2 Multiple API Calls

Series discovery 需要多次 API 调用：
- 1次 `schema.tagKeys`
- N次 `schema.tagValues`（N = tag key 数量）

**可接受**: 迁移是离线操作，这些调用开销可忽略。

## 9. Files Summary

| File | Changes |
|------|---------|
| `pkg/types/query_config.go` | Add `TagKeys []string` field |
| `internal/adapter/adapter.go` | Add `DiscoverTagKeys` method to interface |
| `internal/adapter/source/influxdb.go` | Implement V2: DiscoverTagKeys, DiscoverSeries (rewrite), executeFluxSelect (modify to use tagKeys) |
| `internal/engine/migration.go` | Pass tagKeys to QueryDataBatch |

## 10. Open Issues

1. **String Fields**: 无法区分 string type field 和 tag。后续需要显式配置。
2. **Cardinality Explosion**: 没有限制 series keys 数量。可能导致 OOM。
3. **V1 Incompatibility**: V1 adapter 需要类似方法吗？当前场景不需要 V2 → V1。
