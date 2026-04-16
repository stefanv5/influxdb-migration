# Tasks: V2 Source Tag Preservation & Series Discovery Fix

## Task List

- [x] **Task 1**: 添加 `DiscoverTagKeys` 方法到 V2 SourceAdapter
- [x] **Task 2**: 添加 `TagKeys` 字段到 `QueryConfig`
- [x] **Task 3**: 修改 `executeFluxSelect` 保留 Tags
- [x] **Task 4**: 重写 V2 `DiscoverSeries` 为完整 series keys 实现
- [x] **Task 5**: 重写 V2 `DiscoverSeriesInTimeWindow`
- [x] **Task 6**: 在 migrateTimeWindow 中集成 tag keys 获取
- [x] **Task 7**: 添加单元测试 (QueryConfig TagKeys tests, existing tests pass)
- [ ] **Task 8**: 端到端验证 (requires real V2 instance)

---

## Task 1: 添加 `DiscoverTagKeys` 方法到 V2 SourceAdapter

**文件**: `internal/adapter/source/influxdb.go`

**实现**:
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

**验收标准**:
- 返回该 measurement 的所有 tag key 名称列表
- V1 adapter 不需要此方法（返回空或 nil）

---

## Task 2: 添加 `TagKeys` 字段到 `QueryConfig`

**文件**: `pkg/types/query_config.go`

**修改**:
```go
type QueryConfig struct {
    BatchSize         int
    TimeWindow        time.Duration
    MaxSeriesPerQuery int
    TagKeys           []string  // NEW: 用于区分 tag 和 field
}
```

**验收标准**:
- 新字段可序列化和反序列化
- 默认值为空切片，不影响现有逻辑

---

## Task 3: 修改 `executeFluxSelect` 保留 Tags

**文件**: `internal/adapter/source/influxdb.go`

**修改要点**:
1. 接收 `tagKeys []string` 参数
2. 改为解析成 `[]map[string]interface{}` 接收所有字段
3. 使用 tagKeys 集合判断字段是 tag 还是 field

```go
func (a *InfluxDBV2Adapter) executeFluxSelect(ctx context.Context, query string, tagKeys []string) ([]types.Record, error) {
    // ... 发送请求 ...

    var rawResults []map[string]interface{}
    if err := json.NewDecoder(resp.Body).Decode(&rawResults); err != nil {
        return nil, err
    }

    // 构建 tagKeys 集合用于快速查找
    tagKeySet := make(map[string]bool)
    for _, k := range tagKeys {
        tagKeySet[k] = true
    }

    var records []types.Record
    for _, r := range rawResults {
        record := types.NewRecord()

        // 提取 _time
        if t, ok := r["_time"].(string); ok {
            if tm, err := time.Parse(time.RFC3339Nano, t); err == nil {
                record.Time = tm.UnixNano()
            }
        }

        // 提取 _measurement 作为 tag
        if m, ok := r["_measurement"].(string); ok {
            record.AddTag("_measurement", m)
        }

        // 遍历所有字段，区分 tag 和 field
        for k, v := range r {
            if k == "_time" || k == "_measurement" || k == "_field" || k == "_value" {
                continue
            }
            if tagKeySet[k] {
                record.AddTag(k, fmt.Sprintf("%v", v))  // tag
            } else {
                record.AddField(k, v)  // field
            }
        }

        records = append(records, *record)
    }
    return records, nil
}
```

**注意**: 需要更新所有调用 `executeFluxSelect` 的地方，传入 `tagKeys` 参数。

**验收标准**:
- 查询结果中所有 tag 字段都被保留
- field 字段正确识别
- 不再有数据丢失

---

## Task 4: 重写 V2 `DiscoverSeries` 为完整 series keys 实现

**文件**: `internal/adapter/source/influxdb.go`

**实现**:

```go
func (a *InfluxDBV2Adapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
    // Step 1: 获取所有 tag keys
    tagKeys, err := a.DiscoverTagKeys(ctx, measurement)
    if err != nil {
        return nil, fmt.Errorf("failed to get tag keys: %w", err)
    }

    if len(tagKeys) == 0 {
        // 没有 tags，只有 measurement 本身
        return []string{measurement}, nil
    }

    // Step 2: 获取每个 tag key 的 values
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

    // Step 3: 笛卡尔积组合成 series keys
    var seriesKeys []string
    var helper func(idx int, parts []string)

    helper = func(idx int, parts []string) {
        if idx == len(tagKeys) {
            seriesKeys = append(seriesKeys, measurement+","+strings.Join(parts, ","))
            return
        }
        key := tagKeys[idx]
        values := tagValues[key]
        if len(values) == 0 {
            // 这个 tag 没有 values，跳过它
            helper(idx+1, parts)
            return
        }
        for _, v := range values {
            newParts := append(parts, fmt.Sprintf("%s=%s", key, v))
            helper(idx+1, newParts)
        }
    }

    helper(0, []string{})
    return seriesKeys, nil
}
```

**验收标准**:
- 返回格式: `["cpu,host=server1,region=us-west", "cpu,host=server2,region=us-east"]`
- 覆盖所有 tag key-value 组合
- 无数据时返回 `[]string{measurement}`

---

## Task 5: 重写 V2 `DiscoverSeriesInTimeWindow`

**文件**: `internal/adapter/source/influxdb.go`

**实现**:

V2 没有直接的 `SHOW SERIES WHERE time` 支持。方案：
1. 先获取所有 series keys（复用 DiscoverSeries）
2. 然后在 engine 层按时间窗口过滤（因为 series key 本身不包含时间信息）

实际上，series key 格式是 `measurement,tag1=val1,tag2=val2`，它本身不携带时间信息。时间窗口过滤应该在 QueryDataBatch 的查询层面处理。

**简化方案**: `DiscoverSeriesInTimeWindow` 返回所有 series，然后在查询时用时间过滤。

```go
func (a *InfluxDBV2Adapter) DiscoverSeriesInTimeWindow(ctx context.Context, measurement string, startTime, endTime time.Time) ([]string, error) {
    // V2 的 SHOW SERIES 不支持时间过滤
    // 直接返回所有 series，查询时用 range() 的时间参数过滤
    return a.DiscoverSeries(ctx, measurement)
}
```

**注意**: 这个简化方案意味着 DiscoverSeriesInTimeWindow 和 DiscoverSeries 返回相同结果。时间过滤在 QueryDataBatch 中通过 Flux `range(start, stop)` 实现。

**验收标准**:
- 返回完整的 series keys
- 时间窗口过滤由 QueryDataBatch 的查询参数处理

---

## Task 6: 在 migrateShardGroup 中获取 tag keys 并传递给 migrateTimeWindow

**文件**: `internal/engine/migration.go`

**修改**:

Tag keys 的获取应该在 shard group 级别进行，而不是每个 time window 获取一次。**每个 shard group 开始处理前获取一次 tag keys**，然后传递给所有 time window。

### 6.1 修改 migrateShardGroup

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
            logger.Warn("failed to discover tag keys, treating all as fields",
                zap.String("shard_id", fmt.Sprintf("%d", sg.ID)),
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

### 6.2 修改 migrateTimeWindow 签名

```go
func (e *MigrationEngine) migrateTimeWindow(ctx context.Context, task *MigrationTask,
    sg *adapter.ShardGroup, window TimeWindow,
    sourceAdapter adapter.SourceAdapter, targetAdapter adapter.TargetAdapter,
    queryStart, queryEnd time.Time,  // NEW: 添加 query time range 参数
    tagKeys []string) error {       // NEW: tagKeys 参数

    // ... existing code for checkpoint resume ...

    // Discover series in this time window
    series, err := sourceAdapter.DiscoverSeriesInTimeWindow(ctx,
        task.Mapping.SourceTable, window.Start, window.End)
    // ...

    // Process each batch - 传递 tagKeys 到 QueryConfig
    for batchIdx, batch := range batches {
        batchCheckpoint, err := sourceAdapter.QueryDataBatch(ctx,
            task.Mapping.SourceTable,
            batch,
            window.Start,
            window.End,
            &types.Checkpoint{LastTimestamp: lastTimestamp},
            func(records []types.Record) error {
                return e.processBatch(ctx, task.Mapping, records, targetAdapter)
            },
            &types.QueryConfig{
                BatchSize: e.config.Migration.ChunkSize,
                TagKeys:   tagKeys,  // NEW: 传递 tag keys
            },
        )
        // ...
    }
}
```

**验收标准**:
- tag keys 按 shard group 级别获取一次，不是每个 window 获取
- tag keys 正确传递到所有 time window 处理
- 同一个 shard group 内的所有 time window 共享相同的 tag keys

---

## Task 7: 添加单元测试

**文件**: `internal/adapter/source/influxdb_batch_test.go` (新建或扩展)

**测试用例**:

1. `TestV2DiscoverSeries_WithMultipleTagKeys`
   - 输入: measurement 有 2 个 tag keys，每个有 2 个 values
   - 期望: 返回 4 个 series keys

2. `TestV2DiscoverSeries_WithEmptyTags`
   - 输入: measurement 没有 tags
   - 期望: 返回 `["measurementName"]`

3. `TestV2ExecuteFluxSelect_TagFieldDistinction`
   - 输入: Flux 结果有 tag 和 field
   - 期望: tag 正确添加到 record.Tags，field 添加到 record.Fields

4. `TestV2DiscoverTagKeys`
   - 输入: 有效的 measurement
   - 期望: 返回 tag key 名称列表

---

## Task 8: 端到端验证

**步骤**:
1. 启动 V2 实例，写入包含多个 tags 的测试数据
2. 配置 V2 → V1 迁移任务
3. 执行迁移
4. 验证目标库中数据包含所有原始 tags

---

## Dependencies

- Task 1 → Task 2 → Task 3 → Task 6 → Task 7
- Task 4 独立，可以在 Task 3 之后或之前完成
- Task 5 在 Task 4 之后完成
- Task 8 在所有其他任务之后完成

---

## Notes

1. **Cardinality 问题**: 当前实现会产生完整的笛卡尔积。如果某个 tag 有 10000 个唯一值，会产生 10000 个 series keys。需要考虑添加 cardinality 限制。

2. **String Fields**: 当前方案无法区分 string type 的 field 和 tag。如果 measurement 有 string field，该字段会被误判为 tag。需要在后续版本中添加显式配置。
