# V2 Source Tag Preservation & Series Discovery Fix

## 1. Problem Statement

当前 InfluxDB V2 Source Adapter 存在三个严重问题，导致 V2 → V1 迁移时数据丢失或无法工作：

### P0-1: executeFluxSelect 丢失 Tags
**位置**: `internal/adapter/source/influxdb.go:1067-1117`

Flux 查询返回的 JSON 结构中，每条记录包含 `_time`, `_measurement`, `_field`, `_value` 以及所有 tag 字段。但当前 `fluxRecord` 结构体只定义了前4个字段，Tags 被 JSON 解析器丢弃。

**影响**: 迁移后所有原始 tag 信息全部丢失。

### P0-2: DiscoverSeries 返回 Tag Keys 而非 Series Keys
**位置**: `internal/adapter/source/influxdb.go:764-785`

当前实现：
```go
fluxQuery := `schema.tagKeys(bucket: "%s", measurement: "%s")`
// 返回: ["host", "region", "service"]  ← tag key 名称列表
```

正确的 series key 格式应该是: `["cpu,host=server1,region=us-west", "cpu,host=server2,region=us-east"]`

**影响**: V2 batch/shard-group 模式完全无法工作，因为 series 标识符格式错误。

### P0-3: DiscoverSeriesInTimeWindow 是占位实现
**位置**: `internal/adapter/source/influxdb.go:823-877`

当前实现只是获取 tag keys，无法按时间窗口过滤 series。

**影响**: V2 shard-group 模式无法正确工作。

## 2. Goals

1. **V2 Series Discovery**: 正确获取完整的 series keys（measurement + tag key-value 组合）
2. **Tag Preservation**: Flux 查询结果必须正确区分 tag 和 field，保留所有原始数据
3. **Time Window Support**: DiscoverSeriesInTimeWindow 必须按时间窗口过滤
4. **V2 → V1 迁移**: 确保 V2 Source 到 V1 Target 的迁移能保留所有 Tags 和 Fields

## 3. Non-Goals

1. 不支持 V2 → V2 迁移（当前场景是 V2 → V1）
2. 不实现完整的 catalog API，仅使用 Flux 标准库函数
3. 不改变 V1 Source Adapter 的行为

## 4. Technical Approach

### 4.1 V2 Series Discovery 实现

InfluxDB V2 没有直接的 `SHOW SERIES`，需要通过以下步骤重建 series keys：

```
Step 1: schema.tagKeys(bucket, measurement)
        → 返回: ["host", "region", "service"]

Step 2: 对每个 tag key，调用 schema.tagValues(bucket, tag, measurement)
        → host: ["server1", "server2"]
        → region: ["us-west", "us-east"]
        → service: ["api", "web"]

Step 3: 笛卡尔积组合
        → cpu,host=server1,region=us-west,service=api
        → cpu,host=server1,region=us-west,service=web
        → cpu,host=server1,region=us-east,service=api
        → ...
```

**优化**: 对于大 cardinality 场景，可以限制每个 tag key 的最大 values 数量。

### 4.2 Tag/Field 区分策略

在 Flux 查询结果中，需要区分 tag 和 field：

```
原始 JSON:
{
  "_time": "2024-01-01T00:00:00Z",
  "_measurement": "cpu",
  "_field": "usage",
  "_value": 85.5,
  "host": "server1",      ← tag
  "region": "us-west",     ← tag
  "status_message": "ok"   ← tag (string field 但值是 tag)
}
```

**问题**: 无法单靠类型区分 string field 和 tag（两者都是 string）。

**解决方案**:
1. 在每个 **shard group 开始前**，调用 `schema.tagKeys()` 获取该 measurement 的所有 tag key 名称（不是每个 time window 获取一次）
2. 将 tag keys 列表通过 `QueryConfig.TagKeys` 传递给 `executeFluxSelect`
3. 解析时：任何在 tag keys 列表中的字段作为 tag 处理，其余作为 field 处理

### 4.3 接口变更

#### 4.3.1 SourceAdapter Interface 新增方法

```go
// DiscoverTagKeys 返回指定 measurement 的所有 tag key 名称
// 用于 executeFluxSelect 区分 tag 和 field
DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error)
```

#### 4.3.2 QueryConfig 新增字段

```go
type QueryConfig struct {
    BatchSize         int
    TimeWindow        time.Duration
    MaxSeriesPerQuery int
    TagKeys           []string  // NEW: 用于区分 tag 和 field
}
```

#### 4.3.3 QueryDataBatch 接口扩展

`QueryDataBatch` 需要能够接收 `lastCheckpoint.LastTimestamp` 来支持断点续传。

## 5. Implementation Plan

### Phase 1: Tag Preservation (P0-1)

1. 添加 `DiscoverTagKeys` 到 V2 SourceAdapter
2. 添加 `TagKeys` 到 `QueryConfig`
3. 修改 `executeFluxSelect` 接收并使用 tag keys 列表
4. 在 `migrateShardGroup` 开始前获取 tag keys，传递给所有 time window

### Phase 2: Series Discovery (P0-2)

1. 重写 V2 `DiscoverSeries` 使用 tagKeys + tagValues 组合
2. 添加适当的 cardinality 限制

### Phase 3: Time Window Support (P0-3)

1. 重写 V2 `DiscoverSeriesInTimeWindow`
2. 使用 Flux 查询按时间过滤（如果 V2 支持）
3. 或者返回所有 series，然后在 engine 层按时间窗口过滤

## 6. Files to Modify

```
pkg/types/
└── query_config.go          # 添加 TagKeys 字段

internal/adapter/
├── adapter.go               # 添加 DiscoverTagKeys 方法声明
└── source/
    └── influxdb.go         # 实现 V2 DiscoverSeries, DiscoverTagKeys,
                            # DiscoverSeriesInTimeWindow, executeFluxSelect 修改

internal/engine/
└── migration.go            # 在 migrateTimeWindow 开始前获取 tag keys
```

## 7. Backwards Compatibility

- V1 Source Adapter 不受影响
- V2 Target Adapter 不受影响
- 现有配置格式不需要变更
- 新字段 `TagKeys` 在 V1 场景下为空切片，不影响现有逻辑

## 8. Testing Strategy

### Unit Tests

1. `TestV2DiscoverSeries` - 验证返回正确格式的 series keys
2. `TestV2DiscoverTagKeys` - 验证返回 tag key 名称列表
3. `TestV2ExecuteFluxSelectWithTagKeys` - 验证 tag/field 正确区分

### Integration Tests (Future)

1. 连接真实 V2 实例，验证 series discovery 结果
2. 端到端迁移，验证 tag 数据不丢失

## 9. Open Questions

1. **Cardinality 限制**: 当 tag values 数量非常大时（如 unique user_id），如何处理？
   - 建议: 添加 `MaxTagValuesPerKey` 配置限制

2. **String Fields**: 如果 measurement 确实有 string 类型的 field，如何与 tag 区分？
   - 当前方案无法区分，但实际场景中 string field 较少见
   - 建议: 添加显式的 `StringFields` 配置列表
