# V2 Source V1 兼容接口改造 - 提案

## 提案日期

2026-04-18

## 状态

已提议

## 摘要

将 InfluxDB V2 source adapter 的所有查询从 Flux API 改为 V1 兼容接口（InfluxQL），以解决以下问题：

1. **Series Discovery 时间过滤**: Flux schema 函数不支持 `start`/`stop` 参数
2. **查询性能**: InfluxQL 在简单查询场景下性能优于 Flux
3. **配置简化**: 不再需要 `org`/`token`，使用传统的 `username`/`password`

## 动机

### 问题 1: Shard-Group 模式失效

当前 V2 adapter 的 `DiscoverSeriesInTimeWindow` 完全忽略时间参数：

```go
func (a *InfluxDBV2Adapter) DiscoverSeriesInTimeWindow(...) ([]string, error) {
    return a.DiscoverSeries(ctx, measurement)  // 返回所有series，忽略时间窗口
}
```

这导致 shard-group 模式的内存优化目标完全失效。

### 问题 2: V1 兼容接口支持时间过滤

InfluxDB V2 支持 V1 兼容 API，其 InfluxQL 查询支持 `WHERE time` 过滤：

```sql
SHOW SERIES FROM "measurement" WHERE time >= '2024-01-01' AND time < '2024-01-07'
```

### 问题 3: 配置冗余

V2 source 当前需要配置：
- `url`
- `token`
- `org`
- `bucket`

而 V1 兼容接口只需要：
- `url`
- `username`
- `password`
- `bucket`

使用 V1 兼容接口可以：
- 减少配置字段
- 提高安全性（不使用 long-lived token）
- 提升查询性能

## 提案解决方案

### 方案概述

将 V2 source adapter 的所有查询改为通过 V1 兼容接口执行，使用 InfluxQL：

1. **Discovery**: `SHOW MEASUREMENTS`, `SHOW SERIES`, `SHOW SERIES WHERE time`
2. **Query**: `SELECT ... WHERE time LIMIT n OFFSET o`

### 配置变更

```yaml
sources:
  influxdb:
    - name: "v2-source"
      type: "influxdb"
      version: 2
      url: "http://localhost:8086"
      username: "admin"           # V1兼容认证
      password: "${INFLUX_PASSWORD}"
      bucket: "metrics"
      retention_policy: "autogen"  # 可选
```

**字段变更**:
- 新增: `username`, `password`, `retention_policy`
- 废弃（V2 source）: `token`, `org`

### API 端点

| 组件 | 说明 |
|------|------|
| 端点 | `/query` |
| 方法 | GET |
| 认证 | `u`/`p` 查询参数 |
| 数据库 | `db` = bucket |
| RP | `rp` = retention_policy (可选) |

### 影响范围

| 组件 | 影响 | 说明 |
|------|------|------|
| V2 Source Adapter | 修改 | 所有查询改用 V1 兼容接口 |
| V1 Source Adapter | 无 | 保持不变 |
| V1 Target Adapter | 无 | 保持不变 |
| V2 Target Adapter | 无 | 保持不变 |
| Checkpoint | 无 | 不受影响 |
| Rate Limiter | 无 | 不受影响 |

## 风险与缓解

| 风险 | 级别 | 缓解措施 |
|------|------|----------|
| V1 兼容接口行为差异 | 低 | V1 API 广泛使用，稳定性高 |
| 配置迁移 | 低 | 兼容旧配置，有警告日志 |
| InfluxQL vs Flux 语法差异 | 低 | InfluxQL 是标准，文档完善 |

## 实现计划

### Phase 1: 核心实现
1. V1 查询基础方法
2. Series Discovery 方法重写
3. Data Query 方法重写

### Phase 2: 测试
1. 单元测试
2. 集成测试

### Phase 3: 文档
1. README 更新
2. 配置示例

## 成功标准

1. V2 source 能够通过 V1 兼容接口完成所有查询
2. `DiscoverSeriesInTimeWindow` 正确支持时间过滤
3. 查询性能相比 Flux 有明显提升
4. 所有现有测试通过

## 替代方案

### 方案 A: 保持 Flux，使用 schema.measurementTagValues

- 使用 `schema.measurementTagValues(bucket, tag, measurement, start, stop)`
- **问题**: 文档显示该函数不支持 `start`/`stop` 参数

### 方案 B: Flux + 实际数据查询

- 不做预 discovery，直接用 Flux 查询 + 分组
- **问题**: 违背 shard-group 模式的初衷，增加复杂度

### 方案 C: 混合模式（当前提案）

- Discovery 用 V1 兼容接口
- Data Query 用 V1 兼容接口
- **优点**: 实现简单，性能好，代码复用

## 参考资料

- [InfluxDB V1 兼容 API 文档](https://docs.influxdata.com/influxdb/v2/api-guide/influxdb-1x/query)
- [InfluxQL SHOW SERIES](https://docs.influxdata.com/influxdb/v2/query-data/influxql/explore-schema)
