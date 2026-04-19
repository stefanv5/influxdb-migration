# V2 Source V1 兼容接口改造 - 任务清单

## 1. 配置结构变更

### 1.1 修改 V2 Source 配置结构
**文件**: `pkg/types/config.go`
- 添加 `Username` 和 `Password` 字段到 `InfluxDBConfig`
- 添加 `RetentionPolicy` 字段到 `InfluxDBConfig`
- 保留 `Token` 和 `Org` 字段（向后兼容）

**状态**: 已完成

### 1.2 修改配置验证器
**文件**: `internal/config/validator.go`
- V2 source 验证时检查 `username`/`password` 是否提供
- V2 source 验证时不再要求 `token`/`org`

**状态**: 已完成

### 1.3 修改配置解码
**文件**: `internal/adapter/source/influxdb.go`
- `decodeInfluxV2Config` 支持解析 `username`、`password`、`retention_policy`

**状态**: 已完成

---

## 2. V1 兼容查询基础方法

### 2.1 新增 V1 查询执行方法
**文件**: `internal/adapter/source/influxdb.go`
- `executeV1Query(ctx, query)` - 执行 V1 兼容查询
- `buildV1URL()` - 构建 V1 API URL (`/query`)
- V1 响应解析方法

**状态**: 已完成

### 2.2 新增 V1 结果结构体
**文件**: `internal/adapter/source/influxdb.go`
- `v1Series` 结构体
- `v1Result` 结构体

**状态**: 已完成

---

## 3. Series Discovery 方法重写

### 3.1 DiscoverTables - 使用 SHOW MEASUREMENTS
**文件**: `internal/adapter/source/influxdb.go`
- 实现: `SHOW MEASUREMENTS`

**状态**: 已完成

### 3.2 DiscoverSeries - 使用 SHOW SERIES
**文件**: `internal/adapter/source/influxdb.go`
- 实现: `SHOW SERIES FROM "measurement"`

**状态**: 已完成

### 3.3 DiscoverSeriesInTimeWindow - 使用 SHOW SERIES WHERE time
**文件**: `internal/adapter/source/influxdb.go`
- 实现: `SHOW SERIES FROM "measurement" WHERE time >= 'start' AND time < 'end'`
- **关键功能**: 支持时间过滤，实现 shard-group 模式的精确 series discovery

**状态**: 已完成

### 3.4 DiscoverTagKeys - 返回 nil
**文件**: `internal/adapter/source/influxdb.go`
- V1 兼容查询不支持 tag keys discovery
- 返回空切片

**状态**: 已完成

### 3.5 DiscoverSchema - 返回空 schema
**文件**: `internal/adapter/source/influxdb.go`
- V1 兼容查询返回空 schema，实际类型在查询时确定

**状态**: 已完成

---

## 4. Data Query 方法重写

### 4.1 QueryData - 使用 SELECT ... WHERE time
**文件**: `internal/adapter/source/influxdb.go`
- 使用 V1 兼容接口的 `SELECT` 查询
- 时间戳分页: `WHERE time >= 'start' AND time < 'end' ORDER BY time LIMIT n OFFSET o`
- 将结果解析为 `Record` 列表

**状态**: 已完成

### 4.2 QueryDataBatch - 批量 series 查询
**文件**: `internal/adapter/source/influxdb.go`
- 使用 `WHERE (tag1='v1' AND ...) OR (tag2='v2' AND ...)` 过滤 series
- 支持时间窗口: `WHERE time >= 'start' AND time < 'end'`

**状态**: 已完成

---

## 5. 结果解析

### 5.1 V1 响应解析为 Record
**文件**: `internal/adapter/source/influxdb.go`
- 解析 `v1Series` → `[]Record`
- 处理时间戳格式 (RFC3339Nano)
- 处理字段类型转换

**状态**: 已完成

### 5.2 Series Key 解析
**文件**: `internal/adapter/source/influxdb.go`
- 解析 `measurement,tag1=value1,tag2=value2` 格式
- 提取 measurement 名称和 tag 键值对

**状态**: 已完成

---

## 6. 测试

### 6.1 单元测试
**文件**: `internal/adapter/source/influxdb_v2_test.go`
- `TestV2DiscoverSeriesInTimeWindow`
- `TestV2QueryDataWithV1API`
- `TestV2ConfigDecodeWithAuth`
- `TestV1QueryResultParsing`

**状态**: 已完成

### 6.2 测试辅助
**文件**: `internal/adapter/source/influxdb_v2_test.go`
- Mock V1 兼容接口响应
- 测试 V1 结果解析逻辑

**状态**: 已完成

---

## 7. 文档更新

### 7.1 README 更新
**文件**: `README.md`
- 更新 V2 source 配置示例
- 说明使用 V1 兼容接口

**状态**: 已完成

---

## 任务依赖关系

```
1.1 配置结构变更
    ↓
1.2 配置验证器修改
    ↓
1.3 配置解码修改
    ↓
2.1 V1 查询基础方法
2.2 V1 结果结构体
    ↓
3.1-3.5 Series Discovery 方法
4.1-4.2 Data Query 方法
5.1-5.2 结果解析
    ↓
6.1-6.2 测试
    ↓
7.1 README 更新
```

## 预估工作量

| 模块 | 任务数 | 说明 |
|------|--------|------|
| 配置变更 | 3 | 结构、验证、解码 |
| 基础方法 | 2 | V1查询、V1结果结构 |
| Discovery | 5 | Tables、Series、SeriesInTimeWindow、TagKeys、Schema |
| Query | 2 | QueryData、QueryDataBatch |
| 解析 | 2 | 结果解析、Series Key解析 |
| 测试 | 2 | 单元测试、辅助测试 |
| 文档 | 1 | README |
| **总计** | **17** | |
