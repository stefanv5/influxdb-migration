# V2 Source Adapter V1 兼容接口改造

## 概述

将 InfluxDB V2 source adapter 的所有查询从 Flux API 改为 V1 兼容接口（InfluxQL），提升查询性能。

**原因**：
1. InfluxQL 在某些场景下比 Flux 查询效率更高
2. V1 兼容接口支持 `SHOW SERIES ... WHERE time` 时间过滤，可精确实现 shard-group 模式的 series discovery
3. V2 原生 Flux schema 函数（`schema.measurementTagValues` 等）不支持时间过滤参数

## 配置变更

### V2 Source 配置结构

```yaml
sources:
  influxdb:
    - name: "v2-source"
      type: "influxdb"
      version: 2              # 用于区分V1/V2
      url: "http://localhost:8086"
      username: "admin"      # V1兼容认证
      password: "${INFLUX_PASSWORD}"
      bucket: "metrics"       # 映射到V1的db参数
      retention_policy: "autogen"  # 可选，映射到V1的rp参数
      ssl:
        enabled: false
        skip_verify: false
```

### 配置字段说明

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `version` | int | 是 | 2 表示V2 source |
| `url` | string | 是 | V2服务器地址 |
| `username` | string | 是 | V1兼容接口认证用户 |
| `password` | string | 是 | V1兼容接口认证密码 |
| `bucket` | string | 是 | 映射到V1的 `db` 参数 |
| `retention_policy` | string | 否 | 映射到V1的 `rp` 参数，默认为空 |
| `ssl` | object | 否 | SSL配置 |

**注意**：`org` 和 `token` 字段对于V2 source不再需要。

## 实现方案

### API 端点

V1 兼容接口端点：`/query`

查询参数：
- `db` - 数据库名（来自 `bucket` 配置）
- `rp` - 保留策略（来自 `retention_policy` 配置，可选）
- `u` - 用户名
- `p` - 密码
- `q` - InfluxQL 查询语句

### V2 Source Adapter 结构变更

```go
type InfluxDBV2Config struct {
    URL             string
    Username        string
    Password        string
    Bucket          string
    RetentionPolicy string
    SSL             types.SSLConfig
}
```

### 查询方法实现

#### 1. DiscoverTables - `SHOW MEASUREMENTS`

```sql
SHOW MEASUREMENTS
```

#### 2. DiscoverSeries - `SHOW SERIES`

```sql
SHOW SERIES FROM "measurement"
```

#### 3. DiscoverSeriesInTimeWindow - `SHOW SERIES WHERE time`

```sql
SHOW SERIES FROM "measurement" WHERE time >= 'start' AND time < 'end'
```

**关键改进**：支持时间过滤，可精确获取指定时间窗口内的 series。

#### 4. QueryData - `SELECT ... WHERE time`

使用时间戳分页：
```sql
SELECT * FROM "measurement" WHERE time >= 'start' AND time < 'end' ORDER BY time LIMIT n OFFSET o
```

### 响应格式

V1 兼容接口返回 JSON 格式：
```json
{
  "results": [
    {
      "statement_id": 0,
      "series": [
        {
          "name": "measurement",
          "columns": ["time", "field1", "field2"],
          "values": [["2024-01-01T00:00:00Z", 1.0, "value"]]
        }
      ]
    }
  ]
}
```

## 数据类型映射

### InfluxQL → Record

| InfluxQL 类型 | Go 类型 | Record 处理 |
|---------------|---------|-------------|
| float | float64 | AddField |
| integer | int64 | AddField |
| string | string | AddField (V1无tag概念) |
| boolean | bool | AddField |
| timestamp | int64 (UnixNano) | Time |

**注意**：V1 查询结果中，字符串类型均作为 field 处理，不作为 tag。

## 代码结构

### 文件变更

```
internal/adapter/source/
├── influxdb.go          # 修改：V2 adapter 改用 V1 兼容查询
├── influxdb_v1.go       # 新增：V1 adapter（保持原有实现）
├── influxdb_v2.go        # 重构：V2 adapter
└── influxdb_v2_config.go # 新增：V2 配置解析
```

### 新增方法

```go
// V2 adapter 新增方法
func (a *InfluxDBV2Adapter) executeV1Query(ctx context.Context, query string) (*v1QueryResult, error)
func (a *InfluxDBV2Adapter) parseV1Result(body []byte) ([]record, error)
func (a *InfluxDBV2Adapter) buildV1URL() string
```

### V1 查询结果结构

```go
type v1Series struct {
    Name    string            `json:"name"`
    Tags    map[string]string `json:"tags"`
    Columns []string          `json:"columns"`
    Values  [][]interface{}  `json:"values"`
}

type v1Result struct {
    Results []v1Series `json:"results"`
}
```

## 错误处理

- V1 兼容接口查询失败直接返回错误
- 无回退到 Flux 的机制
- 错误信息包含 HTTP 状态码和响应体

## 向后兼容性

### V1 Source

V1 source adapter 保持原有实现不变，继续使用原生 V1 API。

### 配置兼容性

如果现有配置使用 `org`/`token`，则在 V2 source 中：
- `org` 字段被忽略
- `token` 字段被忽略
- `username`/`password` 优先使用

### Target Adapter

V1/V2 target adapter 不受影响，继续使用原有的写入实现。

## 测试计划

### 单元测试

1. `TestV2DiscoverSeriesInTimeWindow` - 测试带时间过滤的 series discovery
2. `TestV2QueryDataWithPagination` - 测试 V1 兼容查询的分页
3. `TestV2ConfigDecode` - 测试新配置结构解析

### 集成测试

1. V2 source → V1 target 迁移
2. V2 source → V2 target 迁移
3. V2 source → MySQL/TDengine 迁移

## 风险评估

| 风险 | 级别 | 缓解措施 |
|------|------|----------|
| V1 兼容接口在某些 V2 版本中行为差异 | 低 | 已广泛使用，稳定性高 |
| InfluxQL 语法与 Flux 差异 | 低 | InfluxQL 是 V1 标准，兼容性广 |
| 时间戳格式精度问题 | 中 | 使用 RFC3339Nano 格式保持纳秒精度 |
