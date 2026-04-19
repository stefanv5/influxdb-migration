# V2 Source V1 兼容接口改造 - 设计文档

## 1. 背景与目标

### 1.1 问题

当前 V2 source adapter 使用 Flux 原生 API，存在以下问题：
1. `schema.measurementTagValues` 不支持 `start`/`stop` 时间过滤参数
2. `DiscoverSeriesInTimeWindow` 无法精确获取时间窗口内的 series
3. Flux 查询性能在某些场景下低于 InfluxQL

### 1.2 解决方案

改用 V1 兼容接口（InfluxQL）进行所有查询：
1. InfluxQL 的 `SHOW SERIES WHERE time` 支持时间过滤
2. InfluxQL 在简单查询场景下性能更优
3. 配置简化，不再需要 `org`/`token`

## 2. 技术方案

### 2.1 API 选择

| API | 端点 | 认证 | 查询语言 |
|-----|------|------|----------|
| V2 Native | `/api/v2/query` | Token | Flux |
| V1 兼容 | `/query` | Basic Auth (u/p) | InfluxQL |

选择 V1 兼容接口的原因：
- 支持 `SHOW SERIES WHERE time` 时间过滤
- InfluxQL 语法简单，查询效率高
- 广泛兼容，稳定性高

### 2.2 URL 构建

```go
func (a *InfluxDBV2Adapter) buildV1URL() string {
    baseURL := a.config.URL
    if !strings.HasSuffix(baseURL, "/") {
        baseURL += "/"
    }
    return baseURL + "query"
}
```

示例：
- `http://localhost:8086` → `http://localhost:8086/query`
- `http://localhost:8086/` → `http://localhost:8086/query`

### 2.3 查询参数构建

```go
func (a *InfluxDBV2Adapter) buildV1QueryParams(query string) url.Values {
    params := url.Values{}
    params.Set("q", query)
    params.Set("db", a.config.Bucket)
    if a.config.RetentionPolicy != "" {
        params.Set("rp", a.config.RetentionPolicy)
    }
    params.Set("u", a.config.Username)
    params.Set("p", a.config.Password)
    return params
}
```

### 2.4 HTTP 请求

```go
func (a *InfluxDBV2Adapter) executeV1Query(ctx context.Context, query string) (*v1Result, error) {
    params := a.buildV1QueryParams(query)
    u, _ := url.Parse(a.buildV1URL())
    u.RawQuery = params.Encode()

    req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
    if err != nil {
        return nil, err
    }

    resp, err := a.client.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("V1 query failed with status %d: %s", resp.StatusCode, string(body))
    }

    var result v1Result
    if err := json.Unmarshal(body, &result); err != nil {
        return nil, fmt.Errorf("failed to parse V1 response: %w", err)
    }

    return &result, nil
}
```

## 3. 方法实现详解

### 3.1 DiscoverTables

```go
func (a *InfluxDBV2Adapter) DiscoverTables(ctx context.Context) ([]string, error) {
    result, err := a.executeV1Query(ctx, "SHOW MEASUREMENTS")
    if err != nil {
        return nil, err
    }

    var tables []string
    for _, series := range result.Results {
        for _, values := range series.Values {
            if len(values) > 0 {
                if name, ok := values[0].(string); ok {
                    tables = append(tables, name)
                }
            }
        }
    }
    return tables, nil
}
```

### 3.2 DiscoverSeries

```go
func (a *InfluxDBV2Adapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
    query := fmt.Sprintf("SHOW SERIES FROM %s", influxQuoteIdentifier(measurement))
    result, err := a.executeV1Query(ctx, query)
    if err != nil {
        return nil, err
    }

    var series []string
    for _, series := range result.Results {
        for _, values := range series.Values {
            if len(values) > 0 {
                if key, ok := values[0].(string); ok {
                    series = append(series, key)
                }
            }
        }
    }
    return series, nil
}
```

**返回格式**: `measurement,tag1=value1,tag2=value2`

### 3.3 DiscoverSeriesInTimeWindow

```go
func (a *InfluxDBV2Adapter) DiscoverSeriesInTimeWindow(ctx context.Context, measurement string, startTime, endTime time.Time) ([]string, error) {
    query := fmt.Sprintf(
        "SHOW SERIES FROM %s WHERE time >= '%s' AND time < '%s'",
        influxQuoteIdentifier(measurement),
        startTime.Format(time.RFC3339Nano),
        endTime.Format(time.RFC3339Nano))

    result, err := a.executeV1Query(ctx, query)
    if err != nil {
        return nil, err
    }

    var series []string
    for _, series := range result.Results {
        for _, values := range series.Values {
            if len(values) > 0 {
                if key, ok := values[0].(string); ok {
                    series = append(series, key)
                }
            }
        }
    }
    return series, nil
}
```

**关键改进**: 使用 `WHERE time >= 'start' AND time < 'end'` 实现精确时间窗口过滤。

### 3.4 QueryData

```go
func (a *InfluxDBV2Adapter) QueryData(ctx context.Context, measurement string,
    lastCheckpoint *Checkpoint, batchFunc func([]Record) error, cfg *QueryConfig) (*Checkpoint, error) {

    var startTime string
    var lastTS int64
    if lastCheckpoint != nil && lastCheckpoint.LastTimestamp != 0 {
        lastTS = lastCheckpoint.LastTimestamp
        startTime = time.Unix(0, lastTS).Format(time.RFC3339Nano)
    } else {
        startTime = "1970-01-01T00:00:00Z"
    }

    endTime := time.Now().Add(1 * time.Hour).Format(time.RFC3339Nano)
    batchSize := 10000
    if cfg != nil && cfg.BatchSize > 0 {
        batchSize = cfg.BatchSize
    }

    offset := 0
    totalRecords := 0

    for {
        query := fmt.Sprintf(
            `SELECT * FROM %s WHERE time >= '%s' AND time < '%s' ORDER BY time LIMIT %d OFFSET %d`,
            influxQuoteIdentifier(measurement), startTime, endTime, batchSize, offset)

        records, err := a.executeSelectQuery(ctx, query)
        if err != nil {
            return nil, err
        }

        if len(records) == 0 {
            break
        }

        if err := batchFunc(records); err != nil {
            return nil, err
        }

        totalRecords += len(records)
        lastTS = records[len(records)-1].Time

        if len(records) < batchSize {
            break
        }

        offset += batchSize

        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        case <-time.After(100 * time.Millisecond):
        }
    }

    return &Checkpoint{
        LastTimestamp: lastTS,
        ProcessedRows: int64(totalRecords),
    }, nil
}
```

### 3.5 QueryDataBatch

```go
func (a *InfluxDBV2Adapter) QueryDataBatch(ctx context.Context, measurement string,
    series []string, startTime, endTime time.Time, lastCheckpoint *Checkpoint,
    batchFunc func([]Record) error, cfg *QueryConfig) (*Checkpoint, error) {

    whereClause := buildV1WhereClause(series)
    batchSize := getBatchSize(cfg)

    queryStart := startTime
    if lastCheckpoint != nil && lastCheckpoint.LastTimestamp > 0 {
        queryStart = time.Unix(0, lastCheckpoint.LastTimestamp).Format(time.RFC3339Nano)
    }

    offset := 0
    var maxTS int64
    totalRecords := 0

    for {
        query := fmt.Sprintf(
            `SELECT * FROM %s WHERE (%s) AND time >= '%s' AND time < '%s' ORDER BY time LIMIT %d OFFSET %d`,
            influxQuoteIdentifier(measurement),
            whereClause,
            queryStart.Format(time.RFC3339Nano),
            endTime.Format(time.RFC3339Nano),
            batchSize, offset)

        records, err := a.executeSelectQuery(ctx, query)
        if err != nil {
            return nil, err
        }

        if len(records) == 0 {
            break
        }

        if err := batchFunc(records); err != nil {
            return nil, err
        }

        totalRecords += len(records)
        maxTS = records[len(records)-1].Time

        if len(records) < batchSize {
            break
        }

        offset += batchSize
    }

    return &Checkpoint{
        LastTimestamp: maxTS,
        ProcessedRows: int64(totalRecords),
    }, nil
}
```

## 4. WHERE 子句构建

```go
func buildV1WhereClause(series []string) string {
    var conditions []string
    for _, s := range series {
        tags := parseSeriesKey(s)
        var tagConditions []string
        for k, v := range tags {
            escapedValue := strings.ReplaceAll(v, "'", "''")
            tagConditions = append(tagConditions,
                fmt.Sprintf("%s='%s'", influxQuoteIdentifier(k), escapedValue))
        }
        if len(tagConditions) > 0 {
            conditions = append(conditions, "("+strings.Join(tagConditions, " AND ")+")")
        }
    }
    return strings.Join(conditions, " OR ")
}
```

## 5. Series Key 解析

```go
func parseSeriesKey(key string) map[string]string {
    tags := make(map[string]string)
    parts := strings.Split(key, ",")
    for _, part := range parts[1:] {
        kv := strings.SplitN(part, "=", 2)
        if len(kv) == 2 {
            tags[kv[0]] = kv[1]
        }
    }
    return tags
}
```

## 6. 结果解析

```go
func (a *InfluxDBV2Adapter) parseV1Values(columns []string, values []interface{}) *Record {
    record := NewRecord()

    for i, col := range columns {
        if i >= len(values) {
            continue
        }

        val := values[i]
        if val == nil {
            continue
        }

        switch col {
        case "time":
            if ts, ok := val.(string); ok {
                if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
                    record.Time = t.UnixNano()
                }
            }
        default:
            switch v := val.(type) {
            case float64:
                record.AddField(col, v)
            case string:
                record.AddField(col, v)
            case bool:
                record.AddField(col, v)
            }
        }
    }

    return record
}
```

## 7. 标识符转义

```go
func influxQuoteIdentifier(s string) string {
    if s == "" {
        return `""`
    }
    return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
```

## 8. 时间戳处理

| 格式 | 示例 | 说明 |
|------|------|------|
| RFC3339Nano | `2024-01-01T00:00:00.123456789Z` | 查询参数使用 |
| UnixNano | `1704067200000000000` | 内部存储 |
| RFC3339 | `2024-01-01T00:00:00Z` | 降级处理 |

## 9. 错误处理

```go
type V1QueryError struct {
    StatusCode int
    Message    string
    Query      string
}

func (e *V1QueryError) Error() string {
    return fmt.Sprintf("V1 query failed (status %d): %s, query: %s",
        e.StatusCode, e.Message, e.Query)
}
```

## 10. 向后兼容性

### 10.1 配置兼容

如果配置中有 `token` 和 `org` 但没有 `username`：
- 仍然尝试使用（可能有默认值或特殊配置）
- 记录警告日志

### 10.2 代码兼容

V1 adapter 保持原有实现不变：
```go
// internal/adapter/source/influxdb_v1.go 保持不变
```

## 11. 性能考量

### 11.1 分页策略

- 使用 `LIMIT n OFFSET o` 进行分页
- 默认 batch size: 10000
- OFFSET 过大时可能影响性能，未来可考虑 keyset pagination

### 11.2 并发控制

- V1 兼容接口对并发有限制
- 当前实现为串行查询
- 如需并发，需要实现连接池

## 12. 测试策略

### 12.1 Mock V1 响应

```go
func mockV1Response(series []v1Series) []byte {
    result := v1Result{Results: series}
    body, _ := json.Marshal(result)
    return body
}

func TestDiscoverSeriesInTimeWindow(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // 验证查询参数
        assert.Equal(t, "db=test", r.URL.RawQuery)

        w.Header().Set("Content-Type", "application/json")
        w.Write(mockV1Response([]v1Series{
            {
                Name:    "cpu",
                Columns: []string{"key"},
                Values:  [][]interface{}{{"cpu,host=server1"}},
            },
        }))
    }))
    defer ts.Close()

    // 测试代码...
}
```
