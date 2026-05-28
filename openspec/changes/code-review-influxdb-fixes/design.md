# Design: InfluxDB-to-InfluxDB 迁移代码审查修复

## 1. Overview

修复代码审查发现的 14 个问题（2 CRITICAL + 6 HIGH + 6 MEDIUM），按依赖关系组织为 8 个独立修复任务。

## 2. Fix 1: V2 源端认证凭据补全 (C1)

### 2.1 问题根因

`sourceConfigToMap` 在构建 InfluxDB 配置映射时遗漏了 `username`、`password`、`retention_policy` 三个字段。

### 2.2 修改

**文件**: `internal/engine/migration.go:1517-1524`

```go
// 修改前:
case "influxdb":
    m["influxdb"] = map[string]interface{}{
        "url":     src.InfluxDB.URL,
        "token":   src.InfluxDB.Token,
        "org":     src.InfluxDB.Org,
        "bucket":  src.InfluxDB.Bucket,
        "version": src.InfluxDB.Version,
    }

// 修改后:
case "influxdb":
    m["influxdb"] = map[string]interface{}{
        "url":              src.InfluxDB.URL,
        "token":            src.InfluxDB.Token,
        "org":              src.InfluxDB.Org,
        "bucket":           src.InfluxDB.Bucket,
        "version":          src.InfluxDB.Version,
        "username":         src.InfluxDB.Username,
        "password":         src.InfluxDB.Password,
        "retention_policy": src.InfluxDB.RetentionPolicy,
    }
```

## 3. Fix 2: V1 源端 Tag/Field 正确区分 (C2)

### 3.1 问题根因

`parseValues` 把所有 string 值当 Tag，未使用 `DiscoverTagKeys` 区分。

### 3.2 修改方案

在 `QueryData` 和 `QueryDataBatch` 调用 `executeChunkedQuery` 前获取 tag keys，传递给解析逻辑。

**文件**: `internal/adapter/source/influxdb.go`

#### 3.2.1 修改 `parseValues` 签名，接收 `tagKeySet`

```go
// 修改前:
func (a *InfluxDBV1Adapter) parseValues(columns []string, values []interface{}) types.Record {

// 修改后:
func (a *InfluxDBV1Adapter) parseValues(columns []string, values []interface{}, tagKeySet map[string]bool) types.Record {
```

#### 3.2.2 修改 string 分支逻辑

```go
// 修改前 (line 710-713):
case string:
    // In InfluxDB V1, string values from query results are typically tags
    record.AddTag(col, v)

// 修改后:
case string:
    if tagKeySet[col] {
        record.AddTag(col, v)
    } else {
        record.AddField(col, v)
    }
```

#### 3.2.3 在调用方获取 tagKeySet

在 `QueryData`（line ~380）和 `QueryDataBatch`（line ~460）中，在调用 `executeChunkedQuery` 前获取 tag keys：

```go
// 获取 tag keys 用于区分 tag 和 field
tagKeys, _ := a.DiscoverTagKeys(ctx, measurement)
tagKeySet := make(map[string]bool)
for _, k := range tagKeys {
    tagKeySet[k] = true
}
```

然后将 `tagKeySet` 传递给 `executeChunkedQuery`，最终传到 `parseValues`。

#### 3.2.4 增加 int64 case (M2b)

```go
// 在 parseValues 的 switch 中增加:
case int64:
    record.AddField(col, v)
case int:
    record.AddField(col, int64(v))
```

## 4. Fix 3: HTTP 响应体排空 (H1)

### 4.1 问题根因

`executeChunkedQuery`（line 599）和 `executeV1ChunkedQuery`（line 1005）在 `client.Do` 返回 error 时未排空 Body。

### 4.2 修改

**文件**: `internal/adapter/source/influxdb.go`

两处相同的修改模式：

```go
// 修改前:
resp, err := a.client.Do(req)
if err != nil {
    return fmt.Errorf("chunked query request failed: %w", err)
}

// 修改后:
resp, err := a.client.Do(req)
if err != nil {
    if resp != nil {
        io.Copy(io.Discard, resp.Body)
        resp.Body.Close()
    }
    return fmt.Errorf("chunked query request failed: %w", err)
}
```

应用位置:
1. `executeChunkedQuery` (V1 adapter, line ~599)
2. `executeV1ChunkedQuery` (V2 adapter, line ~1005)

## 5. Fix 4: V2 批量模式恢复不跳数据 (H2)

### 5.1 问题根因

V2 `QueryDataBatch` 用 `lastCheckpoint.LastTimestamp` 推进 `queryStart`，与 V1 行为不一致。

### 5.2 修改

**文件**: `internal/adapter/source/influxdb.go:1468-1477`

```go
// 修改前:
var lastTS int64
if lastCheckpoint != nil && lastCheckpoint.LastTimestamp != 0 {
    lastTS = lastCheckpoint.LastTimestamp
}
queryStart := startTime
if lastTS > 0 && lastTS > startTime.UnixNano() {
    queryStart = time.Unix(0, lastTS)
}

// 修改后:
// Always use the original startTime in batch mode.
// The lastCheckpoint.LastTimestamp is for progress tracking only, not for
// modifying query parameters. Each batch queries its full assigned time range.
queryStart := startTime
```

## 6. Fix 5: V2 目标端 HTTP 状态码检查 (H3)

### 6.1 修改

**文件**: `internal/adapter/target/influxdb.go:658-669`

```go
// 修改前:
resp, err := a.client.Do(req)
if err != nil {
    return nil, err
}
defer resp.Body.Close()

var result [][]interface{}
if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
    return nil, fmt.Errorf("failed to decode flux result: %w", err)
}

// 修改后:
resp, err := a.client.Do(req)
if err != nil {
    return nil, err
}
defer resp.Body.Close()

if resp.StatusCode != http.StatusOK {
    body, _ := io.ReadAll(resp.Body)
    return nil, fmt.Errorf("flux query failed with status %d: %s", resp.StatusCode, string(body))
}

var result [][]interface{}
if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
    return nil, fmt.Errorf("failed to decode flux result: %w", err)
}
```

## 7. Fix 6: Checkpoint 查询列名修正 (H4)

### 7.1 修改

**文件**: `internal/checkpoint/store.go:204-208`

```go
// 修改前:
func (s *SQLiteStore) ListCheckpoints(taskID string) ([]*types.Checkpoint, error) {
    query := `... FROM checkpoints WHERE task_name = ?`

// 修改后:
func (s *SQLiteStore) ListCheckpoints(taskName string) ([]*types.Checkpoint, error) {
    query := `... FROM checkpoints WHERE task_name = ?`
```

需同步检查所有调用方，确认传入的确实是 task name 而非 task ID。

## 8. Fix 7: V2 分片时间单位修正 (M2)

### 8.1 修改

**文件**: `internal/adapter/source/influxdb.go:1354-1357`

```go
// 修改前:
shardGroups = append(shardGroups, &adapter.ShardGroup{
    ID:        s.ID,
    StartTime: time.Unix(s.StartTime, 0),
    EndTime:   time.Unix(s.EndTime, 0),
})

// 修改后:
// InfluxDB V2 /api/v2/shards returns timestamps in nanoseconds
shardGroups = append(shardGroups, &adapter.ShardGroup{
    ID:        s.ID,
    StartTime: time.Unix(0, s.StartTime),
    EndTime:   time.Unix(0, s.EndTime),
})
```

## 9. Fix 8: RateLimiter 支持 Context (H6)

### 9.1 修改

**文件**: `internal/engine/ratelimiter.go:52-56`

方案：修改 `Wait` 方法签名增加 `context.Context` 参数，或废弃 `Wait` 改用 `WaitContext`。

推荐方案：直接修改 `Wait` 增加 context 参数（影响范围需检查所有调用方）。

```go
// 修改前:
func (r *RateLimiter) Wait(points int) {
    for !r.Allow(points) {
        time.Sleep(10 * time.Millisecond)
    }
}

// 修改后:
func (r *RateLimiter) Wait(ctx context.Context, points int) {
    ticker := time.NewTicker(10 * time.Millisecond)
    defer ticker.Stop()
    for {
        if r.Allow(points) {
            return
        }
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
        }
    }
}
```

需同步更新所有 `Wait` 调用方传入 ctx。

## 10. 信号处理竞态修复 (M3)

### 10.1 修改

**文件**: `cmd/migrate/run.go:78-84`

```go
// 修改前:
go func() {
    sig := <-sigChan
    logger.Info("received shutdown signal", zap.String("signal", sig.String()))
    logger.Info("marking in-progress tasks for resume...")
    migrationEngine.MarkInProgressAsInterrupted(ctx)
    cancel()
}()

// 修改后:
go func() {
    sig := <-sigChan
    logger.Info("received shutdown signal", zap.String("signal", sig.String()))
    cancel()  // 先取消 context，让 Run 停止
}()

// Run 返回后再标记中断
if err := migrationEngine.Run(ctx); err != nil {
    // ...
}

// 标记进行中的任务为中断状态（此时 Run 已停止，无竞态）
migrationEngine.MarkInProgressAsInterrupted(context.Background())
```

## 11. mapping 不可变复制 (M4)

### 11.1 修改

**文件**: `internal/engine/migration.go:1261`

```go
// 修改前:
if mapping.TimeWindow == "" {
    mapping.TimeWindow = "168h"
}

// 修改后:
// 创建副本，不修改原始 mapping
windowMapping := *mapping
if windowMapping.TimeWindow == "" {
    windowMapping.TimeWindow = "168h"
}
// 后续使用 windowMapping 而非 mapping
```

## 12. 零时间戳警告 (M5)

### 12.1 修改

**文件**: `internal/adapter/target/influxdb.go:146-163`

```go
// 在 formatInfluxLine 中，当 r.Time == 0 时记录警告:
if r.Time == 0 {
    logger.Warn("record has zero timestamp, InfluxDB will assign current time",
        zap.String("measurement", r.Measurement))
}
```

## 13. V1 parseTime 增加 int64 (M6)

### 13.1 修改

**文件**: `internal/adapter/source/influxdb.go:346-369`

```go
// 在 parseTime 的 switch 中增加:
case int64:
    return time.Unix(val, 0), nil
```

## 14. Files Summary

| File | Fixes | Severity |
|------|-------|----------|
| `internal/engine/migration.go` | C1 配置补全, M4 mapping 不可变 | CRITICAL + MEDIUM |
| `internal/adapter/source/influxdb.go` | C2 tag/field 区分, H1 body 排空, H2 批量恢复, M2 时间单位, M6 parseTime, M2b int64 | CRITICAL + HIGH + MEDIUM |
| `internal/adapter/target/influxdb.go` | H3 状态码检查, M5 零时间戳 | HIGH + MEDIUM |
| `internal/checkpoint/store.go` | H4 列名修正 | HIGH |
| `internal/engine/ratelimiter.go` | H6 context 支持 | HIGH |
| `cmd/migrate/run.go` | M3 竞态修复 | MEDIUM |
