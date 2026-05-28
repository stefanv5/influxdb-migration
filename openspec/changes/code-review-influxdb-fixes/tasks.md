# Tasks: InfluxDB-to-InfluxDB 迁移代码审查修复

## Task List

- [ ] **Task 1**: V2 源端认证凭据补全 (C1)
- [ ] **Task 2**: V1 源端 Tag/Field 正确区分 (C2 + M2b)
- [ ] **Task 3**: HTTP 响应体排空 (H1)
- [ ] **Task 4**: V2 批量模式恢复不跳数据 (H2)
- [ ] **Task 5**: V2 分片时间单位修正 (M2)
- [ ] **Task 6**: V2 目标端 HTTP 状态码检查 (H3)
- [ ] **Task 7**: Checkpoint 查询列名修正 (H4)
- [ ] **Task 8**: 信号处理竞态修复 + RateLimiter Context + mapping 不可变 + 零时间戳警告 + parseTime int64 (M3/H6/M4/M5/M6)

---

## 前置条件

- [x] 代码审查完成，问题清单已确认
- [x] openspec spec.md 和 design.md 已编写
- [ ] 当前代码可编译: `go build ./...`
- [ ] 当前测试通过: `go test ./...`

---

## Task 1: V2 源端认证凭据补全 (C1)

**文件**: `internal/engine/migration.go`

**改动**:

在 `sourceConfigToMap` 函数中，`case "influxdb"` 分支补全 `username`、`password`、`retention_policy` 字段。

```go
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

**验证**:
- `go build ./...` 通过
- 检查 `decodeInfluxV2Config`（source/influxdb.go:1659）能正确读取新增字段
- 检查 `buildV1QueryParams`（source/influxdb.go:894）使用非空凭据

---

## Task 2: V1 源端 Tag/Field 正确区分 (C2 + M2b)

**文件**: `internal/adapter/source/influxdb.go`

**改动**:

### 2.1 修改 `parseValues` 签名

```go
// 修改前:
func (a *InfluxDBV1Adapter) parseValues(columns []string, values []interface{}) types.Record {

// 修改后:
func (a *InfluxDBV1Adapter) parseValues(columns []string, values []interface{}, tagKeySet map[string]bool) types.Record {
```

### 2.2 修改 string 分支

```go
// 修改前 (line 710-713):
case string:
    record.AddTag(col, v)

// 修改后:
case string:
    if tagKeySet[col] {
        record.AddTag(col, v)
    } else {
        record.AddField(col, v)
    }
```

### 2.3 增加 int64/int case (M2b)

```go
// 在 switch 中增加:
case int64:
    record.AddField(col, v)
case int:
    record.AddField(col, int64(v))
```

### 2.4 在调用方获取 tagKeySet

在 `QueryData` 和 `QueryDataBatch` 中，调用 `executeChunkedQuery` 前：

```go
tagKeys, _ := a.DiscoverTagKeys(ctx, measurement)
tagKeySet := make(map[string]bool)
for _, k := range tagKeys {
    tagKeySet[k] = true
}
```

将 `tagKeySet` 传递给 `executeChunkedQuery` → `parseValues`。

### 2.5 更新 `executeChunkedQuery` 签名

```go
// 修改前:
func (a *InfluxDBV1Adapter) executeChunkedQuery(ctx context.Context, query string, chunkSize int, callback func([]types.Record) error) (int, int64, error) {

// 修改后:
func (a *InfluxDBV1Adapter) executeChunkedQuery(ctx context.Context, query string, chunkSize int, callback func([]types.Record) error, tagKeySet map[string]bool) (int, int64, error) {
```

在 `executeChunkedQuery` 内部调用 `parseValues` 时传入 `tagKeySet`。

**验证**:
- `go build ./...` 通过
- `go test ./...` 通过
- 单元测试: string field 不再被误判为 tag

---

## Task 3: HTTP 响应体排空 (H1)

**文件**: `internal/adapter/source/influxdb.go`

**改动**:

在 `executeChunkedQuery`（V1 adapter）和 `executeV1ChunkedQuery`（V2 adapter）中，`client.Do` 返回 error 时排空 Body：

```go
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
1. `executeChunkedQuery` (~line 599)
2. `executeV1ChunkedQuery` (~line 1005)

**验证**:
- `go build ./...` 通过
- 代码审查确认与 `executeQuery`（line 746）处理方式一致

---

## Task 4: V2 批量模式恢复不跳数据 (H2)

**文件**: `internal/adapter/source/influxdb.go`

**改动**:

在 `QueryDataBatch`（V2 adapter, line 1468-1477）中，移除 checkpoint 时间推进逻辑：

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

**验证**:
- `go build ./...` 通过
- 与 V1 `QueryDataBatch`（line 447-450）行为一致

---

## Task 5: V2 分片时间单位修正 (M2)

**文件**: `internal/adapter/source/influxdb.go`

**改动**:

在 `DiscoverShardGroups`（V2 adapter, line 1354-1357）中，修正时间单位：

```go
// 修改前:
StartTime: time.Unix(s.StartTime, 0),
EndTime:   time.Unix(s.EndTime, 0),

// 修改后:
// InfluxDB V2 /api/v2/shards returns timestamps in nanoseconds
StartTime: time.Unix(0, s.StartTime),
EndTime:   time.Unix(0, s.EndTime),
```

**验证**:
- `go build ./...` 通过
- 确认 V2 shard 时间不再落在 1970 年

---

## Task 6: V2 目标端 HTTP 状态码检查 (H3)

**文件**: `internal/adapter/target/influxdb.go`

**改动**:

在 `executeFluxQuery`（line 658-669）中，增加状态码检查：

```go
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
// ... rest unchanged
```

**验证**:
- `go build ./...` 通过
- 与源适配器 `executeFluxSelect`（line 1561）处理方式一致

---

## Task 7: Checkpoint 查询列名修正 (H4)

**文件**: `internal/checkpoint/store.go`

**改动**:

修改 `ListCheckpoints` 函数参数名：

```go
// 修改前:
func (s *SQLiteStore) ListCheckpoints(taskID string) ([]*types.Checkpoint, error) {

// 修改后:
func (s *SQLiteStore) ListCheckpoints(taskName string) ([]*types.Checkpoint, error) {
```

检查所有调用方确认传入的是 task name。

**验证**:
- `go build ./...` 通过
- `grep -rn "ListCheckpoints"` 确认所有调用方语义正确

---

## Task 8: 其他修复 (M3/H6/M4/M5/M6)

### 8.1 信号处理竞态修复 (M3)

**文件**: `cmd/migrate/run.go:78-84`

```go
// 修改后:
go func() {
    sig := <-sigChan
    logger.Info("received shutdown signal", zap.String("signal", sig.String()))
    cancel()
}()

if err := migrationEngine.Run(ctx); err != nil {
    logger.Error("migration failed", zap.Error(err))
    return fmt.Errorf("migration failed: %w", err)
}

// Run 返回后标记中断，无竞态
migrationEngine.MarkInProgressAsInterrupted(context.Background())
```

### 8.2 RateLimiter Context 支持 (H6)

**文件**: `internal/engine/ratelimiter.go:52-56`

```go
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

更新所有 `Wait` 调用方传入 ctx。

### 8.3 mapping 不可变复制 (M4)

**文件**: `internal/engine/migration.go:1261`

```go
// 创建副本
windowMapping := *mapping
if windowMapping.TimeWindow == "" {
    windowMapping.TimeWindow = "168h"
}
// 后续使用 windowMapping
```

### 8.4 零时间戳警告 (M5)

**文件**: `internal/adapter/target/influxdb.go:146-163`

```go
if r.Time == 0 {
    logger.Warn("record has zero timestamp, InfluxDB will assign current time",
        zap.String("measurement", r.Measurement))
}
```

### 8.5 V1 parseTime 增加 int64 (M6)

**文件**: `internal/adapter/source/influxdb.go:346-369`

```go
case int64:
    return time.Unix(val, 0), nil
```

**验证**:
- `go build ./...` 通过
- `go test ./...` 通过
- `go vet ./...` 通过

---

## 执行顺序

```
Task 1 (C1 配置补全) ─────────────────────────────────┐
Task 2 (C2 tag/field) ────────────────────────────────┤
Task 3 (H1 body 排空) ────────────────────────────────┤
Task 4 (H2 批量恢复) ─────────────────────────────────┤
Task 5 (M2 时间单位) ─────────────────────────────────┼──→ Task 8 (合并修复) ──→ 验证
Task 6 (H3 状态码) ───────────────────────────────────┤
Task 7 (H4 列名修正) ─────────────────────────────────┘

Task 1-7 相互独立，可并行执行。
Task 8 包含多个小修复，合并为一个提交。
```

## 完成标准

- [ ] `go build ./...` 无错误
- [ ] `go vet ./...` 无警告
- [ ] `go test ./...` 全部通过
- [ ] 所有 CRITICAL 问题已修复
- [ ] 所有 HIGH 问题已修复
- [ ] MEDIUM 问题已修复或标记为已知限制
- [ ] 每个 Task 对应一个独立的 git commit
