# InfluxDB-to-InfluxDB 迁移代码审查修复

## 1. Problem Statement

代码审查发现 InfluxDB-to-InfluxDB 迁移路径存在 2 个严重（CRITICAL）、6 个高危（HIGH）、6 个中等（MEDIUM）问题，涉及数据丢失、认证失败、数据模型损坏等风险。

### P0-1: V2 源端缺少 V1 兼容 API 认证凭据 (CRITICAL)

**位置**: `internal/engine/migration.go:1517-1524`

V2 源适配器内部使用 V1 兼容 API（InfluxQL）执行查询，需要 `username`/`password` 进行 HTTP Basic 认证。但 `sourceConfigToMap` 只传递了 `url`、`token`、`org`、`bucket`、`version`，缺少 `username`、`password`、`retention_policy`。

`decodeInfluxV2Config`（source/influxdb.go:1659）会尝试读取这些 key，读不到就是空字符串。`buildV1QueryParams`（line 894）把空的 `u` 和 `p` 拼到请求里。

**影响**: V2 源查询全部认证失败，或在匿名访问场景下查错数据库。

### P0-2: V1 源端把所有字符串值错误归类为 Tag (CRITICAL)

**位置**: `internal/adapter/source/influxdb.go:707-713`

`parseValues` 把所有 string 类型值存为 Tag。但 InfluxDB V1 `SELECT *` 同时返回 Tag 值和 String Field 值，两者在 JSON 中都是 string 类型。String Field 被写成 Tag 后，数据模型被静默损坏（索引方式、基数统计、字符限制全部改变）。

**影响**: 迁移后目标库的数据模型与源库不一致，属于静默数据损坏。

### P1-1: HTTP 响应体未排空导致连接泄漏 (HIGH)

**位置**: `internal/adapter/source/influxdb.go:599-603`、`1005-1007`

`executeChunkedQuery` 和 `executeV1ChunkedQuery` 在 `client.Do(req)` 返回非 nil response + error 时，未排空 Body 就关闭，导致 HTTP 连接池泄漏。同文件的 `executeQuery`（line 746）正确处理了此情况。

### P1-2: V2 批量模式恢复时跳过数据 (HIGH)

**位置**: `internal/adapter/source/influxdb.go:1468-1477`

V2 `QueryDataBatch` 在恢复时用 `lastCheckpoint.LastTimestamp` 推进 `queryStart`，但 V1 适配器（line 447-450）明确不做此操作，注释写道："always use the original startTime in batch mode"。V2 的行为会在分片组模式下**跳过窗口前半段数据**。

### P1-3: V2 目标端 `executeFluxQuery` 缺少 HTTP 状态码检查 (HIGH)

**位置**: `internal/adapter/target/influxdb.go:658-669`

4xx/5xx 响应直接尝试 JSON 解码，得到令人困惑的 "failed to decode flux result" 错误，掩盖了真正的失败原因。源适配器的 `executeFluxSelect`（line 1561）正确检查了状态码。

### P1-4: `ListCheckpoints` 参数名与查询列名不匹配 (HIGH)

**位置**: `internal/checkpoint/store.go:204-208`

函数签名参数叫 `taskID`，但 SQL 过滤用 `task_name = ?`。UNIQUE 约束是 `(task_id, source_table)`。可能导致 `resume` 命令找不到正确的 checkpoint。

### P1-5: 批量模式用 `ProcessedRows` 存储窗口索引 (HIGH)

**位置**: `internal/engine/migration.go:661-662, 755`

`ProcessedRows` 字段定义是"已处理行数"，但批量模式下存的是窗口索引。报告生成和其他读取方在批量模式下会得到错误的值。

### P1-6: `RateLimiter.Wait` 不支持 Context 取消 (HIGH)

**位置**: `internal/engine/ratelimiter.go:52-56`

`Wait` 使用 `time.Sleep(10ms)` 死循环，不响应 context 取消。阻塞优雅关闭。同文件有 `WaitContext` 方法但未被使用。

### P2-1: V2 `DiscoverShardGroups` 时间单位错误 (MEDIUM)

**位置**: `internal/adapter/source/influxdb.go:1354-1357`

用 `time.Unix(s.StartTime, 0)` 处理 V2 `/api/v2/shards` 返回的时间戳，但 V2 API 返回的是纳秒不是秒。分片组模式在 V2 源上完全失效。

### P2-2: V1 `parseValues` 丢弃 int64 字段 (MEDIUM)

**位置**: `internal/adapter/source/influxdb.go:707-716`

switch 缺少 `int64` case，InfluxDB Integer Field 被静默丢弃。V2 的 `parseV1Values`（line 1104）正确处理了 `int64`。

### P2-3: 信号处理器竞态条件 (MEDIUM)

**位置**: `cmd/migrate/run.go:78-84`

信号处理 goroutine 调用 `MarkInProgressAsInterrupted(ctx)` 时，`Run` 可能还在执行并写入 checkpoint，无同步机制。

### P2-4: `queryWithTimeRange` 就地修改 `mapping.TimeWindow` (MEDIUM)

**位置**: `internal/engine/migration.go:1261`

直接修改传入指针的字段，违反不可变原则。

### P2-5: `formatInfluxLine` 对零时间戳的静默处理 (MEDIUM)

**位置**: `internal/adapter/target/influxdb.go:146-163`

`r.Time == 0` 时 InfluxDB 用 `now()` 填充，解析失败的记录被写入当前时间。

### P2-6: V1 `parseTime` 不处理 int64 (MEDIUM)

**位置**: `internal/adapter/source/influxdb.go:346-369`

缺少 `int64` case，可能返回零时间。

## 2. Goals

1. **认证修复**: V2 源端正确传递 V1 兼容 API 所需的全部凭据
2. **数据模型保真**: V1 源端正确区分 Tag 和 String Field
3. **数据完整性**: V2 批量模式恢复时不再跳过数据
4. **资源管理**: HTTP 连接正确复用，不泄漏
5. **错误可诊断**: HTTP 错误返回有意义的信息
6. **语义清晰**: Checkpoint 字段语义不再混乱
7. **优雅关闭**: RateLimiter 支持 context 取消

## 3. Non-Goals

1. 不改变三种查询模式的整体架构
2. 不修改 MySQL/TDengine 适配器
3. 不添加新的查询模式
4. 不改变配置文件格式

## 4. Priority

| 优先级 | 问题 | 影响 | 修复难度 |
|--------|------|------|---------|
| P0 | C1 V2 源认证缺失 | V2 源完全不可用 | 低 |
| P0 | C2 V1 Tag/Field 误分类 | 数据模型损坏 | 中 |
| P0 | H2 V2 批量恢复跳数据 | 数据不完整 | 低 |
| P0 | M2 V2 分片时间单位错 | 分片组模式失效 | 低 |
| P1 | H1 HTTP 连接泄漏 | 资源耗尽 | 低 |
| P1 | H3 目标端状态码检查 | 错误信息不明 | 低 |
| P1 | H4 Checkpoint 查询列名 | 恢复可能失败 | 低 |
| P2 | H5 ProcessedRows 语义 | 报告不准确 | 中 |
| P2 | H6 RateLimiter 取消 | 退出卡顿 | 低 |
| P2 | M1/M3-M6 | 边界情况 | 低-中 |

## 5. Files to Modify

```
internal/engine/
├── migration.go            # C1: 补全 V2 源配置字段
│                           # H5: ProcessedRows 语义修复
│                           # M4: mapping 不可变复制

internal/adapter/
├── source/
│   └── influxdb.go         # C2: V1 parseValues 使用 tagKeySet
│                           # H1: HTTP body 排空
│                           # H2: V2 QueryDataBatch 移除 checkpoint 推进
│                           # M2: V2 DiscoverShardGroups 时间单位
│                           # M6: V1 parseTime 增加 int64
│                           # M2b: V1 parseValues 增加 int64
└── target/
    └── influxdb.go         # H3: executeFluxQuery 状态码检查
                            # M5: formatInfluxLine 零时间处理

internal/checkpoint/
└── store.go                # H4: ListCheckpoints 列名修正

internal/engine/
└── ratelimiter.go          # H6: Wait 支持 context

cmd/migrate/
└── run.go                  # M3: 信号处理竞态修复
```

## 6. Backwards Compatibility

- 配置文件格式不变（只是补全已有字段的传递）
- Checkpoint 数据库 schema 不变
- 接口签名不变（V1 parseValues 内部改造）
- 现有 E2E 测试应继续通过

## 7. Testing Strategy

1. 每个修复对应一个单元测试或修改现有测试
2. `go vet ./...` 通过
3. `go build ./...` 通过
4. `go test ./...` 全部通过
5. E2E 测试验证 InfluxDB V1→V1 和 V2→V2 迁移正确性
