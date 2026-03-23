## Context

多模态时序数据库迁移工具是一个企业级数据迁移解决方案，支持从 MySQL、TDengine (2.X/3.X)、InfluxDB (1.X/2.X) 迁移数据到 InfluxDB。设计目标是构建一个**插件化**、**高可靠**、**易扩展**的迁移框架。

### 技术背景

| 数据库 | 发现命令 | 查询策略 | 特点 |
|--------|----------|----------|------|
| MySQL | SHOW TABLES | WHERE (ts, id) > (last_ts, last_id) | 关系型，需处理 NULL 和 Decimal |
| TDengine 2.X/3.X | SHOW TABLES | WHERE tbname = 'xxx' AND ts BETWEEN | 超表概念，支持 PARTITION BY |
| InfluxDB 1.X/2.X | SHOW SERIES | WHERE tag='value' AND time BETWEEN | 时序原生，Line Protocol |

### 约束

- Go 1.21+ 语言实现
- SQLite 用于 Checkpoint 存储
- YAML 配置 + 环境变量注入
- 源端保护：避免迁移过程对源数据库造成压力

## Goals / Non-Goals

**Goals:**
- 提供统一的 CLI 接口（run/status/resume/report/verify）
- 插件化 Adapter 架构，支持快速扩展新数据源
- 断点续传能力，任务失败后可从 Checkpoint 恢复
- 支持增量同步（基于 timestamp）
- 完整的迁移报告（JSON/HTML/Markdown）
- 流量控制和源端保护
- SSL/TLS 认证支持（包含 Skip Verify）
- 连接池管理

**Non-Goals:**
- 不支持 CDC 实时同步（仅批处理+增量）
- 不做数据转换（只做格式适配）
- 不支持非时序数据库作为目标端

## Decisions

### 1. 插件化 Adapter 架构

**决策**：定义 SourceAdapter 和 TargetAdapter 接口，通过注册机制动态加载适配器。

```go
type SourceAdapter interface {
    Name() string
    SupportedVersions() []string
    Connect(ctx context.Context, config map[string]interface{}) error
    Disconnect(ctx context.Context) error
    DiscoverTables(ctx context.Context) ([]string, error)
    QueryData(ctx context.Context, table string, lastCheckpoint *Checkpoint, batchFunc func([]Record) error) (*Checkpoint, error)
}

type TargetAdapter interface {
    Name() string
    SupportedVersions() []string
    Connect(ctx context.Context, config map[string]interface{}) error
    Disconnect(ctx context.Context) error
    WriteBatch(ctx context.Context, measurement string, records []Record) error
}
```

**理由**：接口隔离，支持独立开发、测试各数据库适配器。

### 2. 三层任务粒度

| 层级 | 粒度 | 并行性 | Checkpoint 单位 |
|------|------|--------|-----------------|
| Job | 完整迁移任务 | - | - |
| Task | Table/Measurement | 可并行 | 是 |
| Chunk | 批次(10000行) | 同一 Task 内串行 | 是 |

**理由**：Task 级别并行提升吞吐量，Chunk 级别 Checkpoint 保证恢复精度。

### 3. Checkpoint SQLite 存储

```sql
CREATE TABLE checkpoints (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT NOT NULL,
    source_table    TEXT NOT NULL,
    target_meas     TEXT NOT NULL,
    last_id         INTEGER,
    last_timestamp  TEXT,
    processed_rows  INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'pending',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE(task_id, source_table)
);
```

**理由**：轻量、单一文件、支持 SQL 查询、跨平台。

### 4. 令牌桶限流

```go
type RateLimiter struct {
    rate     float64
    burst    int
    tokens   float64
    lastTime time.Time
    mu       sync.Mutex
}
```

**理由**：平滑限流，支持突发流量，避免瞬时压垮数据库。

### 5. 源端查询策略

| 数据库 | 发现 | 查询优化 |
|--------|------|----------|
| MySQL | SHOW TABLES | 索引支持的时间分页 + ID 游标 |
| TDengine | SHOW TABLES | PARTITION BY TBNAME + 时间窗口 |
| InfluxDB | SHOW SERIES | tag 过滤 + 时间窗口分片 |

**理由**：避免全表扫描导致的 OOM 和源端压力。

### 6. 数据一致性策略

- **At-Least-Once + Batch 幂等**：每个 Chunk 作为独立事务
- **NULL 处理**：跳过 NULL 字段不写入
- **MySQL Decimal**：转换为 float

## Risks / Trade-offs

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 源端数据库压力过大 | 业务受影响 | 限流 + 查询间隔 + 读从库 |
| 大数据量迁移 OOM | 迁移失败 | Chunk 分批 + 流式处理 |
| 目标端写入速率不足 | 迁移时间长 | 并发写入 + 批量提交 |
| Checkpoint 丢失 | 任务需重启 | SQLite 定期持久化 |
| TDengine 2.X/3.X 差异 | 适配器复杂 | 版本检测 + 差异化实现 |

## Migration Plan

1. **Phase 1**: 项目初始化（Go mod、目录结构、配置系统）
2. **Phase 2**: 插件系统（接口定义、注册机制）
3. **Phase 3**: Checkpoint 管理（SQLite、CRUD、恢复逻辑）
4. **Phase 4**: Source Adapters（MySQL → TDengine → InfluxDB）
5. **Phase 5**: Target Adapters（InfluxDB 1.X/2.X）
6. **Phase 6**: 迁移引擎（并发控制、重试、流控）
7. **Phase 7**: CLI 命令（run/status/resume/report）
8. **Phase 8**: 报告系统（JSON/HTML/Markdown）
9. **Phase 9**: 测试（单元、集成、端到端）

## Open Questions

1. TDengine 3.X WebSocket 连接库是否稳定？
2. InfluxDB 2.X 的 Flux 查询 vs SQL 查询性能差异？
3. 是否需要支持目标端 InfluxDB 自动创建 Measurement？
