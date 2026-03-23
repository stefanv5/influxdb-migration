## Why

随着时序数据库（TSDB）生态的发展，企业内部往往同时使用 MySQL、TDengine、InfluxDB 等多种数据库。缺乏统一的迁移工具导致数据迁移成本高、风险大、易出错。目前业界缺少一个支持多源、插件化、具备断点续传能力的时序数据库迁移工具。

本工具旨在提供一站式解决方案，支持 MySQL、TDengine (2.X/3.X)、InfluxDB (1.X/2.X) 到 InfluxDB 的迁移，具备插件化架构、断点续传、增量同步、流量控制等企业级特性。

## What Changes

### 核心功能

- **多源支持**：支持 MySQL、TDengine 2.X/3.X、InfluxDB 1.X/2.X 作为源端
- **插件化架构**：新数据库适配仅需实现 SourceAdapter/TargetAdapter 接口
- **断点续传**：基于 SQLite 的 Checkpoint 管理，任务失败后可从失败点继续
- **增量同步**：基于 timestamp 列的增量数据迁移
- **批量迁移 + 流式处理**：大批量数据分批处理，InfluxDB 使用 SHOW SERIES 流式发现
- **流量控制**：令牌桶限流、源端保护（查询间隔、最大 QPS）
- **迁移报告**：支持 JSON/HTML/Markdown 格式的详细迁移报告
- **SSL/TLS 支持**：支持证书验证、Skip Verify 模式
- **连接池管理**：可配置的最大连接数、空闲连接数、连接生命周期

### 源端保护机制

| 数据库 | 发现命令 | 查询策略 |
|--------|----------|----------|
| MySQL | SHOW TABLES | WHERE (ts, id) > (last_ts, last_id) |
| TDengine | SHOW TABLES | WHERE tbname = 'xxx' AND ts BETWEEN |
| InfluxDB | SHOW SERIES | WHERE tag='value' AND time BETWEEN |

### 数据一致性

- **At-Least-Once + Batch 幂等**
- **NULL 处理**：跳过不写入
- **MySQL Decimal**：转换为 float

### 任务粒度

- **Job**：完整迁移任务
- **Task**：Table/Measurement 级别
- **Chunk**：批次级别（默认 10000 行/批）

## Capabilities

### New Capabilities

- `source-adapter`: 源数据库适配器框架
- `target-adapter`: 目标数据库适配器框架
- `checkpoint-manager`: 断点管理器
- `migration-engine`: 迁移引擎核心
- `rate-limiter`: 流量控制器
- `report-generator`: 迁移报告生成器
- `config-validator`: 配置校验器

## Impact

- 新增目录结构：cmd/migrate, internal/{adapter,checkpoint,engine,report}, pkg/types
- 依赖库：go-sql-driver/mysql, go-influxdb1-client/v2, tdengine-go, viper, cobra
