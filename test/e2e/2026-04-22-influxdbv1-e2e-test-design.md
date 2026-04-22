# InfluxDB v1 → InfluxDB v1 端到端测试用例设计

## 1. 概述

本文档定义 InfluxDB v1 源到 InfluxDB v1 目标的端到端黑盒测试用例。

### 1.1 测试范围

- **迁移路径**: InfluxDB v1 → InfluxDB v1
- **测试维度**: 功能（Function）、数据完整性（Data Integrity）、可靠性（Reliability）
- **数据集规模**: 小规模、中规模、大规模三层

### 1.2 硬件约束与本地部署

- 内存限制: 8 GB
- 大规模测试总量: ~10 万条记录

#### 本地 InfluxDB v1 实例

| 角色 | 地址 | 端口 | 状态 |
|------|------|------|------|
| Source | 127.0.0.1 | 8084 | ✅ 正常 |
| Target | 127.0.0.1 | 8086 | ✅ 正常 |

> **注意**: 两个实例均为单机部署，测试前需确保实例可连接且有足够权限。

---

## 2. 数据集规格

### 2.1 规模分层

| 规模 | Measurement 数量 | Tag Keys/Measurement | 记录数/Measurement | 总记录数 | 时间窗口 |
|------|-----------------|--------------------|-------------------|---------|---------|
| 小规模 | 1 | 2 | 100 | ~100 | 短/中/长各3组 |
| 中规模 | 3-5 | 3-5 | 5000 | ~15000-25000 | 短/中/长各3组 |
| 大规模 | 5-10 | 5 | ~10000-20000 | ~100000 | 短/中/长各3组 |

### 2.2 时间窗口规格

| 窗口类型 | 时间范围 | 说明 |
|---------|---------|------|
| 短 | 1-2 天 | 快速验证场景 |
| 中 | 7-14 天 | 覆盖 weekly shard 完整场景 |
| 长 | 30+ 天 | 跨多 shard boundary |

### 2.3 高基数场景约束

| 参数 | 值 |
|------|-----|
| Series 总量上限 | 100,000 |
| 时间线（记录数）下限 | 80,000 |

### 2.4 Tag 配置

| 规模 | Tag Keys | 示例 |
|------|---------|------|
| 小规模 | 2 | `host`, `region` |
| 中规模 | 3-5 | `host`, `region`, `env`, `az`, `cluster` |
| 大规模 | 5 | `host`, `region`, `env`, `az`, `cluster` |

---

## 3. 测试用例清单

### 3.1 功能测试（Function）

#### TC-S-F01: 单 measurement 基本写入读取

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-S-F01 |
| 规模 | 小规模 |
| 查询模式 | single |
| 数据集 | 1 measurement, 2 tags, 100 条记录 |

**输入:**
- Source: InfluxDB v1，measurement=`cpu`, tags=`{host: server-001, region: us-west}`, 100 条记录
- 时间窗口: 短/中/长各执行一次

**操作步骤:**
1. 向 source 写入 100 条记录
2. 执行 single 模式迁移
3. 从 target 读取全部数据

**预期输出:**
- Target 存在 100 条记录
- tag `host` 和 `region` 完整保留
- field 数据类型和值完全一致

---

#### TC-S-F02: 多 measurement 串联迁移

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-S-F02 |
| 规模 | 小规模 |
| 查询模式 | single |
| 数据集 | 3 measurements, 2 tags, 100 条/measurement |

**输入:**
- Source: InfluxDB v1
  - measurement=`cpu`, tags=`{host: server-001, region: us-west}`, 100 条
  - measurement=`memory`, tags=`{host: server-001, region: us-west}`, 100 条
  - measurement=`disk`, tags=`{host: server-001, region: us-west}`, 100 条

**操作步骤:**
1. 向 source 写入 3 个 measurement 各 100 条
2. 执行 single 模式迁移
3. 从 target 读取全部数据

**预期输出:**
- Target 存在 3 个 measurement
- 每个 measurement 各 100 条记录
- 所有 tag 和 field 完整保留

---

#### TC-M-F01: batch 模式批量查询

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-M-F01 |
| 规模 | 中规模 |
| 查询模式 | batch |
| 数据集 | 5 measurements, 3-5 tags, 5000 条/measurement |

**输入:**
- Source: InfluxDB v1
  - 5 个 measurements
  - 每个 5000 条记录
  - 总计 25000 条

**操作步骤:**
1. 向 source 写入 25000 条记录
2. 配置 `query_mode: "batch"`, `max_series_per_query: 100`
3. 执行 batch 模式迁移
4. 从 target 读取全部数据

**预期输出:**
- Target 存在 25000 条记录
- batch 查询次数明显少于 single 模式
- 5 个 measurement 数据均完整

---

#### TC-M-F02: 多 series 并行查询

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-M-F02 |
| 规模 | 中规模 |
| 查询模式 | batch |
| 数据集 | 20 series 并发 |

**输入:**
- Source: InfluxDB v1
  - 1 个 measurement
  - 20 个不同 tag 组合的 series
  - 每个 series 1000 条记录

**操作步骤:**
1. 向 source 写入 20 个 series
2. 配置并行任务数 `parallel_tasks: 4`
3. 执行 batch 模式迁移
4. 记录执行时间

**预期输出:**
- 20 个 series 数据全部迁移成功
- 无数据竞争或数据丢失
- 并行执行时间显著短于串行

---

#### TC-M-F03: 限速机制验证

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-M-F03 |
| 规模 | 中规模 |
| 查询模式 | batch |

**输入:**
- Source: InfluxDB v1，5000 条记录
- 配置: `rate_limiter.enabled: true`, `points_per_second: 10000`

**操作步骤:**
1. 配置限速参数
2. 执行迁移
3. 监控实际 QPS

**预期输出:**
- 实际 QPS 不超过 10000
- 所有数据完整迁移
- 无因限速导致的数据丢失

---

#### TC-M-F04: 时间范围过滤

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-M-F04 |
| 规模 | 中规模 |
| 查询模式 | batch |

**输入:**
- Source: InfluxDB v1，30 天数据（7200 条/天）
- 过滤条件: `start='2026-04-01'`, `stop='2026-04-15'`

**操作步骤:**
1. 向 source 写入 30 天数据
2. 配置时间范围过滤
3. 执行迁移

**预期输出:**
- 仅迁移 4 月 1 日至 4 月 15 日数据
- 约 7200 × 14 = 100800 条记录（抽样验证）
- 超出时间范围的数据不存在于 target

---

#### TC-M-F05: 并行任务数配置

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-M-F05 |
| 规模 | 中规模 |
| 查询模式 | batch |

**输入:**
- Source: InfluxDB v1，5 个 measurements，各 5000 条
- 配置: `parallel_tasks: 4`

**操作步骤:**
1. 配置 `parallel_tasks: 4`
2. 执行迁移
3. 记录执行时间

**预期输出:**
- 4 个任务并行执行
- 执行时间约为串行的 1/4
- 数据完整性不受并行数影响

---

#### TC-L-F01: shard-group 模式分批迁移

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-L-F01 |
| 规模 | 大规模 |
| 查询模式 | shard-group |
| 数据集 | ~10 万条跨 shard 边界 |

**输入:**
- Source: InfluxDB v1，10 万条记录，跨越多个 shard group
- 配置: `query_mode: "shard-group"`, `series_batch_size: 50`

**操作步骤:**
1. 向 source 写入 10 万条记录（覆盖 30 天窗口）
2. 配置 shard-group 模式
3. 执行迁移
4. 监控内存使用

**预期输出:**
- 按 shard 边界分批查询
- 内存使用稳定，无 OOM
- 所有数据完整迁移

---

#### TC-L-F02: 多 measurement 协同迁移

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-L-F02 |
| 规模 | 大规模 |
| 查询模式 | shard-group |
| 数据集 | 8 measurements, 各约 1.2 万条 |

**输入:**
- Source: InfluxDB v1
  - 8 个 measurements
  - 每个约 1.2 万条
  - 总计约 10 万条

**操作步骤:**
1. 向 source 写入 8 个 measurements
2. 执行 shard-group 模式迁移
3. 验证所有 measurement 数据

**预期输出:**
- 8 个 measurement 数据均完整
- 每个约 1.2 万条记录
- 无 measurement 遗漏

---

#### TC-L-F03: 大时间窗口覆盖

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-L-F03 |
| 规模 | 大规模 |
| 查询模式 | shard-group |
| 数据集 | 30 天数据跨多 shard |

**输入:**
- Source: InfluxDB v1，30 天数据，分布在多个 shard groups
- 配置: `query_mode: "shard-group"`, `time_window: 168h`（weekly）

**操作步骤:**
1. 向 source 写入 30 天数据
2. 配置 weekly 时间窗口
3. 执行迁移
4. 按 shard 分批验证

**预期输出:**
- 每个 shard 的数据完整
- 跨 shard 边界无数据丢失
- shard 组内数据连续

---

### 3.2 数据完整性测试（Data Integrity）

#### TC-S-D01: 小规模记录数和 tag/field 完整性

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-S-D01 |
| 规模 | 小规模 |
| 验证点 | 记录数、tag、field |

**输入:**
- Source: 1 measurement, 2 tags, 100 条记录
- Tag keys: `host`, `region`
- Fields: `cpu_usage` (float), `status` (string)

**验证方法:**
1. 统计 source 和 target 记录数
2. 对比 tag keys 和 values
3. 对比 field keys 和 values

**预期输出:**
- Source 和 target 记录数相等: 100 = 100
- Tag keys 集合相等: `{host, region}` = `{host, region}`
- Field keys 集合相等: `{cpu_usage, status}` = `{cpu_usage, status}`

---

#### TC-S-D02: 小规模数据类型一致性

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-S-D02 |
| 规模 | 小规模 |
| 验证点 | 数据类型 |

**输入:**
- Source: 混合数据类型
  - `float_val`: 3.14159
  - `int_val`: 42
  - `string_val`: "hello world"
  - `bool_val`: true

**验证方法:**
1. 查询 source 每条记录的字段类型
2. 查询 target 对应记录的字段类型
3. 对比类型是否完全一致

**预期输出:**
- `float_val`: float64 = float64
- `int_val`: int64 = int64
- `string_val`: string = string
- `bool_val`: bool = bool

---

#### TC-S-D03: 字段值精确性

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-S-D03 |
| 规模 | 小规模 |
| 验证点 | 特殊字符、空值、科学计数法 |

**输入:**
- Source: 100 条记录
  - 特殊字符: `"tag=value,with comma"`
  - 空值: `""`
  - 科学计数法: `1.23e-10`
  - Unicode: `"中文测试"`
  - 换行符: `"line1\nline2"`

**验证方法:**
1. 对比 source 和 target 每个字段的字符串表示
2. 逐字节对比特殊字符

**预期输出:**
- 特殊字符完全一致
- 空值处理正确
- 科学计数法精度保留
- Unicode 正确
- 换行符正确

---

#### TC-S-D04: 时间戳完全一致

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-S-D04 |
| 规模 | 小规模 |
| 验证点 | 时间戳无误差 |

**输入:**
- Source: 100 条记录，使用已知时间戳
  - `t1 = 1745280000000000000` (2026-04-22 00:00:00 UTC)
  - `t2 = 1745283600000000000` (2026-04-22 01:00:00 UTC)
  - ...

**验证方法:**
1. 查询 source 每条记录的时间戳
2. 查询 target 对应记录的时间戳
3. 计算误差: `|target_time - source_time|`

**预期输出:**
- 误差 = 0（纳秒级完全一致）
- 无时间戳丢失
- 时间顺序保持一致

---

#### TC-M-D01: 中等规模记录数完整性

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-M-D01 |
| 规模 | 中规模 |
| 验证点 | 多 measurement 记录数 |

**输入:**
- Source: 5 个 measurements，各 5000 条
  - `cpu`: 5000 条
  - `memory`: 5000 条
  - `disk`: 5000 条
  - `network`: 5000 条
  - `process`: 5000 条

**验证方法:**
1. 对每个 measurement 统计 source 和 target 记录数
2. 计算总和

**预期输出:**
- `cpu`: 5000 = 5000
- `memory`: 5000 = 5000
- `disk`: 5000 = 5000
- `network`: 5000 = 5000
- `process`: 5000 = 5000
- 总计: 25000 = 25000

---

#### TC-M-D02: 时间排序验证

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-M-D02 |
| 规模 | 中规模 |
| 验证点 | 时间升序排列 |

**输入:**
- Source: 5000 条记录，时间戳随机分布（乱序写入）

**验证方法:**
1. 查询 target 数据，按时间升序排列
2. 检查时间戳是否单调非递减

**预期输出:**
- 时间戳序列: `t1 <= t2 <= t3 <= ... <= tn`
- 无时间倒序记录

---

#### TC-M-D03: 高基数 tag value 校验

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-M-D03 |
| 规模 | 中规模 |
| 验证点 | 高基数 tag（50+ 不同值） |

**输入:**
- Source: 1 measurement
  - tag `host`: 50 个不同值（`server-001` 到 `server-050`）
  - tag `region`: 5 个不同值（`us-west-1`, `us-east-1`, `eu-west-1`, `ap-east-1`, `sa-east-1`）
  - 总 series 数: 50 × 5 = 250
  - 每个 series 20 条记录

**验证方法:**
1. 统计 source 各 tag value 的记录数
2. 统计 target 对应 tag value 的记录数
3. 对比每个组合的计数

**预期输出:**
- 每个 `host` tag value 在 target 中均有对应记录
- 每个 `region` tag value 在 target 中均有对应记录
- tag value 组合数量一致: 250 = 250
- 每个组合的记录数一致

---

#### TC-M-D04: 字段类型校验

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-M-D04 |
| 规模 | 中规模 |
| 验证点 | 多种类型无转换错误 |

**输入:**
- Source: 5000 条记录，包含所有支持类型
  - `float_field`: float64
  - `int_field`: int64
  - `string_field`: string
  - `bool_field`: bool
  - `uint_field`: unsigned integer

**验证方法:**
1. 抽样 100 条记录
2. 对比 source 和 target 每种字段的 Go 类型
3. 验证类型映射正确

**预期输出:**
- float64 → float64
- int64 → int64
- string → string
- bool → bool
- 无类型自动转换

---

#### TC-L-D01: 跨 shard 边界数据完整性

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-L-D01 |
| 规模 | 大规模 |
| 验证点 | shard 边界处数据不丢失 |

**输入:**
- Source: 10 万条记录，跨越 shard boundaries
- Shard group 长度: 7 天（weekly）
- 时间窗口: 30 天（跨越 4-5 个 shard groups）

**验证方法:**
1. 获取 source 的 shard group 信息
2. 在每个 shard 边界处抽样 100 条记录
3. 验证 target 中这些记录存在
4. 统计每个 shard 的记录数

**预期输出:**
- 每个 shard 边界处数据无丢失
- 各 shard 记录数与 source 一致
- 无跨 shard 重复记录

---

#### TC-L-D02: 高基数 tag 完整性

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-L-D02 |
| 规模 | 大规模 |
| 验证点 | 5 tags, 50+ 值 |

**输入:**
- Source: 10 万条记录
  - tag `host`: 100 个不同值
  - tag `region`: 5 个不同值
  - tag `env`: 3 个不同值
  - tag `az`: 3 个不同值
  - tag `cluster`: 2 个不同值
  - 总 series 数: 100 × 5 × 3 × 3 × 2 = 9000

**验证方法:**
1. 抽样 1000 条记录
2. 对比 source 和 target 的 tag values
3. 统计各 tag 的基数

**预期输出:**
- 各 tag 基数保持不变
- tag value 组合数量一致
- 无 tag 信息丢失

---

### 3.3 可靠性测试（Reliability）

#### TC-L-R01: checkpoint 保存验证

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-L-R01 |
| 规模 | 大规模 |
| 场景 | 迁移到 50% 时保存 checkpoint |

**输入:**
- Source: 10 万条记录
- 配置: `checkpoint_dir: "./checkpoints"`

**操作步骤:**
1. 启动迁移
2. 在迁移约 50% 时中断（发送 SIGTERM）
3. 检查 checkpoint 文件内容

**预期输出:**
- Checkpoint 文件存在
- 包含 `last_timestamp` 和 `processed_rows`
- `processed_rows` 约 50000
- Migration status 为 `in_progress`

---

#### TC-L-R02: 单次断点续传

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-L-R02 |
| 规模 | 大规模 |
| 场景 | 中断后从 checkpoint 恢复 |

**输入:**
- Source: 10 万条记录
- 中断点: 约 50%

**操作步骤:**
1. 启动迁移
2. 在约 50% 时中断
3. 从 checkpoint 恢复迁移
4. 验证最终数据完整性

**预期输出:**
- 续传后完成 100% 迁移
- 总记录数: 100000 = 100000
- 无数据丢失
- 无数据重复

---

#### TC-L-R03: 多次中断续传

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-L-R03 |
| 规模 | 大规模 |
| 场景 | 多次中断续传 |

**输入:**
- Source: 10 万条记录

**操作步骤:**
1. 启动迁移
2. 在 25% 时中断
3. 恢复迁移
4. 在 50% 时中断
5. 恢复迁移
6. 在 75% 时中断
7. 恢复迁移至完成

**预期输出:**
- 每次续传均成功
- 最终记录数: 100000 = 100000
- 无数据丢失
- 无数据重复
- 总执行时间合理（允许多次重启开销）

---

## 4. 测试数据集模板

### 4.1 小规模数据集模板

```yaml
# test/e2e/datasets/small.yaml
dataset:
  name: "small_dataset"
  scale: "small"
  measurements:
    - name: "cpu"
      tags:
        - name: "host"
          values: ["server-001"]
        - name: "region"
          values: ["us-west"]
      fields:
        - name: "cpu_usage"
          type: "float"
          min: 0.0
          max: 100.0
        - name: "status"
          type: "string"
          values: ["ok", "warning", "error"]
      records_per_series: 100
      time_window:
        short: "1d"
        medium: "7d"
        long: "30d"
```

### 4.2 中规模数据集模板

```yaml
# test/e2e/datasets/medium.yaml
dataset:
  name: "medium_dataset"
  scale: "medium"
  measurements:
    - name: "cpu"
      tags:
        - name: "host"
          values: ["server-001", "server-002", "server-003", "server-004", "server-005"]
        - name: "region"
          values: ["us-west", "us-east", "eu-west"]
        - name: "env"
          values: ["prod", "staging", "dev"]
        - name: "az"
          values: ["az-1", "az-2"]
        - name: "cluster"
          values: ["cluster-a", "cluster-b"]
      fields:
        - name: "cpu_usage"
          type: "float"
          min: 0.0
          max: 100.0
        - name: "memory_usage"
          type: "float"
          min: 0.0
          max: 64.0
      records_per_series: 5000
    - name: "memory"
      # ... similar structure
    - name: "disk"
      # ... similar structure
    - name: "network"
      # ... similar structure
    - name: "process"
      # ... similar structure
  total_records: "~15000-25000"
```

### 4.3 大规模数据集模板

```yaml
# test/e2e/datasets/large.yaml
dataset:
  name: "large_dataset"
  scale: "large"
  constraints:
    max_series: 100000
    min_timeline: 80000
  measurements:
    - name: "metrics"
      count: 8
      tags:
        - name: "host"
          values: ["server-{001..100}"]  # 100 values
        - name: "region"
          values: ["us-west", "us-east", "eu-west", "ap-east", "sa-east"]
        - name: "env"
          values: ["prod", "staging", "dev"]
        - name: "az"
          values: ["az-1", "az-2", "az-3"]
        - name: "cluster"
          values: ["cluster-a", "cluster-b"]
      fields:
        - name: "value"
          type: "float"
          min: 0.0
          max: 100.0
      records_per_measurement: "~10000-20000"
  total_records: "~100000"
  time_windows:
    short: "1d"
    medium: "7d"
    long: "30d"
```

---

## 5. 测试执行要求

### 5.1 前置条件

1. 两个 InfluxDB v1 实例已部署并运行：
   - Source: 127.0.0.1:8084
   - Target: 127.0.0.1:8086
2. 测试用户具备读写权限
3. 8 GB 可用内存
4. 足够的磁盘空间存储测试数据

### 5.2 环境清理

每个测试用例执行前必须清理：
- Source 数据库
- Target 数据库
- Checkpoint 目录
- Report 目录

### 5.3 验收标准

| 维度 | 通过条件 |
|------|---------|
| 功能 | 所有功能测试用例执行通过 |
| 数据完整性 | 记录数、类型、值、时间戳均一致 |
| 可靠性 | 断点续传测试成功，无数据丢失或重复 |

### 5.4 测试报告

每个测试用例执行后生成报告：
- 测试用例 ID
- 执行时间
- 通过/失败状态
- 失败原因（如有）
- 数据完整性校验结果

### 5.5 数据准备脚本

使用 `test/e2e/scripts/setup_test_data.sh` 脚本管理测试数据：

```bash
# 设置测试数据（创建数据库并写入数据）
./test/e2e/scripts/setup_test_data.sh setup

# 验证现有数据
./test/e2e/scripts/setup_test_data.sh verify

# 清理测试数据
./test/e2e/scripts/setup_test_data.sh cleanup
```

**环境变量：**
- `SOURCE_URL` - Source InfluxDB 地址（默认：`http://127.0.0.1:8084`）
- `SOURCE_DB` - Source 数据库名称（默认：`test_source`）
- `SOURCE_RP` - Source Retention Policy（可选）

**写入的数据：**
- TC-S-F01: `cpu` measurement, 100 条记录
- TC-S-F02: `cpu`, `memory`, `disk` measurements, 各 100 条记录

---

## 6. 附录

### 6.1 术语表

| 术语 | 说明 |
|------|-----|
| Measurement | InfluxDB 中的数据表 |
| Tag | InfluxDB 中的索引字段 |
| Field | InfluxDB 中的非索引字段 |
| Series | measurement + tag 组合 |
| Shard | InfluxDB 存储数据的单元 |
| Shard Group | 同一时间范围的 shard 集合 |
| Checkpoint | 迁移进度保存点 |

### 6.2 参考文档

- [InfluxDB v1 文档](https://docs.influxdata.com/influxdb/v1/)
- [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v1/write_protocols/line_protocol_reference/)
- [迁移工具 README](../../README.md)
