# 复杂数据场景 E2E 测试用例补充

## 1. 概述

本文档补充 `2026-04-22-influxdbv1-e2e-test-design.md` 中缺失的复杂数据场景测试用例。

### 1.1 补充范围

- **测试类型:** 数据完整性 (Data Integrity)
- **数据集规模:** 小规模、中规模
- **补充用例:** 4 个

---

## 2. 补充测试用例

### TC-S-D05: Field/Tag 名称含特殊字符

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-S-D05 |
| 规模 | 小规模 |
| 验证点 | 字段名和标签名含特殊字符 |

**输入:**
- Source: InfluxDB v1
  - Field 名称: `cpu usage` (空格), `field.name` (点号), `cpu-usage` (中划线), `a=b` (等号), `x,y` (逗号)
  - Tag 名称: `region/name` (斜杠), `env[prod]` (方括号)
  - 每字段各 100 条记录

**验证方法:**
1. 写入源数据
2. 执行 single 模式迁移
3. 查询 target，获取 field/tag 名称列表
4. 对比名称是否完全一致

**预期输出:**
- 所有 field 名称正确保留，无解析错误
- 所有 tag 名称正确保留，无解析错误
- 记录数: 100 条 × 字段数

---

### TC-S-D06: Tag Value 含分隔符

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-S-D06 |
| 规模 | 小规模 |
| 验证点 | Tag value 含 InfluxDB 分隔符字符 |

**输入:**
- Source: InfluxDB v1
  - `tag_v1=us-west,1` (逗号)
  - `tag_v2=host=server1` (等号)
  - `tag_v3=multi\nline` (换行符)
  - `tag_v4=` (空字符串)
  - `tag_v5= ` (仅空格)
  - 每组合 100 条记录

**验证方法:**
1. 写入源数据（使用 line protocol 转义）
2. 执行 single 模式迁移
3. 查询 target，对比 tag values

**预期输出:**
- 含逗号/等号的 value 完整保留
- 含换行的 value 保留为 `multi\nline`
- 空字符串和空格 value 保留
- 无分隔符被误解析为 tag key-value 分隔

---

### TC-S-D07: 数值边界与特殊浮点

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-S-D07 |
| 规模 | 小规模 |
| 验证点 | 特殊浮点值和类型转换边界 |

**输入:**
- Source: InfluxDB v1
  - `nan_val`: `NaN` (Not a Number)
  - `pos_inf`: `+Inf` (正无穷)
  - `neg_inf`: `-Inf` (负无穷)
  - `neg_uint`: `-1` (写入时目标声明为 unsigned 类型)
  - 每字段各 100 条记录

**验证方法:**
1. 写入源数据
2. 执行 single 模式迁移
3. 查询 target，对比每种类型

**预期输出:**
- `NaN` → 保持为 NaN 或转换为安全的字符串表示
- `+Inf` → 保持或安全降级，无 panic
- `-Inf` → 保持或安全降级，无 panic
- `-1` unsigned → 报错或安全处理，无数据损坏

**注意:** InfluxDB Line Protocol 不直接支持 NaN/Inf，写入时可能需要预处理（如转为字符串或使用替代值）。

---

### TC-M-D05: 高 Series 基数压力测试

| 属性 | 值 |
|------|-----|
| 用例编号 | TC-M-D05 |
| 规模 | 中规模 |
| 验证点 | 高 series 基数下的内存和 checkpoint 正确性 |

**输入:**
- Source: InfluxDB v1
  - 1 个 measurement
  - `host` tag: 50 个不同值
  - `region` tag: 10 个不同值
  - `env` tag: 3 个不同值
  - 总 series 数: 50 × 10 × 3 = 1500
  - 每 series 100 条记录
  - 总计: 150,000 条记录

**验证方法:**
1. 写入 150,000 条记录
2. 配置 `parallel_tasks: 4`
3. 执行 batch 模式迁移
4. 监控内存使用（峰值 < 8GB）
5. 验证 checkpoint 文件存在且正确

**预期输出:**
- 迁移完成，150,000 条记录全部到达 target
- 无 OOM 崩溃
- 无数据丢失或重复
- Checkpoint 文件包含正确的 processed_rows ≈ 150,000

---

## 3. 测试数据集模板

### TC-S-D05 / TC-S-D06 / TC-S-D07 共享数据集

```yaml
# test/e2e/datasets/complex_chars.yaml
dataset:
  name: "complex_chars_dataset"
  scale: "small"
  measurements:
    - name: "special_names"
      tags:
        - name: "region/name"
          values: ["us-west"]
        - name: "env[prod]"
          values: ["prod"]
      fields:
        - name: "cpu usage"
          type: "float"
          min: 0.0
          max: 100.0
        - name: "field.name"
          type: "float"
          min: 0.0
          max: 100.0
        - name: "cpu-usage"
          type: "float"
          min: 0.0
          max: 100.0
        - name: "a=b"
          type: "float"
          min: 0.0
          max: 100.0
        - name: "x,y"
          type: "float"
          min: 0.0
          max: 100.0
      records_per_series: 100

    - name: "special_tag_values"
      tags:
        - name: "tag_v1"
          values: ["us-west,1", "us-east,2"]
        - name: "tag_v2"
          values: ["host=server1", "host=server2"]
        - name: "tag_v3"
          values: ["multi\nline"]
        - name: "tag_v4"
          values: [""]
        - name: "tag_v5"
          values: [" "]
      fields:
        - name: "value"
          type: "float"
          min: 0.0
          max: 100.0
      records_per_series: 100

    - name: "special_floats"
      tags:
        - name: "host"
          values: ["server-001"]
      fields:
        - name: "nan_val"
          type: "float"
          special: "nan"
        - name: "pos_inf"
          type: "float"
          special: "positive_inf"
        - name: "neg_inf"
          type: "float"
          special: "negative_inf"
      records_per_series: 100
```

### TC-M-D05 数据集

```yaml
# test/e2e/datasets/high_cardinality.yaml
dataset:
  name: "high_cardinality_dataset"
  scale: "medium"
  measurements:
    - name: "metrics"
      tags:
        - name: "host"
          values: ["server-{001..050}"]  # 50 values
        - name: "region"
          values: ["us-west", "us-east", "eu-west", "ap-east", "sa-east", "ap-south", "eu-north", "us-central", "ap-northeast", "sa-north"]  # 10 values
        - name: "env"
          values: ["prod", "staging", "dev"]  # 3 values
      fields:
        - name: "value"
          type: "float"
          min: 0.0
          max: 100.0
      records_per_series: 100
  total_series: 1500
  total_records: 150000
```

---

## 4. 实现优先级

| 优先级 | 用例 | 理由 |
|--------|------|------|
| P0 | TC-S-D05 | Field/Tag 名称含特殊字符是常见场景 |
| P0 | TC-S-D06 | Tag value 分隔符是 InfluxDB 解析的高风险区 |
| P1 | TC-S-D07 | 特殊浮点值影响数据完整性 |
| P1 | TC-M-D05 | 高基数是性能/稳定性关键场景 |

---

## 5. 验收标准

所有用例需满足：
- [ ] 数据完整（记录数、field 值、tag value 完全一致）
- [ ] 特殊字符无解析错误
- [ ] 无 panic 或异常退出
- [ ] Checkpoint 正确保存
