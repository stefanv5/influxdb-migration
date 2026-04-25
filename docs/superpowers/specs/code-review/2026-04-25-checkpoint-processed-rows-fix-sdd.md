# TC-M-D05 Checkpoint ProcessedRows Bug Fix Specification

## 1. 问题描述

### 1.1 Bug 现象

E2E 测试 TC-M-D05 (高基数压力测试) 报告显示：
- **预期**: ~143,920 行迁移完成
- **实际**: `processed_rows: 53`

### 1.2 根因分析

**Batch 模式下的 `ProcessedRows` 被滥用：**

| 代码位置 | `ProcessedRows` 值 | 实际含义 |
|----------|---------------------|----------|
| `migration.go:742` | `int64(windowIdx + 1)` | 窗口索引（1, 2, 3...） |
| `migration.go:771` | `int64(len(windows))` | 窗口数量（不是行数！） |
| `generator.go:97` | `totalRows += cp.ProcessedRows` | 累加窗口索引当成行数 |

**问题链条：**
1. Batch 模式将 `ProcessedRows` 用于存储窗口索引（用于断点续传恢复）
2. 报告生成器误将窗口索引理解为行数
3. 最终报告显示 53（53 个时间窗口）而非 143,920（实际迁移行数）

### 1.3 影响范围

- **断点续传功能**: 依赖 `ProcessedRows` 判断恢复位置，batch 模式正确工作
- **报告准确性**: `total_rows` 和 `transferred_rows` 显示错误数值
- **数据完整性验证**: 用户无法通过报告判断迁移是否完整

---

## 2. 修复方案

### 2.1 设计原则

1. **向后兼容**: 不破坏现有的断点续传逻辑
2. **职责分离**: `ProcessedRows` 保持其原有语义（进度追踪），新增字段专门用于报告
3. **最小化变更**: 只改必要文件，不引入不必要的重构

### 2.2 解决方案: 新增 `TotalMigratedRows` 字段

**核心思路:**
- `ProcessedRows`: 保持原意 - 用于断点续传的进度标识（batch 模式存窗口索引）
- `TotalMigratedRows`: 新增字段 - 追踪实际迁移到 target 的行数

**数据流:**
```
迁移过程:
  processBatch() 被调用
    → 累计实际迁移行数到 TotalMigratedRows
    → checkpoint 保存时同时保存 ProcessedRows(窗口索引) 和 TotalMigratedRows(实际行数)

报告生成:
  checkpoint.TotalMigratedRows → 报告的 total_rows / transferred_rows
```

---

## 3. 文件修改规格

### 3.1 pkg/types/checkpoint.go

**新增字段:**
```go
type Checkpoint struct {
    // ... existing fields ...

    // ProcessedRows is used for resume progress tracking:
    // - In single mode: the last processed row ID
    // - In batch/shard-group mode: the window/batch index for resume
    ProcessedRows int64

    // TotalMigratedRows tracks the actual number of rows migrated to target.
    // This is the authoritative count for reporting, regardless of mode.
    TotalMigratedRows int64
}
```

### 3.2 internal/checkpoint/store.go

**3.2.1 Schema 变更:**

`initSchema()` 中 checkpoints 表新增列：
```sql
ALTER TABLE checkpoints ADD COLUMN total_migrated_rows INTEGER DEFAULT 0;
```

**3.2.2 SaveCheckpoint 变更:**

```go
func (s *SQLiteStore) SaveCheckpoint(cp *types.Checkpoint) error {
    query := `
    INSERT INTO checkpoints (..., total_migrated_rows)
    VALUES (..., ?)
    ON CONFLICT(task_id, source_table) DO UPDATE SET
        ...
        total_migrated_rows = excluded.total_migrated_rows
    `
    // ... 执行时传入 cp.TotalMigratedRows
}
```

**3.2.3 LoadCheckpoint 变更:**

```go
func (s *SQLiteStore) LoadCheckpoint(...) (*types.Checkpoint, error) {
    // SELECT 语句添加 total_migrated_rows 列
    // Scan 时填充 cp.TotalMigratedRows
}
```

### 3.3 internal/engine/migration.go

**3.3.1 Track total migrated rows in batch mode:**

在 `runTaskBatchMode()` 中，processBatch 回调内累计行数：

```go
// 在 windowIdx 循环之前添加
var totalMigratedRows int64

// 在 processBatch 回调中
func(records []types.Record) error {
    if len(records) == 0 {
        return nil
    }
    err := e.processBatch(ctx, task.Mapping, records, targetAdapter)
    if err != nil {
        return err
    }
    totalMigratedRows += int64(len(records))  // 累计实际迁移行数
    return nil
}

// checkpoint 保存时
windowCP := &types.Checkpoint{
    ...
    ProcessedRows:     int64(windowIdx + 1),  // 窗口索引（保持不变）
    TotalMigratedRows: totalMigratedRows,       // 实际行数
}
```

**3.3.2 Track total migrated rows in shard-group mode:**

类似地在 `migrateTimeWindow()` 中累计：

```go
var totalMigratedRows int64

// 在 batch 处理循环内
batchCheckpoint, err := sourceAdapter.QueryDataBatch(...,
    func(records []types.Record) error {
        return e.processBatch(ctx, task.Mapping, records, targetAdapter)
    }, ...)
if batchCheckpoint != nil {
    lastTimestamp = batchCheckpoint.LastTimestamp
    totalMigratedRows += batchCheckpoint.ProcessedRows  // 累计
}
```

**3.3.3 Track total migrated rows in single mode:**

在 `runTaskSingleMode()` 中，processBatch 回调内直接累计：

```go
var totalMigratedRows int64

checkpoint, queryErr := sourceAdapter.QueryData(...,
    func(records []types.Record) error {
        if len(records) == 0 {
            return nil
        }
        err := e.processBatch(ctx, task.Mapping, records, targetAdapter)
        if err != nil {
            return err
        }
        totalMigratedRows += int64(len(records))
        return nil
    }, ...)

// 保存时
cp := &types.Checkpoint{
    ...
    ProcessedRows:     totalMigratedRows,  // single 模式直接存行数
    TotalMigratedRows: totalMigratedRows,
}
```

### 3.4 internal/report/generator.go

**3.4.1 报告生成逻辑变更:**

```go
for _, cp := range checkpoints {
    // 使用 TotalMigratedRows 代替 ProcessedRows 进行统计
    totalRows += cp.TotalMigratedRows

    if cp.Status == types.StatusCompleted {
        transferredRows += cp.TotalMigratedRows
    } else if cp.Status == types.StatusFailed {
        failedRows += cp.TotalMigratedRows
    }
}
```

**3.4.2 CheckpointEntry 结构变更:**

```go
type CheckpointEntry struct {
    ...
    ProcessedRows     int64 `json:"processed_rows"`      // 保持 - 用于调试
    TotalMigratedRows int64 `json:"total_migrated_rows"` // 新增 - 用于报告
}
```

---

## 4. 测试验证

### 4.1 验证步骤

1. **清理环境:**
   ```bash
   rm -rf test/e2e/checkpoints/tc-m-d05/*
   curl -s -X POST "http://127.0.0.1:8086/query" --data-urlencode "q=DROP DATABASE test_target"
   curl -s -X POST "http://127.0.0.1:8086/query" --data-urlencode "q=CREATE DATABASE test_target"
   ```

2. **重新写入测试数据:**
   ```bash
   ./test/e2e/scripts/setup_test_data.sh cleanup
   ./test/e2e/scripts/setup_test_data.sh setup
   ```

3. **运行 TC-M-D05:**
   ```bash
   ./migrate run -c test/e2e/config_tc_m_d05.yaml 2>&1
   ```

4. **验证报告:**
   - 打开 `test/e2e/reports/tc-m-d05/TC-M-D05_*.json`
   - 检查 `total_migrated_rows` 字段值是否接近 150000（允许小误差）
   - 检查 `transferred_rows` 字段是否等于 `total_migrated_rows`

5. **验证源和目标一致:**
   ```bash
   # Source count
   curl -s -G "http://127.0.0.1:8084/query" --data-urlencode "db=test_source" --data-urlencode "q=SELECT COUNT(*) FROM metrics"
   # Target count
   curl -s -G "http://127.0.0.1:8086/query" --data-urlencode "db=test_target" --data-urlencode "q=SELECT COUNT(*) FROM metrics"
   ```

### 4.2 预期结果

| 指标 | 预期值 | 修复前 | 修复后 |
|------|--------|--------|--------|
| `total_rows` | ~150,000 | 53 | ~150,000 |
| `transferred_rows` | ~150,000 | 53 | ~150,000 |
| checkpoint `TotalMigratedRows` | ~150,000 | N/A | ~150,000 |
| Source = Target | 相等 | 可能 | 相等 |

---

## 5. 兼容性说明

### 5.1 数据库兼容性

- **新字段默认值**: `total_migrated_rows` 默认为 0
- **已有 checkpoint**: 旧记录 `TotalMigratedRows = 0`，报告生成时会正常处理

### 5.2 API 兼容性

- **断点续传**: 不受影响，`ProcessedRows` 语义未变
- **报告 JSON**: 新增 `total_migrated_rows` 字段是增量变更，向后兼容

---

## 6. 变更文件清单

| 文件 | 变更类型 | 变更内容 |
|------|----------|----------|
| `pkg/types/checkpoint.go` | 修改 | 新增 `TotalMigratedRows` 字段 |
| `internal/checkpoint/store.go` | 修改 | Schema 添加列，Save/Load 方法处理新字段 |
| `internal/engine/migration.go` | 修改 | 在 processBatch 回调中累计 `totalMigratedRows` |
| `internal/report/generator.go` | 修改 | 使用 `TotalMigratedRows` 生成报告 |
| `test/e2e/reports/tc-m-d05/TC-M-D05_*.json` | 验证 | 验证修复后的报告数值 |

---

## 7. 实施顺序

1. **修改 types/checkpoint.go** - 新增字段
2. **修改 checkpoint/store.go** - 数据库 Schema 和读写逻辑
3. **修改 engine/migration.go** - 累计实际迁移行数
4. **修改 report/generator.go** - 报告生成使用新字段
5. **运行测试验证** - 清理环境，重新运行 TC-M-D05，检查报告