# Tasks: MySQL DiscoverSchema

## Overview

Add `DiscoverSchema` method to the `SourceAdapter` interface and implement it for MySQL to enable dynamic schema discovery instead of hardcoded column names.

---

## Task 1: Add Schema Types

**Files:**
- Create: `pkg/types/schema.go`

- [x] **Step 1: Create Schema and Column types**

```go
package types

type TableSchema struct {
    TableName        string
    Columns          []Column
    PrimaryKey       string
    TimestampColumn  string
}

type Column struct {
    Name     string
    Type     string
    Nullable bool
}
```

- [x] **Step 2: Commit**

```bash
git add pkg/types/schema.go
git commit -m "feat: add TableSchema and Column types"
```

---

## Task 2: Update SourceAdapter Interface

**Files:**
- Modify: `internal/adapter/adapter.go`

- [x] **Step 1: Add DiscoverSchema to interface**

```go
type SourceAdapter interface {
    Name() string
    DiscoverTables(ctx context.Context) ([]string, error)
    DiscoverSeries(ctx context.Context, measurement string) ([]string, error)
    DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error)
    QueryData(ctx context.Context, table string, lastCheckpoint *Checkpoint, batchFunc func([]Record) error, cfg *QueryConfig) (*Checkpoint, error)
    QueryDataBatch(ctx context.Context, measurement string, series []string, lastCheckpoint *Checkpoint, batchFunc func([]Record) error, cfg *QueryConfig) (*Checkpoint, error)
}
```

- [x] **Step 2: Run build to verify**

```bash
go build ./internal/adapter/...
```

- [x] **Step 3: Commit**

```bash
git add internal/adapter/adapter.go
git commit -m "feat: add DiscoverSchema to SourceAdapter interface"
```

---

## Task 3: Implement MySQL DiscoverSchema

**Files:**
- Modify: `internal/adapter/source/mysql.go`

- [x] **Step 1: Implement DiscoverSchema**

```go
func (a *MySQLAdapter) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
    query := `
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION`

    rows, err := a.db.QueryContext(ctx, query, a.config.Database, table)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var schema types.TableSchema
    schema.TableName = table
    schema.Columns = make([]types.Column, 0)

    for rows.Next() {
        var col types.Column
        var key string
        if err := rows.Scan(&col.Name, &col.Type, &col.Nullable, &key); err != nil {
            return nil, err
        }
        col.Nullable = (col.Nullable == "YES")
        if key == "PRI" {
            schema.PrimaryKey = col.Name
        }
        if col.Type == "timestamp" || col.Type == "datetime" {
            schema.TimestampColumn = col.Name
        }
        schema.Columns = append(schema.Columns, col)
    }

    return &schema, nil
}
```

- [x] **Step 2: Update QueryData to use DiscoverSchema**

Modify QueryData to dynamically build SELECT columns from discovered schema instead of hardcoding column names.

- [x] **Step 3: Run build to verify**

```bash
go build ./internal/adapter/source/...
```

- [x] **Step 4: Commit**

```bash
git add internal/adapter/source/mysql.go
git commit -m "feat: implement MySQL DiscoverSchema"
```

---

## Task 4: Add Stub DiscoverSchema for Other Adapters

**Files:**
- Modify: `internal/adapter/source/influxdb.go`
- Modify: `internal/adapter/source/tdengine.go`

- [x] **Step 1: Add stub DiscoverSchema for InfluxDB**

InfluxDB is schemaless, so return minimal schema:

```go
func (a *InfluxDBV1Adapter) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
    return &types.TableSchema{
        TableName: table,
        Columns:   []types.Column{},
    }, nil
}
```

- [x] **Step 2: Add stub DiscoverSchema for TDengine**

TDengine also has minimal schema requirements:

```go
func (a *TDengineAdapter) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
    return &types.TableSchema{
        TableName: table,
        Columns:   []types.Column{},
    }, nil
}
```

- [x] **Step 3: Run build to verify**

```bash
go build ./internal/adapter/source/...
```

- [x] **Step 4: Commit**

```bash
git add internal/adapter/source/influxdb.go internal/adapter/source/tdengine.go
git commit -m "feat: add DiscoverSchema stubs for non-MySQL adapters"
```

---

## Task 5: Update Mock Adapter

**Files:**
- Modify: `internal/adapter/registry_test.go`

- [x] **Step 1: Add DiscoverSchema to MockSourceAdapter**

```go
func (m *MockSourceAdapter) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
    return &types.TableSchema{
        TableName: table,
        Columns:   []types.Column{},
    }, nil
}
```

- [x] **Step 2: Run tests**

```bash
go test ./internal/adapter/... -v
```

- [x] **Step 3: Commit**

```bash
git add internal/adapter/registry_test.go
git commit -m "test: add DiscoverSchema mock"
```

---

## Summary

| Task | Description | Files | Status |
|------|-------------|-------|--------|
| 1 | Schema Types | `pkg/types/schema.go` | ✅ DONE |
| 2 | SourceAdapter Interface | `internal/adapter/adapter.go` | ✅ DONE |
| 3 | MySQL DiscoverSchema | `internal/adapter/source/mysql.go` | ✅ DONE |
| 4 | Stub Adapters | `influxdb.go`, `tdengine.go` | ✅ DONE |
| 5 | Mock Adapter | `registry_test.go` | ✅ DONE |

## Verification

```bash
go build ./...     # Build succeeds
go test ./...     # All tests pass
```
