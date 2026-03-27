# Design: MySQL DiscoverSchema Interface

## 1. Overview

This design specifies adding a `DiscoverSchema` method to the `SourceAdapter` interface and implementing it for MySQL to enable dynamic schema discovery instead of hardcoded column names.

## 2. New Types

### 2.1 Schema Types

```go
// pkg/types/schema.go
package types

type Schema struct {
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

## 3. Interface Change

### 3.1 Current Interface

```go
type SourceAdapter interface {
    Name() string
    DiscoverTables(ctx context.Context) ([]string, error)
    DiscoverSeries(ctx context.Context, measurement string) ([]string, error)
    QueryData(ctx context.Context, table string, lastCheckpoint *Checkpoint, batchFunc func([]Record) error, cfg *QueryConfig) (*Checkpoint, error)
}
```

### 3.2 Proposed Interface

```go
type SourceAdapter interface {
    Name() string
    DiscoverTables(ctx context.Context) ([]string, error)
    DiscoverSeries(ctx context.Context, measurement string) ([]string, error)
    DiscoverSchema(ctx context.Context, table string) (*Schema, error)
    QueryData(ctx context.Context, table string, lastCheckpoint *Checkpoint, batchFunc func([]Record) error, cfg *QueryConfig) (*Checkpoint, error)
}
```

## 4. MySQL DiscoverSchema Implementation

### 4.1 SQL Query

```go
func (a *MySQLAdapter) DiscoverSchema(ctx context.Context, table string) (*types.Schema, error) {
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

    var schema types.Schema
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

## 5. MySQL QueryData Changes

### 5.1 Current Implementation

```go
func (a *MySQLAdapter) QueryData(...) {
    query := fmt.Sprintf(`
        SELECT id, timestamp, host_id, cpu_usage, memory_usage
        FROM %s
        WHERE ...`, quoteIdentifier(table))
}
```

### 5.2 Proposed Implementation

```go
func (a *MySQLAdapter) QueryData(...) {
    schema, err := a.DiscoverSchema(ctx, table)
    if err != nil {
        return nil, err
    }

    // Build SELECT columns dynamically
    var columns []string
    for _, col := range schema.Columns {
        columns = append(columns, quoteIdentifier(col.Name))
    }

    query := fmt.Sprintf(`SELECT %s FROM %s WHERE ...`,
        strings.Join(columns, ", "),
        quoteIdentifier(table))
}
```

## 6. Default Schema Fallback

For backward compatibility, if DiscoverSchema fails:

```go
// Default fallback schema for common use case
defaultSchema := &types.Schema{
    TableName: table,
    Columns: []types.Column{
        {Name: "id", Type: "bigint"},
        {Name: "timestamp", Type: "datetime"},
        {Name: "host_id", Type: "varchar"},
        {Name: "cpu_usage", Type: "float"},
        {Name: "memory_usage", Type: "float"},
    },
    PrimaryKey: "id",
    TimestampColumn: "timestamp",
}
```

## 7. Files to Modify

```
pkg/types/
├── schema.go              # NEW: Schema and Column types

internal/adapter/source/
├── mysql.go               # Implement DiscoverSchema, update QueryData
├── influxdb.go            # Add stub DiscoverSchema (not applicable)
├── tdengine.go            # Add stub DiscoverSchema (not applicable)
├── registry_test.go      # Update mock

internal/engine/
├── migration.go           # Optionally call DiscoverSchema before QueryData
```
