# MySQL Target Adapter Specification

## Overview

The MySQL Target Adapter enables the migration tool to write data to MySQL databases from any supported source (InfluxDB, TDengine, etc.).

## Adapter Information

| Property | Value |
|----------|-------|
| Name | `mysql` |
| Registered Name | `mysql` |
| Supported Versions | 5.7, 8.0 |
| Protocol | Native Go mysql driver |

## Interface Implementation

```go
type MySQLTargetAdapter struct {
    db     *sql.DB
    config *MySQLTargetConfig
    schema *SchemaResolver
}

type MySQLTargetConfig struct {
    Host     string
    Port     int
    Username string
    Password string
    Database string
    SSL     SSLConfig
}

type SSLConfig struct {
    Enabled   bool
    SkipVerify bool
    CaCert   string
}
```

## Methods

### Connect

```go
func (a *MySQLTargetAdapter) Connect(ctx context.Context, config map[string]interface{}) error
```

**Behavior:**
1. Decode config from map
2. Build DSN string
3. Open MySQL connection
4. Configure connection pool (MaxOpenConns, MaxIdleConns, ConnMaxLifetime)
5. Verify connection with Ping

**DSN Format:**
```
{username}:{password}@tcp({host}:{port})/{database}?charset=utf8mb4&parseTime=True&loc=Local
```

### Disconnect

```go
func (a *MySQLTargetAdapter) Disconnect(ctx context.Context) error
```

**Behavior:**
1. Close database connection
2. Log disconnection

### Ping

```go
func (a *MySQLTargetAdapter) Ping(ctx context.Context) error
```

**Behavior:**
1. Execute `SELECT 1`
2. Return error if fails

### WriteBatch

```go
func (a *MySQLTargetAdapter) WriteBatch(ctx context.Context, table string, records []types.Record) error
```

**Behavior:**
1. Skip if records is empty
2. Convert records to INSERT SQL
3. Use `INSERT ... ON DUPLICATE KEY UPDATE` for idempotency
4. Execute batch insert
5. Log batch size on debug level

**SQL Template:**
```sql
INSERT INTO {table} ({columns})
VALUES ({placeholders})
ON DUPLICATE KEY UPDATE {update_columns}
```

**Record Mapping:**
| Record Component | MySQL Column |
|-----------------|--------------|
| Fields[key] | Column (name = key) |
| Tags[key] | Column (name = key) |
| Time | timestamp column |

### MeasurementExists

```go
func (a *MySQLTargetAdapter) MeasurementExists(ctx context.Context, name string) (bool, error)
```

**Behavior:**
1. Execute `SHOW TABLES LIKE '{name}'`
2. Return true if result exists

### CreateMeasurement

```go
func (a *MySQLTargetAdapter) CreateMeasurement(ctx context.Context, schema *types.Schema) error
```

**Behavior:**
1. If schema is nil, do nothing (table assumed to exist)
2. Build CREATE TABLE DDL based on schema
3. Execute DDL

**DDL Template:**
```sql
CREATE TABLE IF NOT EXISTS {table} (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    {columns},
    timestamp DATETIME(3) NOT NULL,
    UNIQUE KEY uk_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
```

## Record Conversion

### Field Types
| Go Type | MySQL Type |
|---------|------------|
| float64 | DOUBLE |
| int64 | BIGINT |
| string | VARCHAR(255) |
| bool | TINYINT(1) |
| time.Time | DATETIME(3) |

### Tag Handling
- Tags are converted to VARCHAR columns
- Empty tag values are skipped

### NULL Handling
- Fields with nil values are skipped (not written)
- Empty tag values are skipped

## Idempotency

Uses `ON DUPLICATE KEY UPDATE` with timestamp as unique key:
- If timestamp exists, update the row
- If timestamp is new, insert new row

## Error Handling

| Error | Action |
|-------|--------|
| Connection refused | Return error (retry via engine) |
| Duplicate key | OK (idempotent) |
| Deadlock | Return error (retry via engine) |
| Lock wait timeout | Return error (retry via engine) |
| Table not found | Return error (user must create or use CreateMeasurement) |

## Logging

| Event | Level | Fields |
|-------|-------|--------|
| Connection established | Info | host, port, database |
| Disconnection | Info | database |
| Ping success | Debug | - |
| Batch written | Debug | table, count |
| Table created | Info | table |
| Write error | Error | table, error |

## Dependencies

```go
_ "github.com/go-sql-driver/mysql"
```

## Configuration Example

```yaml
targets:
  - name: "mysql-backup"
    type: "mysql"
    host: "localhost"
    port: 3306
    username: "root"
    password: "${MYSQL_PASSWORD}"
    database: "metrics"
    ssl:
      enabled: false
```
