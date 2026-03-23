# Design: InfluxDB to MySQL/TDengine Target Adapters

## 1. Overview

This design specifies the implementation of MySQL and TDengine as target adapters, enabling the migration tool to write data from any supported source (particularly InfluxDB) to MySQL or TDengine databases.

## 2. Architecture

### 2.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     MigrationEngine                               │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐  │
│  │   Source    │───▶│  Transform  │───▶│   Target Adapter    │  │
│  │  Adapters   │    │   Engine    │    │   (NEW: MySQL/TD)  │  │
│  │             │    │             │    │                     │  │
│  │ - MySQL     │    │ - Filter    │    │ - WriteBatch()      │  │
│  │ - TDengine  │    │ - Convert   │    │ - MeasurementExists │  │
│  │ - InfluxDB  │    │ - Schema    │    │ - CreateMeasurement  │  │
│  └─────────────┘    └─────────────┘    └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow

```
InfluxDB Source                      MySQL/TDengine Target
┌──────────────┐                   ┌──────────────────┐
│ QueryData()  │ ── []Record ────▶ │ WriteBatch()      │
│              │                   │                   │
│ - Fields     │                   │ INSERT ... VALUES │
│ - Tags       │                   │ ON DUPLICATE KEY  │
│ - Time       │                   │                   │
└──────────────┘                   └──────────────────┘
```

## 3. MySQL Target Adapter Design

### 3.1 File Structure
```
internal/adapter/target/
├── influxdb.go      (existing)
└── mysql.go        (NEW)
```

### 3.2 Interface Implementation

```go
type MySQLTargetAdapter struct {
    db     *sql.DB
    config *MySQLTargetConfig
}

type MySQLTargetConfig struct {
    Host     string
    Port     int
    Username string
    Password string
    Database string
    SSL     SSLConfig
}
```

### 3.3 Key Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| Connect | `Connect(ctx, config) error` | Establish MySQL connection |
| Disconnect | `Disconnect(ctx) error` | Close connection |
| Ping | `Ping(ctx) error` | Health check |
| WriteBatch | `WriteBatch(ctx, table, records) error` | Batch insert with ON DUPLICATE KEY |
| MeasurementExists | `MeasurementExists(ctx, table) (bool, error)` | Check if table exists |
| CreateMeasurement | `CreateMeasurement(ctx, schema) error` | Create table with schema |

### 3.4 WriteBatch Implementation

**SQL Template:**
```sql
INSERT INTO {table} (host, region, cpu_usage, time)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    cpu_usage = VALUES(cpu_usage)
```

**Record to MySQL Mapping:**
| Record Field | MySQL Column | Type |
|--------------|--------------|------|
| Fields[key] | Column name | VARCHAR/FLOAT/INT based on value |
| Tags[key] | Column name | VARCHAR |
| Time | time column | TIMESTAMP/DATETIME |

### 3.5 Schema Mapping

Records from InfluxDB have:
- `Fields`: map[string]interface{} (float64, int64, string, bool)
- `Tags`: map[string]string
- `Time`: time.Time

MySQL table requires explicit schema mapping in config:
```yaml
mappings:
  - source_table: "cpu_metrics"
    target_measurement: "cpu_metrics"  # Table name
    schema:
      fields:
        - source: "host"       # Field key in Record
          target: "host_name"  # Column name in MySQL
          type: "varchar(255)"
        - source: "cpu_usage"
          target: "cpu_percent"
          type: "float"
      tags:
        - source: "region"
          target: "region_code"
          type: "varchar(50)"
      time_column: "timestamp"
```

### 3.6 Auto Table Creation DDL

```sql
CREATE TABLE IF NOT EXISTS {table} (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    {tag_columns},
    {field_columns},
    timestamp DATETIME(3) NOT NULL,
    UNIQUE KEY uk_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 4. TDengine Target Adapter Design

### 4.1 File Structure
```
internal/adapter/target/
├── influxdb.go      (existing)
├── mysql.go         (NEW)
└── tdengine.go      (NEW)
```

### 4.2 Interface Implementation

```go
type TDengineTargetAdapter struct {
    client  *http.Client
    config  *TDengineTargetConfig
    baseURL string
}

type TDengineTargetConfig struct {
    Host     string
    Port     int
    Username string
    Password string
    Database string
    Version  string  # "2.x" or "3.x"
    SSL     SSLConfig
}
```

### 4.3 Key Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| Connect | `Connect(ctx, config) error` | Setup HTTP client |
| Disconnect | `Disconnect(ctx) error` | Close connections |
| Ping | `Ping(ctx) error` | Health check via REST |
| WriteBatch | `WriteBatch(ctx, table, records) error` | Batch insert via REST SQL |
| MeasurementExists | `MeasurementExists(ctx, table) (bool, error)` | SHOW {database}.{table} |
| CreateMeasurement | `CreateMeasurement(ctx, schema) error` | CREATE TABLE / CREATE STABLE |

### 4.4 WriteBatch Implementation

**REST API:**
```
POST {host}:{port}/rest/sql
Body: INSERT INTO {database}.{table} VALUES (...)
```

**TDengine Line Format:**
```
{table},{tags} {fields} {timestamp}
```

**Example:**
```
cpu_metrics,host=server1,region=us cpu_usage=85.5,memory_usage=70.2 1709853600000000000
```

### 4.5 TDengine Schema

TDengine uses supertables (STABLE) and subtables:

```sql
-- Super table
CREATE STABLE IF NOT EXISTS metrics (
    cpu_usage FLOAT,
    memory_usage FLOAT,
    timestamp TIMESTAMP
) TAGS (host VARCHAR(64), region VARCHAR(32));

-- Sub table (auto created via INSERT)
INSERT INTO metrics_01 USING metrics TAGS ('server1', 'us') VALUES (...)
```

## 5. Configuration Schema Changes

### 5.1 Target Config Types

Add to `pkg/types/config.go`:

```go
type TargetConfig struct {
    Name string
    Type string  // "influxdb-v1", "influxdb-v2", "mysql", "tdengine"
    
    // MySQL specific
    MySQL *MySQLConfig `mapstructure:"mysql"`
    
    // TDengine specific  
    TDengine *TDengineConfig `mapstructure:"tdengine"`
    
    // Common InfluxDB (existing)
    InfluxDB *InfluxDBConfig `mapstructure:"influxdb"`
}
```

### 5.2 Config Validation

Update `internal/config/validator.go`:

```go
func (v *ConfigValidator) validateTargets() {
    for _, tgt := range v.config.Targets {
        switch tgt.Type {
        case "influxdb-v1", "influxdb-v2":
            // existing validation
        case "mysql":
            validateMySQLTarget(tgt)
        case "tdengine":
            validateTDengineTarget(tgt)
        default:
            v.errors = append(v.errors, fmt.Errorf(
                "target %s: type must be influxdb-v1, influxdb-v2, mysql, or tdengine", 
                tgt.Name))
        }
    }
}
```

## 6. Error Handling

### 6.1 MySQL Errors
| Error | Handling |
|-------|----------|
| Connection failed | Return error, retry with backoff |
| Duplicate key | OK (ON DUPLICATE KEY handles) |
| Deadlock | Retry with backoff |
| Lock wait timeout | Retry with backoff |

### 6.2 TDengine Errors
| Error | Handling |
|-------|----------|
| Connection failed | Return error, retry with backoff |
| Table not exist | Auto create if schema provided |
| Invalid SQL | Return error |
| Timeout | Retry with backoff |

## 7. Idempotency

Both adapters ensure idempotent writes:

- **MySQL**: `INSERT ... ON DUPLICATE KEY UPDATE` uses timestamp as unique key
- **TDengine**: `INSERT` with same timestamp updates existing record

## 8. NULL Handling

- Skip NULL values in Fields (do not write column)
- Skip empty string Tags (do not write tag)
- Consistent with existing InfluxDB target behavior

## 9. Batch Sizing

- MySQL: Configurable via `chunk_size` (default 10000)
- TDengine: Configurable via `chunk_size` (default 10000)
- Use same batching as source adapters

## 10. Logging

Add structured logging for:

| Event | Level | Fields |
|-------|-------|--------|
| Connection established | Info | host, database |
| Batch written | Debug | table, count |
| Table created | Info | table, ddl |
| Write failed | Error | table, error |
| Query failed | Error | query, error |
