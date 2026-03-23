# TDengine Target Adapter Specification

## Overview

The TDengine Target Adapter enables the migration tool to write data to TDengine databases from any supported source (InfluxDB, MySQL, etc.) using the REST API.

## Adapter Information

| Property | Value |
|----------|-------|
| Name | `tdengine` |
| Registered Name | `tdengine` |
| Supported Versions | 2.x, 3.x |
| Protocol | REST API |

## Interface Implementation

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
    Version  string  // "2.x" or "3.x"
    SSL     SSLConfig
}

type SSLConfig struct {
    Enabled   bool
    SkipVerify bool
}
```

## Methods

### Connect

```go
func (a *TDengineTargetAdapter) Connect(ctx context.Context, config map[string]interface{}) error
```

**Behavior:**
1. Decode config from map
2. Build base URL (`http(s)://{host}:{port}`)
3. Configure HTTP client with SSL if enabled
4. Set basic auth credentials
5. Log connection info

### Disconnect

```go
func (a *TDengineTargetAdapter) Disconnect(ctx context.Context) error
```

**Behavior:**
1. Close idle HTTP connections
2. Log disconnection

### Ping

```go
func (a *TDengineTargetAdapter) Ping(ctx context.Context) error
```

**Behavior:**
1. Execute `SHOW DATABASES` via REST API
2. Return error if fails

### WriteBatch

```go
func (a *TDengineTargetAdapter) WriteBatch(ctx context.Context, table string, records []types.Record) error
```

**Behavior:**
1. Skip if records is empty
2. Convert records to TDengine SQL
3. Execute batch insert via REST SQL endpoint
4. Log batch size on debug level

**TDengine SQL Format:**
```sql
INSERT INTO {database}.{table} ({tag_names}) VALUES ({tag_values})
```

**TDengine Line Protocol:**
```
{table},{tags} {fields} {timestamp}
```

**Example:**
```
metrics,host=server1,region=us cpu_usage=85.5,memory_usage=70.2 1709853600000000000
```

### Record Mapping

| Record Component | TDengine |
|-----------------|----------|
| Tags[key] | Tags (VARCHAR) |
| Fields[key] | Fields (FLOAT, INT, etc.) |
| Time | Timestamp (nanoseconds) |

### TDengine Data Types

| Go Type | TDengine Type |
|---------|--------------|
| float64 | FLOAT |
| int64 | BIGINT |
| string | VARCHAR |
| bool | BOOL |
| time.Time | TIMESTAMP |

### MeasurementExists

```go
func (a *TDengineTargetAdapter) MeasurementExists(ctx context.Context, name string) (bool, error)
```

**Behavior:**
1. Execute `SHOW {database}.TABLES` via REST
2. Look for table name in results
3. Return true if exists

### CreateMeasurement

```go
func (a *TDengineTargetAdapter) CreateMeasurement(ctx context.Context, schema *types.Schema) error
```

**Behavior:**
1. If schema is nil, do nothing
2. Build CREATE TABLE or CREATE STABLE DDL
3. Execute DDL via REST SQL

**For TDengine 2.x with Super Table:**
```sql
CREATE STABLE IF NOT EXISTS {table} (
    {field_definitions}
) TAGS ({tag_definitions})
```

**For regular table:**
```sql
CREATE TABLE IF NOT EXISTS {table} (
    {field_definitions}
)
```

## Idempotency

TDengine's `INSERT` is naturally idempotent:
- Same timestamp with same tags updates existing record
- Different timestamp creates new record

## NULL Handling

- Fields with nil values are skipped (not written)
- Empty tag values are skipped

## Error Handling

| Error | Action |
|-------|--------|
| Connection refused | Return error (retry via engine) |
| Table not found | Auto-create if schema provided |
| Invalid SQL | Return error |
| Timeout | Return error (retry via engine) |

## TDengine REST API

### Endpoint
```
POST {host}:{port}/rest/sql
```

### Request Format
```json
{
  "sql": "INSERT INTO ...",
  "db": "{database}"
}
```

### Response Format
```json
{
  "code": 0,
  "message": "success",
  "data": [...]
}
```

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

Uses existing HTTP client (no new dependency):
```go
// No new imports needed - uses net/http from source adapter
```

## Configuration Example

```yaml
targets:
  - name: "tdengine-backup"
    type: "tdengine"
    host: "localhost"
    port: 6030
    username: "root"
    password: "${TD_PASSWORD}"
    database: "metrics"
    version: "3.x"
    ssl:
      enabled: false
```
