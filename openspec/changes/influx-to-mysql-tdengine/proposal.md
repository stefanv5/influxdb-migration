# Proposal: InfluxDB to MySQL/TDengine Target Adapters

## Summary

Add MySQL and TDengine as target adapters, enabling InfluxDB as a source to migrate data to MySQL or TDengine instances. This supports disaster recovery scenarios where data needs to be synchronized back to source systems.

## Motivation

### Current State
- Migration tool supports: MySQL, TDengine, InfluxDB (V1/V2) as **sources**
- Migration tool supports: InfluxDB (V1/V2) as **targets** only

### Problem
- No way to migrate data FROM InfluxDB TO MySQL or TDengine
- Cannot support disaster recovery scenarios requiring data rollback to source systems

### Use Cases
1. **Disaster Recovery**: InfluxDB data synchronized back to MySQL/TDengine for backup
2. **Data Verification**: Migrated InfluxDB data can be verified against source MySQL/TDengine
3. **Hybrid Architecture**: Support for multi-target migrations where InfluxDB serves as intermediate store

## Proposed Solution

### Target Adapters
| Adapter | Version | Protocol | Features |
|---------|---------|----------|----------|
| MySQL Target | 5.7, 8.0 | Native Go mysql driver | Batch INSERT, ON DUPLICATE KEY |
| TDengine Target | 2.x, 3.x | REST API | Batch INSERT (via REST) |

### Architecture
```
Current Architecture:
  Sources: MySQL, TDengine, InfluxDB V1/V2
  Targets: InfluxDB V1/V2

Proposed Architecture:
  Sources: MySQL, TDengine, InfluxDB V1/V2
  Targets: InfluxDB V1/V2, MySQL, TDengine
```

## Configuration Changes

### New Target Configurations
```yaml
targets:
  - name: "mysql-backup"
    type: "mysql"
    host: "localhost"
    port: 3306
    username: "root"
    password: "${MYSQL_PASSWORD}"
    database: "metrics"
    
  - name: "tdengine-backup"
    type: "tdengine"
    host: "localhost"
    port: 6030
    username: "root"
    password: "${TD_PASSWORD}"
    database: "metrics"
```

### New Migration Task
```yaml
tasks:
  - name: "influx-to-mysql-backup"
    source: "prod-influx"
    target: "mysql-backup"
    mappings:
      - source_table: "cpu_metrics"
        target_measurement: "cpu_metrics"  # MySQL table name
        schema:
          fields:
            - source: "host"
              target: "host_name"
              type: "string"
```

## Implementation Scope

### Phase 1: MySQL Target Adapter
- WriteBatch with batch INSERT
- ON DUPLICATE KEY UPDATE for idempotency
- Auto table creation with schema mapping
- NULL value filtering

### Phase 2: TDengine Target Adapter
- WriteBatch via REST API
- Super table support for TDengine 2.x
- Auto table creation

## Success Criteria
- [ ] MySQL Target Adapter implemented and tested
- [ ] TDengine Target Adapter implemented and tested
- [ ] Configuration validation updated for new target types
- [ ] Unit tests for both adapters
- [ ] Documentation updated

## Dependencies
- `github.com/go-sql-driver/mysql` - MySQL driver
- TDengine REST API - Already used for source, no new dependency

## Risks
| Risk | Mitigation |
|------|------------|
| MySQL connection pooling | Use standard database/sql pool |
| TDengine REST performance | Batch writes to reduce HTTP overhead |
| Schema mismatch | Require explicit schema mapping in config |
