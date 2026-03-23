## 1. Project Initialization

- [x] 1.1 Initialize Go module (go mod init github.com/migration-tools/influx-migrator)
- [x] 1.2 Create directory structure (cmd/migrate, internal/{adapter,checkpoint,engine,report,config}, pkg/types)
- [x] 1.3 Add dependencies to go.mod (viper, cobra, go-sql-driver/mysql, go-influxdb1-client, go-influxdb2-client, tdengine-go, mattn/go-sqlite3)
- [x] 1.4 Create config.yaml.example with full configuration schema
- [x] 1.5 Create .gitignore

## 2. Core Types (pkg/types)

- [x] 2.1 Define Record struct (Fields, Tags, Time)
- [x] 2.2 Define TableMetadata struct (Name, Schema, Database)
- [x] 2.3 Define Checkpoint struct (TaskID, LastID, LastTimestamp, ProcessedRows, Status)
- [x] 2.4 Define MigrationConfig struct
- [x] 2.5 Define SourceConfig and TargetConfig structs

## 3. Config System (internal/config)

- [x] 3.1 Implement YAML config loading with viper
- [x] 3.2 Implement environment variable substitution (${VAR} syntax)
- [x] 3.3 Implement ConfigValidator with required field checks
- [x] 3.4 Implement SSL configuration validation
- [x] 3.5 Implement task mapping validation
- [x] 3.6 Implement default value injection

## 4. Plugin Registry (internal/adapter)

- [x] 4.1 Define SourceAdapter interface
- [x] 4.2 Define TargetAdapter interface
- [x] 4.3 Implement AdapterRegistry with RegisterSource/RegisterTarget
- [x] 4.4 Implement GetSourceAdapter and GetTargetAdapter methods

## 5. Checkpoint Manager (internal/checkpoint)

- [x] 5.1 Implement SQLite store initialization
- [x] 5.2 Implement SaveCheckpoint method
- [x] 5.3 Implement LoadCheckpoint method
- [x] 5.4 Implement ListCheckpoints method
- [x] 5.5 Implement UpdateTaskStatus method
- [x] 5.6 Implement GetTasksByStatus method

## 6. Rate Limiter (internal/engine)

- [x] 6.1 Implement TokenBucket rate limiter
- [x] 6.2 Implement Allow(points int) method
- [x] 6.3 Implement token refill logic with elapsed time
- [x] 6.4 Add burst size handling

## 7. MySQL Source Adapter (internal/adapter/source)

- [x] 7.1 Implement MySQLAdapter struct with connection pool
- [x] 7.2 Implement Connect/Disconnect/Ping
- [x] 7.3 Implement DiscoverTables (SHOW TABLES)
- [x] 7.4 Implement QueryData with (ts, id) > (last_ts, last_id) pagination
- [x] 7.5 Implement NULL value filtering
- [x] 7.6 Implement SSL/TLS support with skip_verify
- [x] 7.7 Implement Decimal to float conversion

## 8. TDengine Source Adapter (internal/adapter/source)

- [x] 8.1 Implement TDengineAdapter struct
- [x] 8.2 Implement Connect/Disconnect/Ping
- [x] 8.3 Implement DiscoverTables (SHOW TABLES FROM stable)
- [x] 8.4 Implement QueryData with tbname and time range
- [x] 8.5 Add version detection (2.X vs 3.X)
- [x] 8.6 Implement 3.X WebSocket connection support
- [x] 8.7 Implement PARTITION BY TBNAME optimization

## 9. InfluxDB Source Adapter (internal/adapter/source)

- [x] 9.1 Implement InfluxDBV1Adapter struct
- [x] 9.2 Implement Connect/Disconnect with HTTP client
- [x] 9.3 Implement DiscoverSeries (SHOW SERIES)
- [x] 9.4 Implement QueryData with tag filter and time range
- [x] 9.5 Implement chunked response handling
- [x] 9.6 Implement InfluxDBV2Adapter with Flux queries
- [x] 9.7 Implement schema.measurements() for discovery

## 10. InfluxDB Target Adapter (internal/adapter/target)

- [x] 10.1 Implement InfluxDBV1TargetAdapter struct
- [x] 10.2 Implement Connect/Disconnect
- [x] 10.3 Implement WriteBatch with Line Protocol
- [x] 10.4 Implement transaction support (BeginTx/Commit/Rollback)
- [x] 10.5 Implement InfluxDBV2TargetAdapter with API v2
- [x] 10.6 Implement WriteBatch with organization/bucket
- [x] 10.7 Implement SSL/TLS support

## 11. Migration Engine (internal/engine)

- [x] 11.1 Implement MigrationEngine struct with adapters
- [x] 11.2 Implement TaskQueue with channel-based queue
- [x] 11.3 Implement RunTask with chunk processing loop
- [x] 11.4 Implement parallel task execution (parallel_tasks)
- [x] 11.5 Implement chunk-level checkpoint update
- [x] 11.6 Implement retry logic with exponential backoff
- [x] 11.7 Implement rate limiting integration
- [x] 11.8 Implement source_protection (query_interval, max_qps)

## 12. Transform Engine (internal/engine)

- [x] 12.1 Implement Record transformation pipeline
- [x] 12.2 Implement schema mapping (source_field -> target_field)
- [x] 12.3 Implement tag/field classification
- [x] 12.4 Implement NULL value filtering
- [x] 12.5 Implement data type conversion (Decimal -> float)

## 13. Report Generator (internal/report)

- [x] 13.1 Define Report struct with all required fields
- [x] 13.2 Implement ReportGenerator
- [x] 13.3 Implement JSON formatter
- [x] 13.4 Implement HTML formatter with tables and styling
- [x] 13.5 Implement Markdown formatter
- [x] 13.6 Implement SaveReport to file

## 14. CLI Commands (cmd/migrate)

- [x] 14.1 Implement main.go with cobra root command
- [x] 14.2 Implement run command (load config, start migration)
- [x] 14.3 Implement status command (show task progress)
- [x] 14.4 Implement resume command (continue failed tasks)
- [x] 14.5 Implement report command (generate/show reports)
- [x] 14.6 Implement verify command (data integrity check)

## 15. Incremental Sync

- [x] 15.1 Implement incremental sync detection
- [x] 15.2 Implement timestamp-based query filtering
- [x] 15.3 Implement interval_hours scheduling
- [x] 15.4 Implement delta checkpoint update

## 16. Integration Testing

- [ ] 16.1 Write MySQL -> InfluxDB integration test
- [ ] 16.2 Write TDengine -> InfluxDB integration test
- [ ] 16.3 Write InfluxDB -> InfluxDB integration test
- [ ] 16.4 Test checkpoint recovery scenario
- [ ] 16.5 Test rate limiting scenario
- [ ] 16.6 Test SSL/TLS with skip_verify scenario
