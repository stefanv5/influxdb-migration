# InfluxDB Migration Tool

A high-performance, multi-source migration tool for transferring data between InfluxDB, MySQL, and TDengine databases.

## Features

- **Multi-Source Support**: Migrate data from MySQL, TDengine, or InfluxDB (v1/v2)
- **Multi-Target Support**: Write to InfluxDB (v1/v2), MySQL, or TDengine
- **Bidirectional Migration**: Flexible source-target combinations for disaster recovery scenarios
- **Checkpoint System**: Resume interrupted migrations from the last checkpoint
- **Rate Limiting**: Protect source databases from overwhelming queries
- **Parallel Processing**: Configurable parallel tasks for optimal performance
- **Comprehensive Logging**: Structured logging with rotation support
- **Retry Mechanism**: Automatic retry with exponential backoff

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   MySQL     │────▶│   Engine    │────▶│  InfluxDB   │
│   Source    │     │             │     │   Target    │
└─────────────┘     │  - Transform│     │   (v1/v2)   │
                    │  - RateLimit│     └─────────────┘
┌─────────────┐     │  - Chunking │     ┌─────────────┐
│  TDengine   │────▶│             │────▶│    MySQL    │
│   Source    │     │             │     │   Target    │
└─────────────┘     └─────────────┘     └─────────────┘
                    ┌─────────────┐     ┌─────────────┐
┌─────────────┐     │  Checkpoint │     │  TDengine   │
│  InfluxDB   │────▶│   Manager   │────▶│   Target    │
│   Source    │     │  (SQLite)   │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
```

## Quick Start

### 1. Build the Tool

```bash
go build -o migrate ./cmd/migrate
```

### 2. Configure Migration

Edit `config.yaml`:

```yaml
global:
  name: "migration-task-001"
  checkpoint_dir: "./checkpoints"
  report_dir: "./reports"

sources:
  influxdb:
    - name: "source-influx"
      type: "influxdb"
      version: 2
      url: "http://localhost:8086"
      token: "${INFLUX_TOKEN}"
      org: "my-org"
      bucket: "metrics"

targets:
  mysql:
    - name: "target-mysql"
      type: "mysql"
      mysql:
        host: "localhost"
        port: 3306
        username: "root"
        password: "${MYSQL_PASSWORD}"
        database: "metrics"

tasks:
  - name: "influx-to-mysql"
    source: "source-influx"
    target: "target-mysql"
    mappings:
      - source_table: "cpu_metrics"
        target_measurement: "cpu"
        fields:
          - source_name: "usage"
            target_name: "cpu_usage"
            data_type: "float"
```

### 3. Run Migration

```bash
./migrate --config config.yaml
```

## Configuration

### Global Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | string | - | Migration task name |
| `checkpoint_dir` | string | `./checkpoints` | Directory for checkpoint files |
| `report_dir` | string | `./reports` | Directory for report output |

### Rate Limiting

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `true` | Enable rate limiting |
| `points_per_second` | int | `100000` | Token bucket points per second |
| `burst_size` | int | `50000` | Maximum burst size |

### Migration Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `parallel_tasks` | int | `4` | Number of parallel migration tasks |
| `chunk_size` | int | `10000` | Records per chunk |
| `chunk_interval` | duration | `100ms` | Interval between chunks |

## Source Adapters

### MySQL

```yaml
sources:
  mysql:
    - name: "prod-mysql"
      type: "mysql"
      host: "localhost"
      port: 3306
      user: "root"
      password: "${MYSQL_PASSWORD}"
      database: "metrics"
      charset: "utf8mb4"
      ssl:
        enabled: false
        skip_verify: false
```

### InfluxDB v1

```yaml
sources:
  influxdb:
    - name: "prod-influx"
      type: "influxdb"
      version: 1
      url: "http://localhost:8086"
      username: "admin"
      password: "${INFLUX_PASSWORD}"
      database: "metrics"
```

### InfluxDB v2

```yaml
sources:
  influxdb:
    - name: "prod-influx"
      type: "influxdb"
      version: 2
      url: "http://localhost:8086"
      token: "${INFLUX_TOKEN}"
      org: "my-org"
      bucket: "metrics"
```

### TDengine

```yaml
sources:
  tdengine:
    - name: "prod-tdengine"
      type: "tdengine"
      version: "3.x"
      host: "localhost"
      port: 6041
      user: "root"
      password: "${TD_PASSWORD}"
      database: "metrics"
```

## Target Adapters

### MySQL Target

```yaml
targets:
  mysql:
    - name: "target-mysql"
      type: "mysql"
      mysql:
        host: "localhost"
        port: 3306
        username: "root"
        password: "${MYSQL_PASSWORD}"
        database: "metrics"
```

Features:
- `INSERT ... ON DUPLICATE KEY UPDATE` for idempotency
- Auto-creates tables based on schema
- Batch inserts for performance

### TDengine Target

```yaml
targets:
  tdengine:
    - name: "target-tdengine"
      type: "tdengine"
      tdengine:
        host: "localhost"
        port: 6030
        username: "root"
        password: "${TD_PASSWORD}"
        database: "metrics"
        version: "3.x"
```

Features:
- Supports both STABLE and regular TABLE creation
- Automatic child table creation for STABLEs
- Batched INSERT for performance

### InfluxDB Target

```yaml
targets:
  influxdb:
    - name: "target-influx"
      type: "influxdb-v2"
      influxdb:
        version: 2
        url: "http://localhost:8087"
        token: "${INFLUX_TOKEN}"
        org: "my-org"
        bucket: "metrics"
```

## Supported Migration Paths

| Source | Target | Status |
|--------|--------|--------|
| MySQL | InfluxDB v1/v2 | ✅ Supported |
| TDengine | InfluxDB v1/v2 | ✅ Supported |
| InfluxDB v1 | MySQL | ✅ Supported |
| InfluxDB v2 | MySQL | ✅ Supported |
| InfluxDB v1 | TDengine | ✅ Supported |
| InfluxDB v2 | TDengine | ✅ Supported |
| MySQL | TDengine | Planned |
| TDengine | MySQL | Planned |

## Checkpoint System

The tool automatically saves checkpoints during migration to enable resume after interruption. Checkpoints are stored in SQLite and track:

- Last processed timestamp
- Total rows processed
- Migration status (completed, in-progress, failed)

## Environment Variables

Use `${VAR_NAME}` syntax in config to reference environment variables:

```yaml
password: "${MYSQL_PASSWORD}"
token: "${INFLUX_TOKEN}"
```

## Logging

Configure logging in `config.yaml`:

```yaml
logging:
  level: "info"        # debug, info, warn, error
  output: "both"      # file, console, both
  file:
    path: "logs/migrate.log"
    max_size: 100     # MB
    max_backups: 10
    max_age: 7        # days
    compress: true
```

## Project Structure

```
.
├── cmd/
│   └── migrate/           # CLI entry point
├── internal/
│   ├── adapter/
│   │   ├── adapter.go     # Adapter interfaces
│   │   ├── registry.go    # Adapter registry
│   │   ├── source/        # Source adapters
│   │   │   ├── influxdb.go
│   │   │   ├── mysql.go
│   │   │   └── tdengine.go
│   │   └── target/        # Target adapters
│   │       ├── influxdb.go
│   │       ├── mysql.go
│   │       └── tdengine.go
│   ├── checkpoint/         # Checkpoint management
│   ├── config/            # Configuration handling
│   ├── engine/             # Migration engine
│   ├── logger/             # Logging utilities
│   └── report/             # Report generation
├── pkg/
│   └── types/              # Shared types
├── openspec/               # OpenSpec design documents
├── config.yaml.example     # Example configuration
├── go.mod
└── migrate                 # Compiled binary
```

## Development

### Run Tests

```bash
go test ./... -v
```

### Build

```bash
go build -o migrate ./cmd/migrate
```

## License

MIT License
