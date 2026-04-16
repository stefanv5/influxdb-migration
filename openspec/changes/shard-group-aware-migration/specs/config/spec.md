# Config Spec: Shard Group-Aware Migration

## ADDED Requirements

### Requirement: InfluxMigrationConfig Extension

The configuration SHALL support a new `shard_group_config` section within `influx_to_influx`.

#### Scenario: Default configuration
- **WHEN** `influx_to_influx.shard_group_config` is not specified
- **THEN** the default values SHALL be used: `enabled=false`, `series_batch_size=50`, `shard_parallelism=1`

#### Scenario: Custom configuration
- **WHEN** `influx_to_influx.shard_group_config.series_batch_size` is set to 100
- **THEN** each series batch within a shard group SHALL contain at most 100 series

#### Scenario: Shard parallelism configuration
- **WHEN** `influx_to_influx.shard_group_config.shard_parallelism` is set to 4
- **THEN** up to 4 shard groups SHALL be processed in parallel
- **AND** within each shard group, processing SHALL remain sequential

### Requirement: Query Mode Validation

The configuration validator SHALL enforce valid query mode values.

#### Scenario: Valid query modes
- **WHEN** `query_mode` is set to `"single"`, `"batch"`, or `"shard-group"`
- **THEN** validation SHALL pass

#### Scenario: Invalid query mode
- **WHEN** `query_mode` is set to an unsupported value like `"parallel"`
- **THEN** validation SHALL fail with an error message listing valid modes

### Requirement: Shard Group Config Validation

The configuration validator SHALL enforce constraints on shard group settings.

#### Scenario: Series batch size bounds
- **WHEN** `series_batch_size` is less than 1
- **THEN** validation SHALL fail with an error
- **WHEN** `series_batch_size` is greater than 1000
- **THEN** validation SHALL cap the value at 1000

#### Scenario: Shard parallelism bounds
- **WHEN** `shard_parallelism` is less than 1
- **THEN** validation SHALL set it to 1 (minimum)
- **WHEN** `shard_parallelism` is greater than 10
- **THEN** validation SHALL cap the value at 10

### Requirement: Config File Example

The configuration example SHALL include shard-group mode configuration.

#### Scenario: Complete shard-group config
```yaml
influx_to_influx:
  enabled: true
  query_mode: "shard-group"
  max_series_per_query: 100
  shard_group_config:
    enabled: true
    series_batch_size: 50
    shard_parallelism: 1
```

#### Scenario: Shard-group mode without explicit config
```yaml
influx_to_influx:
  enabled: true
  query_mode: "shard-group"
```
This SHALL use default values for `shard_group_config`.
