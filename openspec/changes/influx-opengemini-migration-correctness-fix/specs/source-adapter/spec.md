# Source Adapter Spec Delta

## Requirements

### Influx Query Time Bounds

Influx source adapters SHALL honor `QueryConfig.StartTime` and `QueryConfig.EndTime` in `QueryData` when they are non-zero.

### V1 API Errors

Influx V1-compatible query handling SHALL detect both top-level response errors and per-statement `results[].error` values.

### Series Filtering

Series key parsing SHALL correctly handle escaped commas, equals signs, and backslashes.

Batch where-clause generation SHALL not drop no-tag series when mixed with tagged series.

### V2 Compatibility Credentials

Influx V2 source V1-compatible query params SHALL not send empty `u`/`p` parameters. Existing username/password compatibility SHALL continue to work.
