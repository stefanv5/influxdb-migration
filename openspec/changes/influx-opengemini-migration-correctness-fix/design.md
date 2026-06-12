# Design: Influx/OpenGemini Migration Correctness Fix

## Overview

Fix the reviewed Influx/OpenGemini migration defects in two implementation slices:

1. Adapter slice: Influx source/target adapters and query config.
2. Engine slice: migration engine, shard/window checkpoints, and checkpoint schema semantics.

## Adapter Design

- Add optional `StartTime` and `EndTime` fields to `types.QueryConfig`.
- In Influx V1/V2 `QueryData`, use `QueryConfig.StartTime/EndTime` when present; otherwise preserve current checkpoint-to-now behavior.
- Set InfluxDB v2 write URL query parameters before constructing the HTTP request.
- Parse per-statement V1 response errors via `results[].error`.
- Build V1 compatibility query params without empty credentials, and keep existing username/password support.
- Escape line protocol measurement names.
- Preserve zero timestamps by writing explicit `0` rather than omitting the timestamp.
- Parse series keys with escaped commas, equals, and backslashes.
- Make no-tag series conditions explicit enough to avoid exclusion when mixed with tagged series.

## Engine/Checkpoint Design

- Mark task-level checkpoints in progress at the start of batch and shard-group modes.
- Do not overwrite completed window checkpoint details with stale outer shard checkpoint objects.
- Include `window_end` in the shard checkpoint uniqueness conflict target and update it on upsert.
- Use `shard_group_config.series_batch_size` for shard-group window batching when configured.
- Use half-open interval overlap checks: `[start, end)`.
- In `queryWithTimeRange`, pass per-window query bounds through `QueryConfig`.

## Verification

- Unit tests must cover v2 write URL parameters, line protocol escaping, zero timestamp preservation, per-statement V1 errors, series key parsing, no-tag series handling, time-range query bounds, shard checkpoint overwrite prevention, shard checkpoint uniqueness, shard batch size configuration, and half-open overlap.
- Run focused packages first, then broad `go test ./...`.
