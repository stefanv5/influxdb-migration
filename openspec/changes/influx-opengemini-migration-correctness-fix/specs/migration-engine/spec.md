# Migration Engine Spec Delta

## Requirements

### Resume Status

Batch and shard-group modes SHALL mark task-level checkpoints as `in_progress` before writing data.

### Time Range

Single-mode time-range migration SHALL only query records inside each configured window.

### Shard Windows

Shard-group overlap SHALL use half-open interval semantics: `[start, end)`.

Shard-group mode SHALL use `shard_group_config.series_batch_size` when it is configured.
