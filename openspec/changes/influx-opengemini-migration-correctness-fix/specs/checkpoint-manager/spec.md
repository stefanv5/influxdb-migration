# Checkpoint Manager Spec Delta

## Requirements

### Shard Window Checkpoints

Shard-group window checkpoints SHALL be unique by `task_id`, `shard_group_id`, `window_start`, and `window_end`.

Saving an outer shard checkpoint SHALL NOT overwrite the accurate `last_completed_batch`, `last_timestamp`, `total_processed_rows`, or completed status saved for a window.
