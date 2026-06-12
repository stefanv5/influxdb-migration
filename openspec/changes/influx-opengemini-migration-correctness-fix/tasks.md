# Tasks: Influx/OpenGemini Migration Correctness Fix

## OpenSpec

- [x] Create proposal, design, tasks, and spec deltas.

## Implementation

- [x] Adapter slice: fix query config time bounds, Influx source parsing/errors/series filters, Influx target write URL and line protocol formatting.
- [x] Engine slice: fix batch/shard task resume status, shard checkpoint overwrite/uniqueness, shard batch sizing, half-open overlap, and time-range propagation.

## Review

- [x] 1+1 review agent A reviews all fixes for correctness and regressions.
- [x] 1+1 review agent B independently reviews all fixes for correctness and regressions.

## Verification

- [x] Run focused unit tests for adapter/source/target/engine/checkpoint/types.
- [x] Run `go test ./...`.
- [x] Document any remaining failures with exact package and reason.
