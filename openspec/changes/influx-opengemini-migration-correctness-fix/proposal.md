# Proposal: Influx/OpenGemini Migration Correctness Fix

## Problem

The InfluxDB/OpenGemini to InfluxDB/OpenGemini migration path has correctness defects that can cause failed writes, skipped data, duplicate data, incorrect resume behavior, and corrupted line protocol semantics.

## Goals

- Preserve nanosecond timestamps, tag/field identity, measurement names, and series boundaries.
- Make InfluxDB v2 target writes include required `org` and `bucket` parameters.
- Make configured `time_range` bounds effective in single-mode Influx queries.
- Make batch and shard-group resume recoverable after process crashes.
- Keep shard-group window checkpoints accurate and uniquely loadable.
- Make series filtering correct for no-tag series and escaped tag keys/values.
- Add focused tests for the fixed behavior.

## Non-Goals

- Do not redesign the adapter interfaces beyond minimal query configuration needed for time bounds.
- Do not change MySQL or TDengine migration behavior.
- Do not add new migration modes.
- Do not require existing configuration files to change.

## SDD Scope

This change follows the existing Spec/Design/Tasks pattern in `openspec/changes`. The implementation must satisfy the requirements in the `specs/*/spec.md` files before being considered complete.
