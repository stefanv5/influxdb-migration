package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/internal/adapter"
	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/pkg/types"
)

type executionSourceAdapter struct {
	queryErr            error
	panicInQuery        bool
	afterQueryDataBatch func()
	tagKeysErr          error
	tagKeys             []string

	// batchMaxTimestamps, when set, fixes the max (last) record timestamp for
	// each batch call so tests can simulate non-monotonic per-batch time
	// ranges. Index aligns with the order of batch calls. When nil, the
	// adapter derives timestamps from the series index as before.
	batchMaxTimestamps []int64

	mu         sync.Mutex
	series     []string
	batchCalls [][]string
}

func (m *executionSourceAdapter) Name() string                { return "execution-source" }
func (m *executionSourceAdapter) SupportedVersions() []string { return []string{"test"} }
func (m *executionSourceAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	return nil
}
func (m *executionSourceAdapter) Disconnect(ctx context.Context) error { return nil }
func (m *executionSourceAdapter) Ping(ctx context.Context) error       { return nil }

func (m *executionSourceAdapter) DiscoverTables(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (m *executionSourceAdapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
	if m.series != nil {
		return append([]string(nil), m.series...), nil
	}
	return []string{measurement}, nil
}

func (m *executionSourceAdapter) DiscoverSeriesInTimeWindow(ctx context.Context, measurement string, start, end time.Time) ([]string, error) {
	return []string{measurement}, nil
}

func (m *executionSourceAdapter) DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error) {
	if m.tagKeysErr != nil {
		return nil, m.tagKeysErr
	}
	if m.tagKeys != nil {
		return append([]string(nil), m.tagKeys...), nil
	}
	return nil, nil
}

func (m *executionSourceAdapter) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
	return &types.TableSchema{TableName: table}, nil
}

func (m *executionSourceAdapter) DiscoverShardGroups(ctx context.Context) ([]*adapter.ShardGroup, error) {
	return nil, nil
}

func (m *executionSourceAdapter) QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	if m.panicInQuery {
		panic("query panic")
	}
	if m.queryErr != nil {
		return nil, m.queryErr
	}

	record := types.NewRecord()
	record.Time = time.Unix(1, 0).UnixNano()
	record.AddTag("host", "server-a")
	record.AddField("usage", 1.0)
	if err := batchFunc([]types.Record{*record}); err != nil {
		return nil, err
	}

	return &types.Checkpoint{
		ProcessedRows: 1,
		LastTimestamp: record.Time,
	}, nil
}

func (m *executionSourceAdapter) QueryDataBatch(ctx context.Context, measurement string, series []string, startTime, endTime time.Time, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	m.mu.Lock()
	m.batchCalls = append(m.batchCalls, append([]string(nil), series...))
	m.mu.Unlock()

	if m.queryErr != nil {
		return nil, m.queryErr
	}

	records := make([]types.Record, 0, len(series))
	m.mu.Lock()
	batchIdx := len(m.batchCalls) - 1
	m.mu.Unlock()
	for i, serie := range series {
		record := types.NewRecord()
		if len(m.batchMaxTimestamps) > batchIdx && m.batchMaxTimestamps[batchIdx] != 0 {
			// Lay out records so the last one in the batch carries the
			// configured max timestamp; earlier records step back by 1ns each.
			offset := time.Duration(len(series)-1-i) * time.Nanosecond
			record.Time = time.Unix(0, m.batchMaxTimestamps[batchIdx]).Add(-offset).UnixNano()
		} else {
			record.Time = endTime.Add(time.Duration(i) * time.Nanosecond).UnixNano()
		}
		record.AddTag("series", serie)
		record.AddField("usage", float64(i+1))
		records = append(records, *record)
	}
	if err := batchFunc(records); err != nil {
		return nil, err
	}
	if m.afterQueryDataBatch != nil {
		m.afterQueryDataBatch()
	}

	lastTimestamp := int64(0)
	if len(records) > 0 {
		lastTimestamp = records[len(records)-1].Time
	}
	return &types.Checkpoint{
		ProcessedRows: int64(len(records)),
		LastTimestamp: lastTimestamp,
	}, nil
}

type executionTargetAdapter struct {
	writeErr error

	mu      sync.Mutex
	written int
}

func (m *executionTargetAdapter) Name() string                { return "execution-target" }
func (m *executionTargetAdapter) SupportedVersions() []string { return []string{"test"} }
func (m *executionTargetAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	return nil
}
func (m *executionTargetAdapter) Disconnect(ctx context.Context) error { return nil }
func (m *executionTargetAdapter) Ping(ctx context.Context) error       { return nil }

func (m *executionTargetAdapter) WriteBatch(ctx context.Context, measurement string, records []types.Record) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.written += len(records)
	return nil
}

func (m *executionTargetAdapter) MeasurementExists(ctx context.Context, name string) (bool, error) {
	return true, nil
}

func (m *executionTargetAdapter) CreateMeasurement(ctx context.Context, schema *types.Schema) error {
	return nil
}

func newExecutionTestEngine(t *testing.T, source *executionSourceAdapter, target *executionTargetAdapter, mappings []types.MappingConfig) (*MigrationEngine, *checkpoint.Manager) {
	t.Helper()

	cpMgr, err := checkpoint.NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %v", err)
	}
	t.Cleanup(func() {
		_ = cpMgr.Close()
	})

	cfg := &types.MigrationConfig{
		Sources: []types.SourceConfig{{
			Name: "src",
			Type: "execution-source",
		}},
		Targets: []types.TargetConfig{{
			Name: "tgt",
			Type: "execution-target",
		}},
		Tasks: []types.TaskConfig{{
			Name:     "execution-task",
			Source:   "src",
			Target:   "tgt",
			Mappings: mappings,
		}},
		Migration: types.MigrationSettings{
			ParallelTasks: 1,
			ChunkSize:     10,
			ChunkInterval: time.Nanosecond,
		},
		Retry: types.RetryConfig{
			MaxAttempts: 1,
		},
	}

	engine := NewMigrationEngine(cfg, cpMgr)
	engine.sourceRegistry = adapter.NewRegistry()
	engine.targetRegistry = adapter.NewRegistry()
	engine.sourceRegistry.RegisterSource("execution-source", func() adapter.SourceAdapter {
		return source
	})
	engine.targetRegistry.RegisterTarget("execution-target", func() adapter.TargetAdapter {
		return target
	})

	return engine, cpMgr
}

func executionMappings(count int) []types.MappingConfig {
	mappings := make([]types.MappingConfig, 0, count)
	for i := 0; i < count; i++ {
		mappings = append(mappings, types.MappingConfig{
			SourceTable:       fmt.Sprintf("m%d", i),
			TargetMeasurement: fmt.Sprintf("tm%d", i),
		})
	}
	return mappings
}

func TestRunDoesNotDeadlockWhenTaskCountExceedsQueueCapacity(t *testing.T) {
	engine, _ := newExecutionTestEngine(t, &executionSourceAdapter{}, &executionTargetAdapter{}, executionMappings(5))

	done := make(chan error, 1)
	go func() {
		done <- engine.Run(context.Background())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run deadlocked while enqueueing tasks before workers could drain the queue")
	}
}

func TestRunReturnsWorkerTaskErrors(t *testing.T) {
	sourceErr := errors.New("source query failed")
	engine, _ := newExecutionTestEngine(t, &executionSourceAdapter{queryErr: sourceErr}, &executionTargetAdapter{}, executionMappings(1))

	err := engine.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to return worker error")
	}
	if !strings.Contains(err.Error(), sourceErr.Error()) {
		t.Fatalf("expected worker error to include source error, got %v", err)
	}
}

func TestRunReturnsWorkerPanicsAsErrors(t *testing.T) {
	engine, _ := newExecutionTestEngine(t, &executionSourceAdapter{panicInQuery: true}, &executionTargetAdapter{}, executionMappings(1))

	err := engine.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to return worker panic as an error")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected panic error, got %v", err)
	}
}

func TestRunDoesNotAdvanceCheckpointWhenTargetWriteFails(t *testing.T) {
	writeErr := errors.New("target write failed")
	engine, cpMgr := newExecutionTestEngine(t, &executionSourceAdapter{}, &executionTargetAdapter{writeErr: writeErr}, executionMappings(1))

	err := engine.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to return target write error")
	}

	cp, loadErr := cpMgr.LoadCheckpoint(context.Background(), "execution-task-tm0", "m0")
	if loadErr != nil {
		t.Fatalf("failed to load checkpoint: %v", loadErr)
	}
	if cp == nil {
		t.Fatal("expected checkpoint to exist")
	}
	if cp.LastTimestamp != 0 {
		t.Fatalf("checkpoint advanced to %d after failed target write", cp.LastTimestamp)
	}
	if cp.ProcessedRows != 0 {
		t.Fatalf("checkpoint processed rows advanced to %d after failed target write", cp.ProcessedRows)
	}
	if cp.Status != types.StatusFailed {
		t.Fatalf("expected failed checkpoint status, got %s", cp.Status)
	}
}

func TestResumeDoesNotDeadlockWhenFailedTaskCountExceedsQueueCapacity(t *testing.T) {
	engine, cpMgr := newExecutionTestEngine(t, &executionSourceAdapter{}, &executionTargetAdapter{}, executionMappings(1))
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		cp := &types.Checkpoint{
			TaskID:      fmt.Sprintf("resume-%d", i),
			TaskName:    "execution-task",
			SourceTable: fmt.Sprintf("m%d", i),
			TargetMeas:  fmt.Sprintf("tm%d", i),
			Status:      types.StatusFailed,
			MappingConfig: types.MappingConfig{
				SourceTable:       fmt.Sprintf("m%d", i),
				TargetMeasurement: fmt.Sprintf("tm%d", i),
			},
		}
		if err := cpMgr.SaveCheckpoint(ctx, cp); err != nil {
			t.Fatalf("failed to seed checkpoint: %v", err)
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- engine.Resume(ctx)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Resume returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Resume deadlocked while enqueueing failed tasks before workers could drain the queue")
	}
}

func TestBatchModeResumeSkipsCompletedSeriesBatchInsideWindow(t *testing.T) {
	source := &executionSourceAdapter{
		series: []string{"cpu,host=a", "cpu,host=b", "cpu,host=c", "cpu,host=d", "cpu,host=e"},
	}
	target := &executionTargetAdapter{}
	mapping := types.MappingConfig{
		SourceTable:       "cpu",
		TargetMeasurement: "cpu",
		TimeRange: types.TimeRange{
			Start: "2024-01-01T00:00:00Z",
			End:   "2024-01-02T00:00:00Z",
		},
		TimeWindow: "24h",
	}
	engine, cpMgr := newExecutionTestEngine(t, source, target, []types.MappingConfig{mapping})
	engine.config.InfluxToInflux = types.InfluxToInfluxConfig{
		Enabled:           true,
		QueryMode:         "batch",
		MaxSeriesPerQuery: 2,
	}

	ctx := context.Background()
	cp := &types.Checkpoint{
		TaskID:            "batch-task",
		TaskName:          "execution-task",
		SourceTable:       "cpu",
		TargetMeas:        "cpu",
		LastID:            0,
		LastTimestamp:     time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC).UnixNano(),
		ProcessedRows:     0,
		TotalMigratedRows: 2,
		Status:            types.StatusInProgress,
		MappingConfig:     mapping,
	}
	if err := cpMgr.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatalf("failed to seed checkpoint: %v", err)
	}

	task := &MigrationTask{
		ID:            "batch-task",
		SourceAdapter: "src",
		TargetAdapter: "tgt",
		Mapping:       &mapping,
		Status:        types.StatusPending,
	}
	if err := engine.runTaskBatchMode(ctx, task); err != nil {
		t.Fatalf("batch mode failed: %v", err)
	}

	source.mu.Lock()
	defer source.mu.Unlock()
	if len(source.batchCalls) != 2 {
		t.Fatalf("expected 2 remaining batch calls, got %d: %#v", len(source.batchCalls), source.batchCalls)
	}
	expected := [][]string{
		{"cpu,host=c", "cpu,host=d"},
		{"cpu,host=e"},
	}
	for i := range expected {
		if !reflect.DeepEqual(source.batchCalls[i], expected[i]) {
			t.Fatalf("batch call %d = %#v, want %#v", i, source.batchCalls[i], expected[i])
		}
	}

	finalCP, err := cpMgr.LoadCheckpoint(ctx, "batch-task", "cpu")
	if err != nil {
		t.Fatalf("failed to load final checkpoint: %v", err)
	}
	if finalCP == nil {
		t.Fatal("expected final checkpoint")
	}
	if finalCP.Status != types.StatusCompleted {
		t.Fatalf("expected completed checkpoint, got %s", finalCP.Status)
	}
	if finalCP.ProcessedRows != 1 {
		t.Fatalf("expected one completed window, got %d", finalCP.ProcessedRows)
	}
}

func TestShardGroupCheckpointSaveFailureHonorsFailOnCheckpoint(t *testing.T) {
	source := &executionSourceAdapter{
		series: []string{"cpu,host=a"},
	}
	target := &executionTargetAdapter{}
	mapping := types.MappingConfig{
		SourceTable:       "cpu",
		TargetMeasurement: "cpu",
	}
	engine, cpMgr := newExecutionTestEngine(t, source, target, []types.MappingConfig{mapping})
	engine.config.Migration.FailOnCheckpointError = true
	source.afterQueryDataBatch = func() {
		_ = cpMgr.Close()
	}

	task := &MigrationTask{
		ID:            "shard-task",
		SourceAdapter: "src",
		TargetAdapter: "tgt",
		Mapping:       &mapping,
		Status:        types.StatusPending,
	}
	sg := &adapter.ShardGroup{
		ID:        1,
		StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	window := TimeWindow{
		Start: sg.StartTime,
		End:   sg.EndTime,
	}

	err := engine.migrateTimeWindow(context.Background(), task, sg, window, source, target, nil)
	if err == nil {
		t.Fatal("expected shard group checkpoint save failure")
	}
	if !strings.Contains(err.Error(), "failed to save shard group checkpoint") {
		t.Fatalf("expected shard checkpoint save error, got %v", err)
	}
}

func TestShardGroupMigrationStopsWhenTagDiscoveryFails(t *testing.T) {
	tagErr := errors.New("tag query failed")
	source := &executionSourceAdapter{
		series:     []string{"cpu,host=a"},
		tagKeysErr: tagErr,
	}
	target := &executionTargetAdapter{}
	mapping := types.MappingConfig{
		SourceTable:       "cpu",
		TargetMeasurement: "cpu",
	}
	engine, _ := newExecutionTestEngine(t, source, target, []types.MappingConfig{mapping})

	task := &MigrationTask{
		ID:            "shard-tag-task",
		SourceAdapter: "src",
		TargetAdapter: "tgt",
		Mapping:       &mapping,
		Status:        types.StatusPending,
	}
	sg := &adapter.ShardGroup{
		ID:        1,
		StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}

	err := engine.migrateShardGroup(context.Background(), task, sg, source, target, sg.StartTime, sg.EndTime)
	if err == nil {
		t.Fatal("expected tag discovery error")
	}
	if !strings.Contains(err.Error(), tagErr.Error()) {
		t.Fatalf("expected tag discovery error, got %v", err)
	}
}

// TestWindowCompletionCheckpointSaveHonorsFailOnCheckpoint verifies that the
// window-completion shard-group checkpoint save (the one that marks the window
// StatusCompleted) honors FailOnCheckpointError. When false, a save failure
// must NOT abort migrateTimeWindow: the window's data is already written, so a
// stale checkpoint merely means re-migration on resume (safe duplication under
// InfluxDB overwrite semantics). This mirrors the per-batch checkpoint save
// behavior and closes the inconsistency where the window-completion save was
// always fatal.
func TestWindowCompletionCheckpointSaveHonorsFailOnCheckpoint(t *testing.T) {
	source := &executionSourceAdapter{
		series: []string{"cpu,host=a"},
	}
	target := &executionTargetAdapter{}
	mapping := types.MappingConfig{
		SourceTable:       "cpu",
		TargetMeasurement: "cpu",
	}
	engine, cpMgr := newExecutionTestEngine(t, source, target, []types.MappingConfig{mapping})
	// FailOnCheckpointError=false: checkpoint save failures must be logged and
	// tolerated, not returned as fatal errors.
	engine.config.Migration.FailOnCheckpointError = false
	// Close the checkpoint manager after the source query completes so that
	// BOTH the per-batch and window-completion SaveShardGroupCheckpoint calls
	// fail. With FailOnCheckpointError=false, migrateTimeWindow must still
	// return nil — proving the window-completion site no longer aborts.
	source.afterQueryDataBatch = func() {
		_ = cpMgr.Close()
	}

	task := &MigrationTask{
		ID:            "window-completion-task",
		SourceAdapter: "src",
		TargetAdapter: "tgt",
		Mapping:       &mapping,
		Status:        types.StatusPending,
	}
	sg := &adapter.ShardGroup{
		ID:        1,
		StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	window := TimeWindow{
		Start: sg.StartTime,
		End:   sg.EndTime,
	}

	err := engine.migrateTimeWindow(context.Background(), task, sg, window, source, target, nil)
	if err != nil {
		t.Fatalf("expected migrateTimeWindow to tolerate window-completion checkpoint save failure when FailOnCheckpointError=false, got: %v", err)
	}

	// The target must have received the data regardless of the checkpoint
	// failure — the write happens before the checkpoint save.
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.written == 0 {
		t.Fatal("expected target to have received records despite checkpoint save failure")
	}
}

// TestBatchModeWatermarkDoesNotRegressAcrossBatches verifies that the
// window-wide max timestamp high-water mark is preserved across batches whose
// own max timestamps are non-monotonic. Because different series batches cover
// different time ranges, a later batch can report a lower max timestamp than an
// earlier one. The final reported watermark must be the true window-wide max
// (batch 1's max), not the regressed value from batch 2.
func TestBatchModeWatermarkDoesNotRegressAcrossBatches(t *testing.T) {
	source := &executionSourceAdapter{
		series: []string{"cpu,host=a", "cpu,host=b", "cpu,host=c", "cpu,host=d"},
		// Two batches of 2 series each. Batch 1 max TS = 200, batch 2 max TS =
		// 100 — deliberately non-monotonic so an overwrite would regress the
		// watermark from 200 down to 100.
		batchMaxTimestamps: []int64{200, 100},
	}
	target := &executionTargetAdapter{}
	mapping := types.MappingConfig{
		SourceTable:       "cpu",
		TargetMeasurement: "cpu",
		TimeRange: types.TimeRange{
			Start: "2024-01-01T00:00:00Z",
			End:   "2024-01-02T00:00:00Z",
		},
		TimeWindow: "24h",
	}
	engine, cpMgr := newExecutionTestEngine(t, source, target, []types.MappingConfig{mapping})
	engine.config.InfluxToInflux = types.InfluxToInfluxConfig{
		Enabled:           true,
		QueryMode:         "batch",
		MaxSeriesPerQuery: 2,
	}

	task := &MigrationTask{
		ID:            "watermark-task",
		SourceAdapter: "src",
		TargetAdapter: "tgt",
		Mapping:       &mapping,
		Status:        types.StatusPending,
	}
	if err := engine.runTaskBatchMode(context.Background(), task); err != nil {
		t.Fatalf("batch mode failed: %v", err)
	}

	finalCP, err := cpMgr.LoadCheckpoint(context.Background(), "watermark-task", "cpu")
	if err != nil {
		t.Fatalf("failed to load final checkpoint: %v", err)
	}
	if finalCP == nil {
		t.Fatal("expected final checkpoint")
	}
	if finalCP.LastTimestamp != 200 {
		t.Fatalf("expected window-wide max timestamp 200 (batch 1), got %d — watermark regressed to a later batch's lower max", finalCP.LastTimestamp)
	}
}
