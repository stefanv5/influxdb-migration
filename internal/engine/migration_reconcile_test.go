package engine

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/internal/adapter"
	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/pkg/types"
)

// dropSourceAdapter emits a fixed set of records per batch call: some with
// valid fields and some whose only field is nil, so that FilterNulls produces
// fieldless records that processBatch must drop. This lets the reconciliation
// detect that fewer records were written than the source reported.
type dropSourceAdapter struct {
	mu         sync.Mutex
	batchCalls int
}

func (m *dropSourceAdapter) Name() string                { return "drop-source" }
func (m *dropSourceAdapter) SupportedVersions() []string { return []string{"test"} }
func (m *dropSourceAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	return nil
}
func (m *dropSourceAdapter) Disconnect(ctx context.Context) error { return nil }
func (m *dropSourceAdapter) Ping(ctx context.Context) error       { return nil }

func (m *dropSourceAdapter) DiscoverTables(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (m *dropSourceAdapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
	return []string{measurement}, nil
}

func (m *dropSourceAdapter) DiscoverSeriesInTimeWindow(ctx context.Context, measurement string, start, end time.Time) ([]string, error) {
	return []string{measurement}, nil
}

func (m *dropSourceAdapter) DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error) {
	return nil, nil
}

func (m *dropSourceAdapter) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
	return &types.TableSchema{TableName: table}, nil
}

func (m *dropSourceAdapter) DiscoverShardGroups(ctx context.Context) ([]*adapter.ShardGroup, error) {
	return nil, nil
}

func (m *dropSourceAdapter) QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	records := buildMixedRecords()
	if err := batchFunc(records); err != nil {
		return nil, err
	}
	return &types.Checkpoint{
		ProcessedRows: int64(len(records)),
		LastTimestamp:  records[0].Time,
	}, nil
}

func (m *dropSourceAdapter) QueryDataBatch(ctx context.Context, measurement string, series []string, startTime, endTime time.Time, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	m.mu.Lock()
	m.batchCalls++
	m.mu.Unlock()

	records := buildMixedRecords()
	if err := batchFunc(records); err != nil {
		return nil, err
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

// buildMixedRecords returns 3 records: 2 valid (writable) and 1 whose only
// field is nil. After FilterNulls the nil-field record has zero fields and
// must be dropped by processBatch, so source-read=3 but written-to-target=2.
func buildMixedRecords() []types.Record {
	base := endTimeForRecords().UnixNano()
	r1 := types.NewRecord()
	r1.Time = base
	r1.AddTag("host", "a")
	r1.AddField("usage", 1.0)

	r2 := types.NewRecord()
	r2.Time = base + 1
	r2.AddTag("host", "b")
	r2.AddField("usage", 2.0)

	// r3 has only a nil field: after FilterNulls it is fieldless and unwritable.
	r3 := types.NewRecord()
	r3.Time = base + 2
	r3.AddTag("host", "c")
	r3.AddField("usage", nil)

	return []types.Record{*r1, *r2, *r3}
}

func endTimeForRecords() time.Time {
	return time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
}

// TestReconcileRowCounts_BuildsDeltaMessage verifies the pure reconciliation
// helper produces an accurate warning message when source-read and
// written-to-target counts diverge, and returns an empty message when they
// match.
func TestReconcileRowCounts_BuildsDeltaMessage(t *testing.T) {
	cases := []struct {
		name        string
		sourceRows  int64
		writtenRows int64
		wantMsg     string
		wantEmpty   bool
	}{
		{
			name:        "match produces no warning",
			sourceRows:  10,
			writtenRows: 10,
			wantEmpty:   true,
		},
		{
			name:        "drop produces delta warning",
			sourceRows:  3,
			writtenRows: 2,
			wantMsg:     "row count mismatch: source=3 target=2, delta=1",
		},
		{
			name:        "large drop",
			sourceRows:  100,
			writtenRows: 80,
			wantMsg:     "row count mismatch: source=100 target=80, delta=20",
		},
		{
			name:        "zero source produces no warning",
			sourceRows:  0,
			writtenRows: 0,
			wantEmpty:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := reconcileRowCounts(tc.sourceRows, tc.writtenRows)
			if tc.wantEmpty {
				if got != "" {
					t.Fatalf("expected empty message for matching counts, got %q", got)
				}
				return
			}
			if !strings.Contains(got, tc.wantMsg) {
				t.Fatalf("expected message to contain %q, got %q", tc.wantMsg, got)
			}
		})
	}
}

// TestBatchModeReconcilesRowsWhenTransformDropsRecords runs a batch-mode task
// whose source emits records that become fieldless after FilterNulls and are
// therefore dropped by processBatch. It asserts that:
//   - the target receives fewer records than the source reported (proving the
//     drop is real and the written-row accumulator diverges from the source
//     row count), and
//   - reconcileRowCounts, invoked with the source vs written counts the engine
//     tracks, produces the expected delta warning (proving the reconciliation
//     would fire at completion).
//
// The engine emits the WARN via the package-level logger, which reads a global
// that is not redirectable from this package without modifying the logger
// package (out of scope). So we verify the observable inputs to the
// reconciliation (source rows from the checkpoint, written rows from the
// target) and the reconciliation message directly, rather than capturing the
// log line.
func TestBatchModeReconcilesRowsWhenTransformDropsRecords(t *testing.T) {
	source := &dropSourceAdapter{
		batchCalls: 0,
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
	cpMgr, err := checkpoint.NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %v", err)
	}
	t.Cleanup(func() { _ = cpMgr.Close() })

	cfg := &types.MigrationConfig{
		Sources: []types.SourceConfig{{
			Name: "src",
			Type: "drop-source",
		}},
		Targets: []types.TargetConfig{{
			Name: "tgt",
			Type: "execution-target",
		}},
		Tasks: []types.TaskConfig{{
			Name:     "execution-task",
			Source:   "src",
			Target:   "tgt",
			Mappings: []types.MappingConfig{mapping},
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
	engine.sourceRegistry.RegisterSource("drop-source", func() adapter.SourceAdapter {
		return source
	})
	engine.targetRegistry.RegisterTarget("execution-target", func() adapter.TargetAdapter {
		return target
	})
	engine.config.InfluxToInflux = types.InfluxToInfluxConfig{
		Enabled:           true,
		QueryMode:         "batch",
		MaxSeriesPerQuery: 100,
	}

	task := &MigrationTask{
		ID:            "reconcile-task",
		SourceAdapter: "src",
		TargetAdapter: "tgt",
		Mapping:       &mapping,
		Status:        types.StatusPending,
	}
	if err := engine.runTaskBatchMode(context.Background(), task); err != nil {
		t.Fatalf("batch mode failed: %v", err)
	}

	// The source emitted 3 records; 1 became fieldless after FilterNulls and
	// was dropped. The target must have received exactly 2 — this is the
	// written-row count the engine accumulates for reconciliation.
	target.mu.Lock()
	targetWritten := target.written
	target.mu.Unlock()
	if targetWritten != 2 {
		t.Fatalf("expected target to receive 2 written records (3 source - 1 dropped), got %d", targetWritten)
	}

	// The final checkpoint records the source-reported row count (3). This is
	// the source-rows input to reconciliation.
	finalCP, loadErr := cpMgr.LoadCheckpoint(context.Background(), "reconcile-task", "cpu")
	if loadErr != nil || finalCP == nil {
		t.Fatalf("failed to load final checkpoint: %v", loadErr)
	}
	if finalCP.TotalMigratedRows != 3 {
		t.Fatalf("expected checkpoint source-reported TotalMigratedRows=3, got %d", finalCP.TotalMigratedRows)
	}

	// Reconstruct the reconciliation the engine performs at completion:
	// source rows (from checkpoint) vs written rows (from target). The delta
	// message must fire with source=3 target=2 delta=1.
	msg := reconcileRowCounts(finalCP.TotalMigratedRows, int64(targetWritten))
	if msg == "" {
		t.Fatal("expected reconciliation warning for source=3 vs written=2, got empty message")
	}
	if !strings.Contains(msg, "source=3") || !strings.Contains(msg, "target=2") || !strings.Contains(msg, "delta=1") {
		t.Fatalf("reconciliation message missing expected counts/delta, got: %s", msg)
	}
}
