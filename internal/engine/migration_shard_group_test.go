package engine

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/internal/adapter"
	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/pkg/types"
)

// MockSourceAdapterForShardGroup implements SourceAdapter for shard group testing
type MockSourceAdapterForShardGroup struct {
	shardGroups    []*adapter.ShardGroup
	seriesInWindow map[string][]string // key is "windowStart-windowEnd"
	queryCalled    bool
	queryBatches   [][]string
}

func (m *MockSourceAdapterForShardGroup) Name() string                                    { return "mock" }
func (m *MockSourceAdapterForShardGroup) SupportedVersions() []string                     { return []string{"1.0"} }
func (m *MockSourceAdapterForShardGroup) Connect(ctx context.Context, config map[string]interface{}) error { return nil }
func (m *MockSourceAdapterForShardGroup) Disconnect(ctx context.Context) error             { return nil }
func (m *MockSourceAdapterForShardGroup) Ping(ctx context.Context) error                  { return nil }
func (m *MockSourceAdapterForShardGroup) DiscoverTables(ctx context.Context) ([]string, error) { return nil, nil }
func (m *MockSourceAdapterForShardGroup) DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error) {
	return &types.TableSchema{TableName: table, Columns: []types.Column{}}, nil
}

func (m *MockSourceAdapterForShardGroup) DiscoverShardGroups(ctx context.Context) ([]*adapter.ShardGroup, error) {
	return m.shardGroups, nil
}

func (m *MockSourceAdapterForShardGroup) DiscoverSeriesInTimeWindow(ctx context.Context, measurement string, startTime, endTime time.Time) ([]string, error) {
	key := startTime.Format(time.RFC3339Nano) + "-" + endTime.Format(time.RFC3339Nano)
	series, ok := m.seriesInWindow[key]
	if !ok {
		return []string{}, nil
	}
	return series, nil
}

func (m *MockSourceAdapterForShardGroup) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
	return []string{}, nil
}

func (m *MockSourceAdapterForShardGroup) QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	return nil, nil
}

func (m *MockSourceAdapterForShardGroup) QueryDataBatch(ctx context.Context, measurement string, series []string, startTime, endTime time.Time, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error) {
	m.queryCalled = true
	// Copy the slice to avoid shared underlying array
	m.queryBatches = append(m.queryBatches, append([]string(nil), series...))

	// Calculate offset based on lastCheckpoint for resume simulation
	offset := int64(0)
	if lastCheckpoint != nil {
		offset = lastCheckpoint.ProcessedRows
	}

	// Generate mock records
	records := make([]types.Record, len(series)*10)
	for i := range records {
		records[i] = *types.NewRecord()
		records[i].Time = time.Now().UnixNano()
		records[i].AddField("value", float64(i))
	}

	if batchFunc != nil {
		if err := batchFunc(records); err != nil {
			return nil, err
		}
	}

	return &types.Checkpoint{
		LastTimestamp: time.Now().UnixNano(),
		ProcessedRows: int64(len(records)) + offset,
	}, nil
}

// MockTargetAdapterForShardGroup implements TargetAdapter for testing
type MockTargetAdapterForShardGroup struct {
	receivedRecords []types.Record
}

func (m *MockTargetAdapterForShardGroup) Name() string                          { return "mock-target" }
func (m *MockTargetAdapterForShardGroup) SupportedVersions() []string           { return []string{"1.0"} }
func (m *MockTargetAdapterForShardGroup) Connect(ctx context.Context, config map[string]interface{}) error { return nil }
func (m *MockTargetAdapterForShardGroup) Disconnect(ctx context.Context) error { return nil }
func (m *MockTargetAdapterForShardGroup) Ping(ctx context.Context) error        { return nil }
func (m *MockTargetAdapterForShardGroup) WriteBatch(ctx context.Context, measurement string, records []types.Record) error {
	m.receivedRecords = append(m.receivedRecords, records...)
	return nil
}
func (m *MockTargetAdapterForShardGroup) MeasurementExists(ctx context.Context, name string) (bool, error) { return true, nil }
func (m *MockTargetAdapterForShardGroup) CreateMeasurement(ctx context.Context, schema *types.Schema) error { return nil }

func TestShardGroupMigration(t *testing.T) {
	// Create mock adapters
	sourceAdapter := &MockSourceAdapterForShardGroup{
		shardGroups: []*adapter.ShardGroup{
			{ID: 1, StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), EndTime: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)},
			{ID: 2, StartTime: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC), EndTime: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
			{ID: 3, StartTime: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), EndTime: time.Date(2024, 1, 22, 0, 0, 0, 0, time.UTC)},
		},
		seriesInWindow: map[string][]string{
			"2024-01-01T00:00:00Z-2024-01-08T00:00:00Z": {"series1", "series2"},
			"2024-01-08T00:00:00Z-2024-01-15T00:00:00Z": {"series1", "series2"},
			"2024-01-15T00:00:00Z-2024-01-22T00:00:00Z": {"series1", "series2"},
		},
	}
	targetAdapter := &MockTargetAdapterForShardGroup{}

	// Create checkpoint manager with temp directory
	cpMgr, err := checkpoint.NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %v", err)
	}
	defer cpMgr.Close()

	// Create engine
	engine := &MigrationEngine{
		sourceRegistry: adapter.NewRegistry(),
		targetRegistry: adapter.NewRegistry(),
		checkpointMgr:  cpMgr,
		config: &types.MigrationConfig{
			InfluxToInflux: types.InfluxToInfluxConfig{
				Enabled:           true,
				QueryMode:         "shard-group",
				MaxSeriesPerQuery: 100,
				ShardGroupConfig: &types.ShardGroupConfig{
					Enabled:          true,
					SeriesBatchSize:  50,
					ShardParallelism: 1,
					TimeWindow:       168 * time.Hour,
				},
			},
			Migration: types.MigrationSettings{
				ChunkSize: 10000,
			},
		},
	}

	// Register adapters
	engine.sourceRegistry.RegisterSource("mock", func() adapter.SourceAdapter { return sourceAdapter })
	engine.targetRegistry.RegisterTarget("mock-target", func() adapter.TargetAdapter { return targetAdapter })

	// Create task
	task := &MigrationTask{
		ID:            "test-task-1",
		SourceAdapter: "mock",
		TargetAdapter: "mock-target",
		Mapping: &types.MappingConfig{
			SourceTable:       "test_measurement",
			TargetMeasurement: "test_measurement",
			TimeRange: types.TimeRange{
				Start: "2024-01-01T00:00:00Z",
				End:   "2024-01-22T00:00:00Z",
			},
		},
	}

	// Run migration
	err = engine.runTaskShardGroupMode(context.Background(), task)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	// Verify query was called
	if !sourceAdapter.queryCalled {
		t.Error("expected QueryDataBatch to be called")
	}

	// Verify we processed 3 batches (one per shard group)
	if len(sourceAdapter.queryBatches) != 3 {
		t.Errorf("expected 3 batch queries, got %d", len(sourceAdapter.queryBatches))
	}

	// Verify data was written to target
	if len(targetAdapter.receivedRecords) == 0 {
		t.Error("expected records to be written to target adapter")
	}
}

func TestShardGroupMigrationWithResume(t *testing.T) {
	// Use 10 series per window to create multiple batches when MaxSeriesPerQuery is small
	sourceAdapter := &MockSourceAdapterForShardGroup{
		shardGroups: []*adapter.ShardGroup{
			{ID: 1, StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), EndTime: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)},
			{ID: 2, StartTime: time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC), EndTime: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
		},
		seriesInWindow: map[string][]string{
			"2024-01-01T00:00:00Z-2024-01-08T00:00:00Z": {"series1", "series2", "series3", "series4", "series5"},
			"2024-01-08T00:00:00Z-2024-01-15T00:00:00Z": {"series1", "series2", "series3", "series4", "series5"},
		},
	}
	targetAdapter := &MockTargetAdapterForShardGroup{}

	cpMgr, err := checkpoint.NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %v", err)
	}
	defer cpMgr.Close()

	// Pre-populate checkpoint for first shard group indicating batch 0 is done
	// With 5 series and batch size of 2, we get 3 batches total
	windowStart := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	windowEnd := time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC).UnixNano()
	sgCP := &types.ShardGroupCheckpoint{
		TaskID:              "test-task-resume",
		ShardGroupID:        "1",
		WindowStart:         windowStart,
		WindowEnd:           windowEnd,
		LastCompletedBatch:  0, // First batch (index 0) completed, so start from batch 1
		LastTimestamp:      time.Now().UnixNano(),
		TotalProcessedRows:  10,
		Status:              types.StatusInProgress,
	}
	err = cpMgr.SaveShardGroupCheckpoint(context.Background(), sgCP)
	if err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	engine := &MigrationEngine{
		sourceRegistry: adapter.NewRegistry(),
		targetRegistry: adapter.NewRegistry(),
		checkpointMgr:  cpMgr,
		config: &types.MigrationConfig{
			InfluxToInflux: types.InfluxToInfluxConfig{
				Enabled:           true,
				QueryMode:         "shard-group",
				MaxSeriesPerQuery: 2, // Small batch size to create multiple batches
				ShardGroupConfig: &types.ShardGroupConfig{
					Enabled:          true,
					SeriesBatchSize:  2,
					ShardParallelism: 1,
					TimeWindow:       168 * time.Hour,
				},
			},
			Migration: types.MigrationSettings{
				ChunkSize: 10000,
			},
		},
	}

	engine.sourceRegistry.RegisterSource("mock", func() adapter.SourceAdapter { return sourceAdapter })
	engine.targetRegistry.RegisterTarget("mock-target", func() adapter.TargetAdapter { return targetAdapter })

	task := &MigrationTask{
		ID:            "test-task-resume",
		SourceAdapter: "mock",
		TargetAdapter: "mock-target",
		Mapping: &types.MappingConfig{
			SourceTable:       "test_measurement",
			TargetMeasurement: "test_measurement",
			TimeRange: types.TimeRange{
				Start: "2024-01-01T00:00:00Z",
				End:   "2024-01-15T00:00:00Z",
			},
		},
	}

	err = engine.runTaskShardGroupMode(context.Background(), task)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	// SG 1: 5 series with batch size 2 = 3 batches (0, 1, 2)
	//        Batch 0 done, so skip it, process batches 1 and 2 = 2 queries
	// SG 2: 5 series with batch size 2 = 3 batches (0, 1, 2)
	//        No checkpoint, so process all 3 batches = 3 queries
	// Total: 5 queries
	if len(sourceAdapter.queryBatches) != 5 {
		t.Errorf("expected 5 batch queries after resume, got %d", len(sourceAdapter.queryBatches))
	}

	// Verify batch contents - SG1 batch 0 was skipped, so we should see:
	// SG1: [series3,series4], [series5] (batches 1 and 2)
	// SG2: [series1,series2], [series3,series4], [series5] (all batches)
	expectedBatches := [][]string{
		{"series3", "series4"}, // SG1 batch 1
		{"series5"},            // SG1 batch 2
		{"series1", "series2"}, // SG2 batch 0
		{"series3", "series4"}, // SG2 batch 1
		{"series5"},            // SG2 batch 2
	}
	for i, expected := range expectedBatches {
		if !slices.Equal(sourceAdapter.queryBatches[i], expected) {
			t.Errorf("batch %d: expected %v, got %v", i, expected, sourceAdapter.queryBatches[i])
		}
	}

	// Verify records were written to target
	if len(targetAdapter.receivedRecords) == 0 {
		t.Error("expected records to be written to target adapter")
	}
}

func TestTimeWindowSplitting(t *testing.T) {
	// Test with 7-day windows on a 21-day range
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 22, 0, 0, 0, 0, time.UTC)
	windowDuration := 7 * 24 * time.Hour

	windows := SplitTimeWindows(start, end, windowDuration)

	if len(windows) != 3 {
		t.Errorf("expected 3 windows, got %d", len(windows))
	}

	// Verify window boundaries
	expectedBoundaries := []struct {
		start, end time.Time
	}{
		{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)},
		{time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
		{time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 22, 0, 0, 0, 0, time.UTC)},
	}

	for i, w := range windows {
		if !w.Start.Equal(expectedBoundaries[i].start) {
			t.Errorf("window %d start: expected %v, got %v", i, expectedBoundaries[i].start, w.Start)
		}
		if !w.End.Equal(expectedBoundaries[i].end) {
			t.Errorf("window %d end: expected %v, got %v", i, expectedBoundaries[i].end, w.End)
		}
	}
}

func TestCrashRecoveryBetweenWindows(t *testing.T) {
	sourceAdapter := &MockSourceAdapterForShardGroup{
		shardGroups: []*adapter.ShardGroup{
			{ID: 1, StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), EndTime: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
		},
		seriesInWindow: map[string][]string{
			"2024-01-01T00:00:00Z-2024-01-08T00:00:00Z": {"series1"},
			"2024-01-08T00:00:00Z-2024-01-15T00:00:00Z": {"series1"},
		},
	}
	targetAdapter := &MockTargetAdapterForShardGroup{}

	cpMgr, err := checkpoint.NewManager(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create checkpoint manager: %v", err)
	}
	defer cpMgr.Close()

	// Simulate crash after first window was completed
	sgCP := &types.ShardGroupCheckpoint{
		TaskID:              "test-crash-recovery",
		ShardGroupID:        "1",
		WindowStart:         time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		WindowEnd:           time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC).UnixNano(),
		LastCompletedBatch:  0,
		LastTimestamp:      time.Now().UnixNano(),
		TotalProcessedRows:  10,
		Status:              types.StatusCompleted,
	}
	err = cpMgr.SaveShardGroupCheckpoint(context.Background(), sgCP)
	if err != nil {
		t.Fatalf("failed to save checkpoint: %v", err)
	}

	engine := &MigrationEngine{
		sourceRegistry: adapter.NewRegistry(),
		targetRegistry: adapter.NewRegistry(),
		checkpointMgr:  cpMgr,
		config: &types.MigrationConfig{
			InfluxToInflux: types.InfluxToInfluxConfig{
				Enabled:           true,
				QueryMode:         "shard-group",
				MaxSeriesPerQuery: 100,
				ShardGroupConfig: &types.ShardGroupConfig{
					TimeWindow: 168 * time.Hour,
				},
			},
			Migration: types.MigrationSettings{
				ChunkSize: 10000,
			},
		},
	}

	engine.sourceRegistry.RegisterSource("mock", func() adapter.SourceAdapter { return sourceAdapter })
	engine.targetRegistry.RegisterTarget("mock-target", func() adapter.TargetAdapter { return targetAdapter })

	task := &MigrationTask{
		ID:            "test-crash-recovery",
		SourceAdapter: "mock",
		TargetAdapter: "mock-target",
		Mapping: &types.MappingConfig{
			SourceTable:       "test_measurement",
			TargetMeasurement: "test_measurement",
			TimeRange: types.TimeRange{
				Start: "2024-01-01T00:00:00Z",
				End:   "2024-01-15T00:00:00Z",
			},
		},
	}

	err = engine.runTaskShardGroupMode(context.Background(), task)
	if err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	// Should only query once (second window)
	if len(sourceAdapter.queryBatches) != 1 {
		t.Errorf("expected 1 batch query after crash recovery, got %d", len(sourceAdapter.queryBatches))
	}

	// Verify records were written to target
	if len(targetAdapter.receivedRecords) == 0 {
		t.Error("expected records to be written to target adapter")
	}

	// Verify we got exactly 1 batch which is expected for crash recovery
	t.Logf("Crash recovery: processed %d batches, target received %d records",
		len(sourceAdapter.queryBatches), len(targetAdapter.receivedRecords))
}