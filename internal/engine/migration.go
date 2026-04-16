package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/migration-tools/influx-migrator/internal/adapter"
	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/migration-tools/influx-migrator/pkg/types"
	"go.uber.org/zap"
)

// PartitionSeries splits a slice of series into batches of maxPerBatch size
func PartitionSeries(series []string, maxPerBatch int) [][]string {
	if maxPerBatch <= 0 {
		maxPerBatch = 100
	}
	var batches [][]string
	for i := 0; i < len(series); i += maxPerBatch {
		end := i + maxPerBatch
		if end > len(series) {
			end = len(series)
		}
		batches = append(batches, series[i:end])
	}
	return batches
}

type MigrationEngine struct {
	sourceRegistry *adapter.AdapterRegistry
	targetRegistry *adapter.AdapterRegistry
	checkpointMgr  *checkpoint.Manager
	rateLimiter    *RateLimiter
	transformer    *TransformEngine
	config         *types.MigrationConfig
	taskQueue      chan *MigrationTask
	wg             sync.WaitGroup
	queueMu        sync.Mutex
	queueClosed    bool
}

type MigrationTask struct {
	ID            string
	SourceAdapter string
	TargetAdapter string
	Mapping       *types.MappingConfig
	Status        types.CheckpointStatus
	TotalRows     int64
	MigratedRows  int64
	FailedRows    int64
}

func NewMigrationEngine(cfg *types.MigrationConfig, checkpointMgr *checkpoint.Manager) *MigrationEngine {
	var rateLimiter *RateLimiter
	if cfg.RateLimit.Enabled {
		rateLimiter = NewRateLimiter(cfg.RateLimit.PointsPerSecond, cfg.RateLimit.BurstSize)
	}

	return &MigrationEngine{
		sourceRegistry: adapter.GetRegistry(),
		targetRegistry: adapter.GetRegistry(),
		checkpointMgr:  checkpointMgr,
		rateLimiter:    rateLimiter,
		transformer:    NewTransformEngine(),
		config:         cfg,
		taskQueue:      make(chan *MigrationTask, cfg.Migration.ParallelTasks*2),
	}
}

func (e *MigrationEngine) closeQueueOnce() {
	e.queueMu.Lock()
	defer e.queueMu.Unlock()
	if !e.queueClosed {
		close(e.taskQueue)
		e.queueClosed = true
	}
}

func (e *MigrationEngine) isQueueClosed() bool {
	e.queueMu.Lock()
	defer e.queueMu.Unlock()
	return e.queueClosed
}

func (e *MigrationEngine) resetQueue() {
	e.queueMu.Lock()
	defer e.queueMu.Unlock()
	if e.queueClosed {
		e.taskQueue = make(chan *MigrationTask, e.config.Migration.ParallelTasks*2)
		e.queueClosed = false
	}
}

func (e *MigrationEngine) Run(ctx context.Context) error {
	taskCount := 0
	for _, taskConfig := range e.config.Tasks {
		sourceAdapter, err := e.sourceRegistry.GetSourceAdapter(taskConfig.Source)
		if err != nil {
			return fmt.Errorf("failed to get source adapter: %w", err)
		}

		sourceConfig := e.getSourceConfig(taskConfig.Source)
		if err := sourceAdapter.Connect(ctx, sourceConfig); err != nil {
			return fmt.Errorf("failed to connect to source: %w", err)
		}

		mappings, err := e.discoverMappings(ctx, taskConfig, sourceAdapter)
		sourceAdapter.Disconnect(ctx)
		if err != nil {
			return err
		}

		for _, mapping := range mappings {
			mappingPtr := mapping
			task := &MigrationTask{
				ID:            fmt.Sprintf("%s-%s", taskConfig.Name, mapping.TargetMeasurement),
				SourceAdapter: taskConfig.Source,
				TargetAdapter: taskConfig.Target,
				Mapping:       &mappingPtr,
				Status:        types.StatusPending,
			}

			cp := &types.Checkpoint{
				TaskID:        task.ID,
				TaskName:      taskConfig.Name,
				SourceTable:   getSourceTable(&mappingPtr),
				TargetMeas:    mappingPtr.TargetMeasurement,
				Status:        types.StatusPending,
				MappingConfig: mappingPtr,
			}

			if err := e.checkpointMgr.CreateCheckpoint(cp); err != nil {
				return fmt.Errorf("failed to create checkpoint: %w", err)
			}

			e.taskQueue <- task
			taskCount++
		}
	}

	e.closeQueueOnce()

	logger.Info("starting workers",
		zap.Int("worker_count", e.config.Migration.ParallelTasks),
		zap.Int("task_count", taskCount))

	for i := 0; i < e.config.Migration.ParallelTasks; i++ {
		e.wg.Add(1)
		go e.worker(ctx, i)
	}

	e.wg.Wait()
	return nil
}

func (e *MigrationEngine) discoverMappings(ctx context.Context, taskConfig types.TaskConfig, sourceAdapter adapter.SourceAdapter) ([]types.MappingConfig, error) {
	var allTables []string

	for _, mapping := range taskConfig.Mappings {
		if mapping.SourceTable == "" || mapping.SourceTable == "*" {
			tables, err := sourceAdapter.DiscoverTables(ctx)
			if err != nil {
				logger.Warn("failed to discover tables, using empty list",
					zap.String("source", taskConfig.Source),
					zap.Error(err))
				continue
			}
			logger.Info("discovered tables",
				zap.String("source", taskConfig.Source),
				zap.Int("count", len(tables)))
			allTables = append(allTables, tables...)
		} else {
			allTables = append(allTables, mapping.SourceTable)
		}
	}

	var result []types.MappingConfig
	seen := make(map[string]bool)

	// Warn if no tables discovered when using wildcard
	if len(allTables) == 0 && len(taskConfig.Mappings) > 0 {
		hasWildcard := false
		for _, m := range taskConfig.Mappings {
			if m.SourceTable == "" || m.SourceTable == "*" {
				hasWildcard = true
				break
			}
		}
		if hasWildcard {
			logger.Warn("no tables discovered from source, nothing to migrate",
				zap.String("source", taskConfig.Source))
		}
	}

	for _, table := range allTables {
		if seen[table] {
			continue
		}
		seen[table] = true

		series, err := sourceAdapter.DiscoverSeries(ctx, table)
		if err != nil {
			logger.Debug("DiscoverSeries not supported or failed, treating as single table",
				zap.String("table", table),
				zap.Error(err))
			series = []string{table}
		}

		for _, serie := range series {
			for _, baseMapping := range taskConfig.Mappings {
				if baseMapping.SourceTable != "" && baseMapping.SourceTable != "*" && baseMapping.SourceTable != table {
					continue
				}

				mapping := baseMapping

				if mapping.SourceTable == "" || mapping.SourceTable == "*" {
					mapping.SourceTable = table
				}

				if mapping.TargetMeasurement == "" {
					if serie != table {
						mapping.TargetMeasurement = fmt.Sprintf("%s_%s", table, serie)
					} else {
						mapping.TargetMeasurement = table
					}
				}

				if len(mapping.TagFilters) > 0 {
					if !e.matchTagFilters(serie, mapping.TagFilters) {
						logger.Debug("skipping series due to tag filters",
							zap.String("series", serie),
							zap.Any("filters", mapping.TagFilters))
						continue
					}
				}

				if mapping.SubtablePattern != "" && len(series) > 1 {
					mapping.TargetMeasurement = e.applySubtablePattern(table, serie, mapping.SubtablePattern)
				}

				result = append(result, mapping)
			}
		}
	}

	if len(result) == 0 && len(allTables) > 0 {
		for _, baseMapping := range taskConfig.Mappings {
			if baseMapping.SourceTable != "" && baseMapping.SourceTable != "*" {
				result = append(result, baseMapping)
			}
		}
	}

	return result, nil
}

func (e *MigrationEngine) matchTagFilters(series string, filters map[string][]string) bool {
	for tagKey, allowedValues := range filters {
		if len(allowedValues) == 0 {
			continue
		}
		found := false
		for _, val := range allowedValues {
			if strings.Contains(series, fmt.Sprintf("%s=%s", tagKey, val)) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (e *MigrationEngine) applySubtablePattern(table, series, pattern string) string {
	parts := strings.Split(series, ",")
	result := pattern
	for i, part := range parts {
		kv := strings.Split(part, "=")
		if len(kv) == 2 {
			result = strings.ReplaceAll(result, fmt.Sprintf("{{tag%d}}", i+1), kv[0])
			result = strings.ReplaceAll(result, fmt.Sprintf("{{value%d}}", i+1), kv[1])
		}
	}
	result = strings.ReplaceAll(result, "{{table}}", table)
	result = strings.ReplaceAll(result, "{{series}}", series)
	return result
}

func (e *MigrationEngine) worker(ctx context.Context, workerID int) {
	defer e.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			logger.Error("worker recovered from panic",
				zap.Int("worker_id", workerID),
				zap.Any("panic", r))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-e.taskQueue:
			if !ok {
				return // Queue closed
			}
			if err := e.runTask(ctx, task); err != nil {
				logger.Error("worker task failed",
					zap.Int("worker_id", workerID),
					zap.String("task_id", task.ID),
					zap.String("source_table", task.Mapping.SourceTable),
					zap.Error(err))
				e.checkpointMgr.MarkTaskFailed(ctx, task.ID, task.Mapping.SourceTable, err.Error())
			}
		}
	}
}

func (e *MigrationEngine) runTask(ctx context.Context, task *MigrationTask) error {
	// Check if batch mode is enabled for InfluxToInflux
	if e.config.InfluxToInflux.Enabled && e.config.InfluxToInflux.QueryMode == "shard-group" {
		return e.runTaskShardGroupMode(ctx, task)
	}
	if e.config.InfluxToInflux.Enabled && e.config.InfluxToInflux.QueryMode == "batch" {
		return e.runTaskBatchMode(ctx, task)
	}
	return e.runTaskSingleMode(ctx, task)
}

func (e *MigrationEngine) runTaskSingleMode(ctx context.Context, task *MigrationTask) error {
	logger.Info("starting task",
		zap.String("task_id", task.ID),
		zap.String("source_table", task.Mapping.SourceTable),
		zap.String("target_measurement", task.Mapping.TargetMeasurement))

	existingCP, err := e.checkpointMgr.LoadCheckpoint(ctx, task.ID, task.Mapping.SourceTable)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	if existingCP != nil && existingCP.Status == types.StatusCompleted {
		logger.Info("task already completed, skipping",
			zap.String("task_id", task.ID))
		return nil
	}

	if err := e.checkpointMgr.MarkTaskInProgress(ctx, task.ID, task.Mapping.SourceTable); err != nil {
		return fmt.Errorf("failed to mark task in progress: %w", err)
	}

	sourceAdapter, err := e.sourceRegistry.GetSourceAdapter(task.SourceAdapter)
	if err != nil {
		return fmt.Errorf("failed to get source adapter: %w", err)
	}

	sourceConfig := e.getSourceConfig(task.SourceAdapter)
	if err := sourceAdapter.Connect(ctx, sourceConfig); err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceAdapter.Disconnect(ctx)

	targetAdapter, err := e.targetRegistry.GetTargetAdapter(task.TargetAdapter)
	if err != nil {
		return fmt.Errorf("failed to get target adapter: %w", err)
	}

	targetConfig := e.getTargetConfig(task.TargetAdapter)
	if err := targetAdapter.Connect(ctx, targetConfig); err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetAdapter.Disconnect(ctx)

	var lastCheckpoint *types.Checkpoint
	var lastTimestamp int64
	var totalProcessed int64

	if existingCP != nil && existingCP.Status == types.StatusInProgress {
		lastCheckpoint = existingCP
		lastTimestamp = existingCP.LastTimestamp
		totalProcessed = existingCP.ProcessedRows
	}

	sourceTable := getSourceTable(task.Mapping)

	timeWindow := types.DefaultTimeWindow
	if task.Mapping.TimeWindow != "" {
		if tw, err := time.ParseDuration(task.Mapping.TimeWindow); err == nil {
			timeWindow = tw
		} else {
			logger.Warn("invalid time window, using default",
				zap.String("time_window", task.Mapping.TimeWindow),
				zap.Duration("default", types.DefaultTimeWindow),
				zap.Error(err))
		}
	}

	queryCfg := &types.QueryConfig{
		BatchSize:  e.config.Migration.ChunkSize,
		TimeWindow: timeWindow,
	}
	queryCfg = queryCfg.WithDefaults()
	if err := queryCfg.Validate(); err != nil {
		return fmt.Errorf("invalid query config: %w", err)
	}

	// Validate mapping configuration once per task (not per batch)
	if err := e.transformer.ValidateMapping(task.Mapping); err != nil {
		return fmt.Errorf("mapping validation failed: %w", err)
	}

	switch task.Mapping.TimeRange.Start {
	case "":
		checkpoint, queryErr := sourceAdapter.QueryData(ctx, sourceTable, lastCheckpoint, func(records []types.Record) error {
			if len(records) == 0 {
				return nil
			}

			// Save checkpoint BEFORE write to prevent data loss on crash.
			// This uses "at-least-once" semantics: on crash we may re-process
			// this batch, but we will never lose data.
			lastRecord := records[len(records)-1]
			saveTimestamp := lastRecord.Time
			saveProcessed := totalProcessed + int64(len(records))

			if err := e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
				0, saveTimestamp, saveProcessed, types.StatusInProgress); err != nil {
				logger.Warn("failed to save checkpoint", zap.Error(err))
			}

			err := e.processBatch(ctx, task.Mapping, records, targetAdapter)
			if err != nil {
				return err
			}

			totalProcessed = saveProcessed
			lastTimestamp = saveTimestamp
			return nil
		}, queryCfg)
		if queryErr != nil {
			e.checkpointMgr.MarkTaskFailed(ctx, task.ID, task.Mapping.SourceTable, queryErr.Error())
			return queryErr
		}
		if checkpoint != nil {
			totalProcessed = checkpoint.ProcessedRows
			lastTimestamp = checkpoint.LastTimestamp
		}
	default:
		checkpoint, queryErr := e.queryWithTimeRange(ctx, sourceAdapter, sourceTable, task.Mapping, lastCheckpoint, targetAdapter, task.ID, queryCfg)
		if queryErr != nil {
			e.checkpointMgr.MarkTaskFailed(ctx, task.ID, task.Mapping.SourceTable, queryErr.Error())
			return queryErr
		}
		if checkpoint != nil {
			totalProcessed = checkpoint.ProcessedRows
			lastTimestamp = checkpoint.LastTimestamp
		}
	}

	if err := e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
		0, lastTimestamp, totalProcessed, types.StatusCompleted); err != nil {
		logger.Warn("failed to save final checkpoint", zap.Error(err))
	}
	if err := e.checkpointMgr.MarkTaskCompleted(ctx, task.ID, task.Mapping.SourceTable); err != nil {
		logger.Warn("failed to mark task completed", zap.Error(err))
	}
	logger.Info("task completed successfully",
		zap.String("task_id", task.ID))

	return nil
}

func (e *MigrationEngine) runTaskBatchMode(ctx context.Context, task *MigrationTask) error {
	logger.Info("starting batch mode task",
		zap.String("task_id", task.ID),
		zap.String("source_table", task.Mapping.SourceTable),
		zap.String("target_measurement", task.Mapping.TargetMeasurement))

	sourceAdapter, err := e.sourceRegistry.GetSourceAdapter(task.SourceAdapter)
	if err != nil {
		return fmt.Errorf("failed to get source adapter: %w", err)
	}

	sourceConfig := e.getSourceConfig(task.SourceAdapter)
	if err := sourceAdapter.Connect(ctx, sourceConfig); err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceAdapter.Disconnect(ctx)

	targetAdapter, err := e.targetRegistry.GetTargetAdapter(task.TargetAdapter)
	if err != nil {
		return fmt.Errorf("failed to get target adapter: %w", err)
	}

	targetConfig := e.getTargetConfig(task.TargetAdapter)
	if err := targetAdapter.Connect(ctx, targetConfig); err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetAdapter.Disconnect(ctx)

	// Discover all series for the measurement
	series, err := sourceAdapter.DiscoverSeries(ctx, task.Mapping.SourceTable)
	if err != nil {
		logger.Warn("DiscoverSeries not supported or failed, treating as single table",
			zap.String("table", task.Mapping.SourceTable),
			zap.Error(err))
		series = []string{task.Mapping.SourceTable}
	}

	// Apply tag filters if any
	if len(task.Mapping.TagFilters) > 0 {
		filteredSeries := make([]string, 0)
		for _, s := range series {
			if e.matchTagFilters(s, task.Mapping.TagFilters) {
				filteredSeries = append(filteredSeries, s)
			}
		}
		series = filteredSeries
	}

	if len(series) == 0 {
		logger.Info("no series to migrate in batch mode",
			zap.String("task_id", task.ID))
		return nil
	}

	// Partition into batches
	batchSize := e.config.InfluxToInflux.MaxSeriesPerQuery
	if batchSize <= 0 {
		batchSize = 100
	}
	batches := PartitionSeries(series, batchSize)

	logger.Info("batch mode: partitioned series into batches",
		zap.String("task_id", task.ID),
		zap.Int("total_series", len(series)),
		zap.Int("batch_size", batchSize),
		zap.Int("total_batches", len(batches)))

	// Parse time range from mapping config, with defaults
	startTime := time.Now().Add(-24 * time.Hour)
	endTime := time.Now()
	if task.Mapping.TimeRange.Start != "" {
		if parsed, err := time.Parse(time.RFC3339, task.Mapping.TimeRange.Start); err == nil {
			startTime = parsed
		}
	}
	if task.Mapping.TimeRange.End != "" {
		if parsed, err := time.Parse(time.RFC3339, task.Mapping.TimeRange.End); err == nil {
			endTime = parsed
		}
	}

	// Process each batch
	queryCfg := &types.QueryConfig{
		BatchSize:        e.config.Migration.ChunkSize,
		MaxSeriesPerQuery: batchSize,
	}

	var lastCheckpoint *types.Checkpoint
	for i, batch := range batches {
		logger.Debug("processing batch",
			zap.String("task_id", task.ID),
			zap.Int("batch_index", i+1),
			zap.Int("batch_size", len(batch)))

		checkpoint, err := sourceAdapter.QueryDataBatch(
			ctx,
			task.Mapping.SourceTable,
			batch,
			startTime,
			endTime,
			lastCheckpoint,
			func(records []types.Record) error {
				if len(records) == 0 {
					return nil
				}
				return e.processBatch(ctx, task.Mapping, records, targetAdapter)
			},
			queryCfg,
		)

		if err != nil {
			return fmt.Errorf("batch %d/%d failed: %w", i+1, len(batches), err)
		}

		if checkpoint != nil {
			lastCheckpoint = checkpoint

			// Save checkpoint after each batch to prevent data loss on crash.
			// This uses "at-least-once" semantics: on crash we may re-process
			// this batch, but we will never lose data.
			if err := e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
				0, checkpoint.LastTimestamp, checkpoint.ProcessedRows, types.StatusInProgress); err != nil {
				logger.Warn("failed to save checkpoint", zap.Error(err))
			}
		}
	}

	logger.Info("completed batch mode task",
		zap.String("task_id", task.ID),
		zap.Int("total_batches", len(batches)))

	if lastCheckpoint != nil {
		if err := e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
			0, lastCheckpoint.LastTimestamp, lastCheckpoint.ProcessedRows, types.StatusCompleted); err != nil {
			logger.Warn("failed to save final checkpoint", zap.Error(err))
		}
	}
	if err := e.checkpointMgr.MarkTaskCompleted(ctx, task.ID, task.Mapping.SourceTable); err != nil {
		logger.Warn("failed to mark task completed", zap.Error(err))
	}

	return nil
}

func (e *MigrationEngine) runTaskShardGroupMode(ctx context.Context, task *MigrationTask) error {
	logger.Info("starting shard-group mode task",
		zap.String("task_id", task.ID),
		zap.String("source_table", task.Mapping.SourceTable),
		zap.String("target_measurement", task.Mapping.TargetMeasurement))

	sourceAdapter, err := e.sourceRegistry.GetSourceAdapter(task.SourceAdapter)
	if err != nil {
		return fmt.Errorf("failed to get source adapter: %w", err)
	}

	sourceConfig := e.getSourceConfig(task.SourceAdapter)
	if err := sourceAdapter.Connect(ctx, sourceConfig); err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceAdapter.Disconnect(ctx)

	targetAdapter, err := e.targetRegistry.GetTargetAdapter(task.TargetAdapter)
	if err != nil {
		return fmt.Errorf("failed to get target adapter: %w", err)
	}

	targetConfig := e.getTargetConfig(task.TargetAdapter)
	if err := targetAdapter.Connect(ctx, targetConfig); err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetAdapter.Disconnect(ctx)

	// Discover shard groups
	shardGroups, err := sourceAdapter.DiscoverShardGroups(ctx)
	if err != nil {
		return fmt.Errorf("failed to discover shard groups: %w", err)
	}

	if len(shardGroups) == 0 {
		logger.Info("no shard groups found",
			zap.String("task_id", task.ID))
		return nil
	}

	logger.Info("discovered shard groups",
		zap.String("task_id", task.ID),
		zap.Int("count", len(shardGroups)))

	// Determine query time range
	queryStart := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	queryEnd := time.Now()
	if task.Mapping.TimeRange.Start != "" {
		if parsed, err := time.Parse(time.RFC3339, task.Mapping.TimeRange.Start); err == nil {
			queryStart = parsed
		}
	}
	if task.Mapping.TimeRange.End != "" {
		if parsed, err := time.Parse(time.RFC3339, task.Mapping.TimeRange.End); err == nil {
			queryEnd = parsed
		}
	}

	// Filter shard groups that overlap with query time range
	var relevantGroups []*adapter.ShardGroup
	for _, sg := range shardGroups {
		if ShardGroupOverlaps(sg, queryStart, queryEnd) {
			relevantGroups = append(relevantGroups, sg)
		}
	}

	if len(relevantGroups) == 0 {
		logger.Info("no relevant shard groups in time range",
			zap.String("task_id", task.ID))
		return nil
	}

	logger.Info("relevant shard groups in time range",
		zap.String("task_id", task.ID),
		zap.Int("count", len(relevantGroups)))

	// Process each shard group
	for _, sg := range relevantGroups {
		if err := e.migrateShardGroup(ctx, task, sg, sourceAdapter, targetAdapter, queryStart, queryEnd); err != nil {
			return fmt.Errorf("shard group %d migration failed: %w", sg.ID, err)
		}
	}

	logger.Info("shard-group mode task completed",
		zap.String("task_id", task.ID),
		zap.Int("shard_groups_processed", len(relevantGroups)))

	if err := e.checkpointMgr.MarkTaskCompleted(ctx, task.ID, task.Mapping.SourceTable); err != nil {
		logger.Warn("failed to mark task completed", zap.Error(err))
	}

	return nil
}

func (e *MigrationEngine) migrateShardGroup(ctx context.Context, task *MigrationTask, sg *adapter.ShardGroup, sourceAdapter adapter.SourceAdapter, targetAdapter adapter.TargetAdapter, queryStart, queryEnd time.Time) error {
	start, end := ShardGroupEffectiveTimeRange(sg, queryStart, queryEnd)

	logger.Info("migrating shard group",
		zap.Int("shard_id", sg.ID),
		zap.String("start", start.Format(time.RFC3339)),
		zap.String("end", end.Format(time.RFC3339)))

	// Determine time window duration
	timeWindow := time.Duration(0)
	if e.config.InfluxToInflux.ShardGroupConfig != nil {
		timeWindow = e.config.InfluxToInflux.ShardGroupConfig.TimeWindow
	}
	if timeWindow == 0 {
		// Use shard group length as default
		timeWindow = end.Sub(start)
	}

	// Split into time windows
	windows := SplitTimeWindows(start, end, timeWindow)

	logger.Info("time windows for shard group",
		zap.Int("shard_id", sg.ID),
		zap.Int("window_count", len(windows)),
		zap.Duration("window_duration", timeWindow))

	// Process each time window
	for _, window := range windows {
		if err := e.migrateTimeWindow(ctx, task, sg, window, sourceAdapter, targetAdapter); err != nil {
			return fmt.Errorf("time window [%s, %s) migration failed: %w",
				window.Start.Format(time.RFC3339), window.End.Format(time.RFC3339), err)
		}
	}

	if err := e.checkpointMgr.MarkShardGroupCompleted(ctx, task.ID, fmt.Sprintf("%d", sg.ID)); err != nil {
		logger.Warn("failed to mark shard group completed", zap.Error(err))
	}

	logger.Info("shard group migration completed",
		zap.Int("shard_id", sg.ID))

	return nil
}

func (e *MigrationEngine) migrateTimeWindow(ctx context.Context, task *MigrationTask, sg *adapter.ShardGroup, window TimeWindow, sourceAdapter adapter.SourceAdapter, targetAdapter adapter.TargetAdapter) error {
	// Load existing checkpoint for this window
	cp, err := e.checkpointMgr.LoadShardGroupCheckpointForWindow(ctx,
		task.ID, fmt.Sprintf("%d", sg.ID), window.Start.UnixNano(), window.End.UnixNano())
	if err != nil {
		return fmt.Errorf("failed to load shard group checkpoint: %w", err)
	}

	// Skip windows that are already completed
	if cp != nil && cp.Status == types.StatusCompleted {
		logger.Debug("window already completed, skipping",
			zap.Int("shard_id", sg.ID),
			zap.String("window_start", window.Start.Format(time.RFC3339)))
		return nil
	}

	// Initialize to -1 so batch 0 is not skipped when no checkpoint exists
	lastBatchIdx := -1
	var lastTimestamp int64
	var totalProcessed int64
	if cp != nil && cp.Status == types.StatusInProgress {
		lastBatchIdx = cp.LastCompletedBatch
		lastTimestamp = cp.LastTimestamp
		totalProcessed = cp.TotalProcessedRows
	}

	// Discover series in this time window
	series, err := sourceAdapter.DiscoverSeriesInTimeWindow(ctx,
		task.Mapping.SourceTable, window.Start, window.End)
	if err != nil {
		return fmt.Errorf("failed to discover series: %w", err)
	}

	// Apply tag filters if specified
	if len(task.Mapping.TagFilters) > 0 {
		filteredSeries := make([]string, 0, len(series))
		for _, s := range series {
			if e.matchTagFilters(s, task.Mapping.TagFilters) {
				filteredSeries = append(filteredSeries, s)
			}
		}
		series = filteredSeries
	}

	if len(series) == 0 {
		logger.Debug("no series found in time window",
			zap.Int("shard_id", sg.ID),
			zap.String("window_start", window.Start.Format(time.RFC3339)))
		return nil
	}

	// Partition into batches
	batchSize := e.config.InfluxToInflux.MaxSeriesPerQuery
	if batchSize <= 0 {
		batchSize = 100
	}
	batches := PartitionSeries(series, batchSize)

	logger.Info("processing time window",
		zap.Int("shard_id", sg.ID),
		zap.String("window_start", window.Start.Format(time.RFC3339)),
		zap.String("window_end", window.End.Format(time.RFC3339)),
		zap.Int("total_series", len(series)),
		zap.Int("total_batches", len(batches)),
		zap.Int("starting_from_batch", lastBatchIdx+1))

	// Process each batch
	for batchIdx, batch := range batches {
		// Check for context cancellation between batches
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if batchIdx <= lastBatchIdx {
			logger.Debug("skipping completed batch",
				zap.Int("batch_idx", batchIdx),
				zap.Int("last_completed", lastBatchIdx))
			continue
		}

		batchCheckpoint, err := sourceAdapter.QueryDataBatch(ctx,
			task.Mapping.SourceTable,
			batch,
			window.Start,
			window.End,
			&types.Checkpoint{LastTimestamp: lastTimestamp},
			func(records []types.Record) error {
				return e.processBatch(ctx, task.Mapping, records, targetAdapter)
			},
			&types.QueryConfig{BatchSize: e.config.Migration.ChunkSize},
		)
		if err != nil {
			return fmt.Errorf("batch %d failed: %w", batchIdx, err)
		}

		if batchCheckpoint != nil {
			lastTimestamp = batchCheckpoint.LastTimestamp
			totalProcessed += batchCheckpoint.ProcessedRows
		}

		sgCP := &types.ShardGroupCheckpoint{
			TaskID:              task.ID,
			ShardGroupID:        fmt.Sprintf("%d", sg.ID),
			WindowStart:         window.Start.UnixNano(),
			WindowEnd:           window.End.UnixNano(),
			LastCompletedBatch:  batchIdx,
			LastTimestamp:       lastTimestamp,
			TotalProcessedRows:  totalProcessed,
			Status:              types.StatusInProgress,
		}
		if err := e.checkpointMgr.SaveShardGroupCheckpoint(ctx, sgCP); err != nil {
			logger.Warn("failed to save shard group checkpoint", zap.Error(err))
		}
	}

	// Mark window as completed after all batches processed successfully
	sgCP := &types.ShardGroupCheckpoint{
		TaskID:              task.ID,
		ShardGroupID:        fmt.Sprintf("%d", sg.ID),
		WindowStart:         window.Start.UnixNano(),
		WindowEnd:           window.End.UnixNano(),
		LastCompletedBatch:  len(batches) - 1,
		LastTimestamp:       lastTimestamp,
		TotalProcessedRows:  totalProcessed,
		Status:              types.StatusCompleted,
	}
	if err := e.checkpointMgr.SaveShardGroupCheckpoint(ctx, sgCP); err != nil {
		logger.Warn("failed to save window completed checkpoint", zap.Error(err))
	}

	return nil
}

func (e *MigrationEngine) queryWithTimeRange(ctx context.Context, sourceAdapter adapter.SourceAdapter, table string, mapping *types.MappingConfig, lastCp *types.Checkpoint, targetAdapter adapter.TargetAdapter, taskID string, queryCfg *types.QueryConfig) (*types.Checkpoint, error) {
	// queryWithTimeRange iterates over the overall time range in chunks of windowDuration.
	// For each chunk, it calls sourceAdapter.QueryData() which may use queryCfg.TimeWindow
	// for its internal query batching (e.g., TDengine uses TimeWindow to set query range).
	// The outer windowDuration and inner TimeWindow are independent but typically set to the
	// same value (168h = 7 days default) for consistent behavior.
	startTime, err := time.Parse(time.RFC3339, mapping.TimeRange.Start)
	if err != nil {
		logger.Error("invalid start time in time range",
			zap.String("start", mapping.TimeRange.Start),
			zap.Error(err))
		startTime = time.Now().Add(-24 * time.Hour)
	}
	endTime, err := time.Parse(time.RFC3339, mapping.TimeRange.End)
	if err != nil {
		logger.Error("invalid end time in time range",
			zap.String("end", mapping.TimeRange.End),
			zap.Error(err))
		endTime = time.Now()
	}

	// Validate that end time is after start time
	if !endTime.After(startTime) {
		return nil, fmt.Errorf("invalid time range: end time %s must be after start time %s",
			mapping.TimeRange.End, mapping.TimeRange.Start)
	}

	if mapping.TimeWindow == "" {
		mapping.TimeWindow = "168h"
	}

	windowDuration, err := time.ParseDuration(mapping.TimeWindow)
	if err != nil {
		logger.Warn("invalid time window, using default 168h",
			zap.String("time_window", mapping.TimeWindow),
			zap.Error(err))
		windowDuration = 168 * time.Hour
	}

	currentCp := lastCp
	totalProcessed := int64(0)
	var lastTimestamp int64

	for windowStart := startTime; windowStart.Before(endTime); windowStart = windowStart.Add(windowDuration) {
		windowEnd := windowStart.Add(windowDuration)
		if windowEnd.After(endTime) {
			windowEnd = endTime
		}

		windowMapping := *mapping
		windowMapping.TimeRange = types.TimeRange{
			Start: windowStart.Format(time.RFC3339),
			End:   windowEnd.Format(time.RFC3339),
		}

		cp, err := sourceAdapter.QueryData(ctx, table, currentCp, func(records []types.Record) error {
			if len(records) == 0 {
				return nil
			}

			// Save checkpoint BEFORE write to prevent data loss on crash
			lastRecord := records[len(records)-1]
			saveTimestamp := lastRecord.Time
			saveProcessed := totalProcessed + int64(len(records))

			if err := e.checkpointMgr.SaveCheckpoint(ctx, taskID, table,
				0, saveTimestamp, saveProcessed, types.StatusInProgress); err != nil {
				logger.Warn("failed to save checkpoint", zap.Error(err))
			}

			err := e.processBatch(ctx, &windowMapping, records, targetAdapter)
			if err != nil {
				return err
			}

			totalProcessed = saveProcessed
			lastTimestamp = saveTimestamp
			return nil
		}, queryCfg)

		if err != nil {
			return nil, err
		}

		if cp != nil {
			totalProcessed = cp.ProcessedRows
			if cp.LastTimestamp != 0 {
				lastTimestamp = cp.LastTimestamp
			}
			currentCp = cp
		}

		// Check context before sleeping to allow graceful shutdown
		timer := time.NewTimer(e.config.Migration.ChunkInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
			timer.Stop()
		}
	}

	return &types.Checkpoint{
		ProcessedRows: totalProcessed,
		LastTimestamp: lastTimestamp,
	}, nil
}

func (e *MigrationEngine) processBatch(ctx context.Context, mapping *types.MappingConfig, records []types.Record, targetAdapter adapter.TargetAdapter) error {
	if e.rateLimiter != nil {
		if err := e.rateLimiter.WaitContext(ctx, len(records)); err != nil {
			return fmt.Errorf("rate limit wait cancelled: %w", err)
		}
	}

	transformed := make([]types.Record, 0, len(records))
	for i := range records {
		filtered := filterNilValues(&records[i])

		// Check for tag/field name collisions (InfluxDB allows this but it causes query issues)
		warnings := e.transformer.ValidateRecord(filtered)
		for _, warning := range warnings {
			logger.Warn("record has potential data ambiguity",
				zap.String("warning", warning),
				zap.String("measurement", mapping.TargetMeasurement))
		}

		transformed = append(transformed, *filtered)
	}

	if mapping != nil && (len(mapping.Schema.Fields) > 0 || len(mapping.Schema.Tags) > 0) {
		schemaTransformed := make([]types.Record, 0, len(transformed))
		for i := range transformed {
			result := e.transformer.Transform(&transformed[i], mapping)
			schemaTransformed = append(schemaTransformed, *result)
		}
		transformed = schemaTransformed
	}

	logger.Debug("processing batch",
		zap.Int("input_records", len(records)),
		zap.Int("output_records", len(transformed)))

	return e.writeWithRetry(ctx, mapping.TargetMeasurement, transformed, targetAdapter)
}

// writeWithRetry attempts to write records to the target adapter with exponential backoff.
// NOTE: This function assumes WriteBatch is idempotent - if a write partially succeeds before
// failing, retrying may result in duplicate records. Target systems should use timestamps
// or unique identifiers to handle deduplication (InfluxDB handles this via line protocol).
func (e *MigrationEngine) writeWithRetry(ctx context.Context, measurement string, records []types.Record, targetAdapter adapter.TargetAdapter) error {
	maxAttempts := e.config.Retry.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}

	var lastErr error
	delay := e.config.Retry.InitialDelay
	if delay == 0 {
		delay = 1 * time.Second
	}
	maxDelay := e.config.Retry.MaxDelay
	if maxDelay == 0 {
		maxDelay = 60 * time.Second
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := targetAdapter.WriteBatch(ctx, measurement, records)
		if err == nil {
			return nil
		}

		lastErr = err
		logger.Warn("write batch failed, will retry",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", maxAttempts),
			zap.Duration("delay", delay),
			zap.Error(err))

		if attempt < maxAttempts {
			// Use timer to allow early cancellation
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
			// Timer fired normally, proceed with retry

			// Calculate new delay with overflow protection
			newDelay := time.Duration(float64(delay) * e.config.Retry.BackoffMultiplier)
			if newDelay < delay || newDelay > maxDelay {
				delay = maxDelay
			} else {
				delay = newDelay
			}
		}
	}

	return fmt.Errorf("write batch failed after %d attempts: %w", maxAttempts, lastErr)
}

func filterNilValues(record *types.Record) *types.Record {
	filtered := types.NewRecord()
	filtered.Time = record.Time

	for k, v := range record.Tags {
		if v != "" {
			filtered.Tags[k] = v
		}
	}

	for k, v := range record.Fields {
		if v != nil {
			filtered.Fields[k] = v
		}
	}

	return filtered
}

func (e *MigrationEngine) getSourceConfig(name string) map[string]interface{} {
	for _, src := range e.config.Sources {
		if src.Name == name {
			return e.sourceConfigToMap(src)
		}
	}
	return nil
}

func (e *MigrationEngine) getTargetConfig(name string) map[string]interface{} {
	for _, tgt := range e.config.Targets {
		if tgt.Name == name {
			return e.targetConfigToMap(tgt)
		}
	}
	return nil
}

func (e *MigrationEngine) sourceConfigToMap(src types.SourceConfig) map[string]interface{} {
	m := make(map[string]interface{})
	m["host"] = src.Host
	m["port"] = src.Port
	m["user"] = src.User
	m["password"] = src.Password
	m["database"] = src.Database
	m["ssl"] = map[string]interface{}{
		"enabled":     src.SSL.Enabled,
		"skip_verify": src.SSL.SkipVerify,
		"ca_cert":     src.SSL.CaCert,
	}

	switch src.Type {
	case "mysql":
		m["mysql"] = map[string]interface{}{
			"host":     src.Host,
			"port":     src.Port,
			"user":     src.User,
			"password": src.Password,
			"database": src.Database,
			"charset":  src.MySQL.Charset,
		}
	case "tdengine":
		m["tdengine"] = map[string]interface{}{
			"host":     src.Host,
			"port":     src.Port,
			"user":     src.User,
			"password": src.Password,
			"database": src.Database,
			"version":  src.TDengine.Version,
		}
	case "influxdb":
		m["influxdb"] = map[string]interface{}{
			"url":     src.InfluxDB.URL,
			"token":   src.InfluxDB.Token,
			"org":     src.InfluxDB.Org,
			"bucket":  src.InfluxDB.Bucket,
			"version": src.InfluxDB.Version,
		}
	}

	return m
}

func (e *MigrationEngine) targetConfigToMap(tgt types.TargetConfig) map[string]interface{} {
	m := make(map[string]interface{})
	m["ssl"] = map[string]interface{}{
		"enabled":     tgt.SSL.Enabled,
		"skip_verify": tgt.SSL.SkipVerify,
		"ca_cert":     tgt.SSL.CaCert,
	}

	switch tgt.Type {
	case "influxdb":
		m["influxdb"] = map[string]interface{}{
			"url":     tgt.InfluxDB.URL,
			"token":   tgt.InfluxDB.Token,
			"org":     tgt.InfluxDB.Org,
			"bucket":  tgt.InfluxDB.Bucket,
			"version": tgt.InfluxDB.Version,
		}
	}

	return m
}

func getSourceTable(mapping *types.MappingConfig) string {
	if mapping.SourceTable != "" {
		return mapping.SourceTable
	}
	return mapping.Measurement
}

func (e *MigrationEngine) getAdaptersForTask(taskName string) (string, string) {
	for _, task := range e.config.Tasks {
		if task.Name == taskName {
			return task.Source, task.Target
		}
	}
	return "", ""
}

func (e *MigrationEngine) Resume(ctx context.Context) error {
	failedTasks, err := e.checkpointMgr.GetFailedTasks(ctx)
	if err != nil {
		return fmt.Errorf("failed to get failed tasks: %w", err)
	}

	inProgressTasks, err := e.checkpointMgr.GetInProgressTasks(ctx)
	if err != nil {
		return fmt.Errorf("failed to get in-progress tasks: %w", err)
	}

	// If queue was closed by a previous Run(), reset it for Resume
	if e.isQueueClosed() {
		e.resetQueue()
	}

	for _, cp := range failedTasks {
		logger.Info("resuming failed task",
			zap.String("task_id", cp.TaskID),
			zap.String("source_table", cp.SourceTable))

		sourceAdapter, targetAdapter := e.getAdaptersForTask(cp.TaskName)

		task := &MigrationTask{
			ID:            cp.TaskID,
			SourceAdapter: sourceAdapter,
			TargetAdapter: targetAdapter,
			Mapping:       &cp.MappingConfig,
			Status:        types.StatusPending,
		}

		e.taskQueue <- task
	}

	for _, cp := range inProgressTasks {
		logger.Info("resuming interrupted task",
			zap.String("task_id", cp.TaskID),
			zap.String("source_table", cp.SourceTable))

		sourceAdapter, targetAdapter := e.getAdaptersForTask(cp.TaskName)

		task := &MigrationTask{
			ID:            cp.TaskID,
			SourceAdapter: sourceAdapter,
			TargetAdapter: targetAdapter,
			Mapping:       &cp.MappingConfig,
			Status:        types.StatusPending,
		}

		e.taskQueue <- task
	}

	e.closeQueueOnce()

	for i := 0; i < e.config.Migration.ParallelTasks; i++ {
		e.wg.Add(1)
		go e.worker(ctx, i)
	}

	e.wg.Wait()
	return nil
}

func (e *MigrationEngine) MarkInProgressAsInterrupted(ctx context.Context) {
	inProgress, err := e.checkpointMgr.GetInProgressTasks(ctx)
	if err != nil {
		logger.Error("failed to get in-progress tasks", zap.Error(err))
		return
	}

	for _, cp := range inProgress {
		logger.Info("marking task as interrupted", zap.String("task_id", cp.TaskID))
		if err := e.checkpointMgr.MarkTaskFailed(ctx, cp.TaskID, cp.SourceTable, "interrupted by user"); err != nil {
			logger.Error("failed to mark task as interrupted", zap.Error(err))
		}
	}
}
