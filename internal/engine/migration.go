package engine

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/migration-tools/influx-migrator/internal/adapter"
	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/migration-tools/influx-migrator/pkg/types"
	"go.uber.org/zap"
)

// writeStatusRe extracts the HTTP status code from the target adapter's
// string-based write error of the form "write failed with status <N>: ...".
// This is a pragmatic bridge until the target adapter returns a typed error
// (see B4/engine-typed-error work). Matching the status lets writeWithRetry
// avoid burning retries on permanent 4xx failures (auth/permission/malformed
// line protocol) that no amount of backoff will fix.
var writeStatusRe = regexp.MustCompile(`status (\d{3})`)

// rateLimitStatus is the 4xx status that IS retryable: 429 Too Many Requests
// indicates a transient rate-limit condition, not a permanent configuration
// error.
const rateLimitStatus = 429

// isRetryableWriteError classifies a target WriteBatch error as retryable or
// permanent. It treats 5xx server errors, 429 rate-limit responses, and
// non-HTTP (network/timeout) errors as retryable. 4xx client errors (except
// 429) are permanent: they reflect misconfiguration (auth, permissions) or
// malformed input (line protocol) that retries cannot fix, so retrying only
// wastes time and masks the underlying config bug as a transient failure.
//
// NOTE: This inspects the error *string* because the current target adapter
// returns fmt.Errorf("write failed with status %d: ...") rather than a typed
// error. Once the target adapter is refactored to return a typed
// WriteHTTPError (deferred to the B4/engine-typed-error stream), this should
// switch to errors.As for robustness. A nil error is treated as retryable
// (callers must not invoke this on nil, but the safe default avoids masking a
// nil-check bug as a permanent failure).
func isRetryableWriteError(err error) bool {
	if err == nil {
		return true
	}
	msg := err.Error()
	m := writeStatusRe.FindStringSubmatch(msg)
	if m == nil {
		// No HTTP status in the message: treat as a network/timeout error,
		// which is retryable.
		return true
	}
	var code int
	if _, parseErr := fmt.Sscanf(m[1], "%d", &code); parseErr != nil {
		return true
	}
	switch {
	case code == rateLimitStatus:
		return true
	case code >= 500 && code < 600:
		return true
	case code >= 400 && code < 500:
		// 4xx (except 429 handled above) is a permanent client error.
		return false
	default:
		// Unknown status class (e.g. 3xx, 2xx reported as an error): default
		// to retryable to preserve prior behavior for unusual cases.
		return true
	}
}

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
	workerErrMu    sync.Mutex
	workerErrs     []error
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
		taskQueue:      make(chan *MigrationTask, taskQueueCapacity(cfg.Migration.ParallelTasks)),
	}
}

func taskQueueCapacity(parallelTasks int) int {
	if parallelTasks <= 0 {
		return 2
	}
	return parallelTasks * 2
}

func workerCount(parallelTasks int) int {
	if parallelTasks <= 0 {
		return 1
	}
	return parallelTasks
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
		e.taskQueue = make(chan *MigrationTask, taskQueueCapacity(e.config.Migration.ParallelTasks))
		e.queueClosed = false
	}
}

func (e *MigrationEngine) resetWorkerErrors() {
	e.workerErrMu.Lock()
	defer e.workerErrMu.Unlock()
	e.workerErrs = nil
}

func (e *MigrationEngine) recordWorkerError(err error) {
	if err == nil {
		return
	}
	e.workerErrMu.Lock()
	defer e.workerErrMu.Unlock()
	e.workerErrs = append(e.workerErrs, err)
}

func (e *MigrationEngine) workerError() error {
	e.workerErrMu.Lock()
	defer e.workerErrMu.Unlock()
	if len(e.workerErrs) == 0 {
		return nil
	}
	if len(e.workerErrs) == 1 {
		return e.workerErrs[0]
	}
	return fmt.Errorf("%d migration tasks failed; first error: %w", len(e.workerErrs), e.workerErrs[0])
}

func (e *MigrationEngine) startWorkers(ctx context.Context) int {
	count := workerCount(e.config.Migration.ParallelTasks)
	for i := 0; i < count; i++ {
		e.wg.Add(1)
		go e.worker(ctx, i)
	}
	return count
}

func (e *MigrationEngine) finishWorkers() error {
	e.closeQueueOnce()
	e.wg.Wait()
	return e.workerError()
}

func (e *MigrationEngine) enqueueTask(ctx context.Context, task *MigrationTask) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case e.taskQueue <- task:
		return nil
	}
}

func (e *MigrationEngine) Run(ctx context.Context) (err error) {
	if e.isQueueClosed() {
		e.resetQueue()
	}
	e.resetWorkerErrors()
	workerCount := e.startWorkers(ctx)
	defer func() {
		if workerErr := e.finishWorkers(); err == nil && workerErr != nil {
			err = workerErr
		}
	}()

	taskCount := 0
	for _, taskConfig := range e.config.Tasks {
		sourceAdapterType, err := e.getSourceAdapterType(taskConfig.Source)
		if err != nil {
			return err
		}
		sourceAdapter, err := e.sourceRegistry.GetSourceAdapter(sourceAdapterType)
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

			if err := e.enqueueTask(ctx, task); err != nil {
				return fmt.Errorf("failed to enqueue task: %w", err)
			}
			taskCount++
		}
	}

	logger.Info("queued migration tasks",
		zap.Int("worker_count", workerCount),
		zap.Int("task_count", taskCount))
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

				// Deduplicate based on source_table:target_measurement
				dedupKey := fmt.Sprintf("%s:%s", table, mapping.TargetMeasurement)
				if seen[dedupKey] {
					continue
				}
				seen[dedupKey] = true

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
	tags := types.ParseSeriesKey(series).Tags
	for tagKey, allowedValues := range filters {
		if len(allowedValues) == 0 {
			continue
		}
		tagValue, ok := tags[tagKey]
		if !ok {
			return false
		}
		found := false
		for _, val := range allowedValues {
			if tagValue == val {
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
	parsed := types.ParseSeriesKey(series)
	result := pattern
	for i, tag := range parsed.TagPairs {
		idx := i + 1
		result = strings.ReplaceAll(result, fmt.Sprintf("{{tag%d}}", idx), tag.Key)
		result = strings.ReplaceAll(result, fmt.Sprintf("{{value%d}}", idx), tag.Value)
	}
	result = strings.ReplaceAll(result, "{{table}}", table)
	result = strings.ReplaceAll(result, "{{series}}", series)
	return result
}

func (e *MigrationEngine) worker(ctx context.Context, workerID int) {
	defer e.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-e.taskQueue:
			if !ok {
				return // Queue closed
			}
			// Wrap in closure to capture task for panic recovery
			func(t *MigrationTask) {
				defer func() {
					if r := recover(); r != nil {
						panicErr := fmt.Errorf("panic: %v", r)
						logger.Error("worker recovered from panic",
							zap.Int("worker_id", workerID),
							zap.String("task_id", t.ID),
							zap.String("source_table", t.Mapping.SourceTable),
							zap.Any("panic", r))
						e.checkpointMgr.MarkTaskFailed(ctx, t.ID, t.Mapping.SourceTable, panicErr.Error())
						e.recordWorkerError(fmt.Errorf("task %s failed: %w", t.ID, panicErr))
					}
				}()
				if err := e.runTask(ctx, t); err != nil {
					logger.Error("worker task failed",
						zap.Int("worker_id", workerID),
						zap.String("task_id", t.ID),
						zap.String("source_table", t.Mapping.SourceTable),
						zap.Error(err))
					e.checkpointMgr.MarkTaskFailed(ctx, t.ID, t.Mapping.SourceTable, err.Error())
					e.recordWorkerError(fmt.Errorf("task %s failed: %w", t.ID, err))
				}
			}(task)
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

	sourceAdapterType, err := e.getSourceAdapterType(task.SourceAdapter)
	if err != nil {
		return err
	}
	sourceAdapter, err := e.sourceRegistry.GetSourceAdapter(sourceAdapterType)
	if err != nil {
		return fmt.Errorf("failed to get source adapter: %w", err)
	}

	sourceConfig := e.getSourceConfig(task.SourceAdapter)
	if err := sourceAdapter.Connect(ctx, sourceConfig); err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceAdapter.Disconnect(ctx)

	targetAdapterType, err := e.getTargetAdapterType(task.TargetAdapter)
	if err != nil {
		return err
	}
	targetAdapter, err := e.targetRegistry.GetTargetAdapter(targetAdapterType)
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
	// totalWrittenRows tracks records actually passed to the target (after
	// transform-side drops) so the completion reconciliation can compare
	// source-read rows against written-to-target rows.
	var totalWrittenRows int64

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

			lastRecord := records[len(records)-1]
			saveTimestamp := lastRecord.Time
			saveProcessed := totalProcessed + int64(len(records))

			written, err := e.processBatch(ctx, task.Mapping, records, targetAdapter)
			if err != nil {
				return err
			}
			totalWrittenRows += int64(written)

			// Build checkpoint with updated progress fields, preserving existing data
			cp := &types.Checkpoint{
				TaskID:        task.ID,
				SourceTable:   task.Mapping.SourceTable,
				LastID:        0,
				LastTimestamp: saveTimestamp,
				ProcessedRows: saveProcessed,
				Status:        types.StatusInProgress,
			}
			if existingCP != nil {
				cp.TaskName = existingCP.TaskName
				cp.TargetMeas = existingCP.TargetMeas
				cp.MappingConfig = existingCP.MappingConfig
			}
			if err := e.checkpointMgr.SaveCheckpoint(ctx, cp); err != nil {
				if e.config.Migration.FailOnCheckpointError {
					return fmt.Errorf("failed to save checkpoint: %w", err)
				}
				logger.Warn("checkpoint save failed; persisted cursor is stale — duplication/re-work risk on crash (safe for InfluxDB overwrite semantics)", zap.Error(err))
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
		checkpoint, queryErr := e.queryWithTimeRange(ctx, sourceAdapter, sourceTable, task.Mapping, lastCheckpoint, targetAdapter, task.ID, queryCfg, &totalWrittenRows)
		if queryErr != nil {
			e.checkpointMgr.MarkTaskFailed(ctx, task.ID, task.Mapping.SourceTable, queryErr.Error())
			return queryErr
		}
		if checkpoint != nil {
			totalProcessed = checkpoint.ProcessedRows
			lastTimestamp = checkpoint.LastTimestamp
		}
	}

	// Build final checkpoint with all fields
	cp := &types.Checkpoint{
		TaskID:            task.ID,
		SourceTable:       task.Mapping.SourceTable,
		LastID:            0,
		LastTimestamp:     lastTimestamp,
		ProcessedRows:     totalProcessed,
		TotalMigratedRows: totalProcessed,
		Status:            types.StatusCompleted,
	}
	if existingCP != nil {
		cp.TaskName = existingCP.TaskName
		cp.TargetMeas = existingCP.TargetMeas
		cp.MappingConfig = existingCP.MappingConfig
	}
	if err := e.checkpointMgr.SaveCheckpoint(ctx, cp); err != nil {
		logger.Warn("failed to save final checkpoint", zap.Error(err))
	}
	if err := e.checkpointMgr.MarkTaskCompleted(ctx, task.ID, task.Mapping.SourceTable); err != nil {
		logger.Warn("failed to mark task completed", zap.Error(err))
	}
	if msg := reconcileRowCounts(totalProcessed, totalWrittenRows); msg != "" {
		logger.Warn(msg,
			zap.String("task_id", task.ID),
			zap.Int64("source_rows", totalProcessed),
			zap.Int64("written_rows", totalWrittenRows))
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

	lastCheckpoint, err := e.startTaskCheckpoint(ctx, task)
	if err != nil {
		return err
	}
	if lastCheckpoint != nil && lastCheckpoint.Status == types.StatusCompleted {
		logger.Info("batch mode task already completed, skipping",
			zap.String("task_id", task.ID))
		return nil
	}

	sourceAdapterType, err := e.getSourceAdapterType(task.SourceAdapter)
	if err != nil {
		return err
	}
	sourceAdapter, err := e.sourceRegistry.GetSourceAdapter(sourceAdapterType)
	if err != nil {
		return fmt.Errorf("failed to get source adapter: %w", err)
	}

	sourceConfig := e.getSourceConfig(task.SourceAdapter)
	if err := sourceAdapter.Connect(ctx, sourceConfig); err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceAdapter.Disconnect(ctx)

	targetAdapterType, err := e.getTargetAdapterType(task.TargetAdapter)
	if err != nil {
		return err
	}
	targetAdapter, err := e.targetRegistry.GetTargetAdapter(targetAdapterType)
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
		e.saveCompletedTaskCheckpoint(ctx, task, lastCheckpoint, 0, 0, 0)
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

	// Determine time range: use config if specified, otherwise discover from shard groups
	var startTime, endTime time.Time
	windowDuration := 168 * time.Hour // default 7 days

	if task.Mapping.TimeRange.Start != "" && task.Mapping.TimeRange.End != "" {
		// Use configured time range
		startTime, err = time.Parse(time.RFC3339, task.Mapping.TimeRange.Start)
		if err != nil {
			return fmt.Errorf("invalid start time: %w", err)
		}
		endTime, err = time.Parse(time.RFC3339, task.Mapping.TimeRange.End)
		if err != nil {
			return fmt.Errorf("invalid end time: %w", err)
		}
	} else {
		// Discover from shard groups: use first shard start and last shard end
		shardGroups, err := sourceAdapter.DiscoverShardGroups(ctx)
		if err != nil {
			return fmt.Errorf("failed to discover shard groups: %w", err)
		}
		if len(shardGroups) == 0 {
			logger.Warn("no shard groups found, using default time range (1970 to now+1h)")
			startTime = time.Unix(0, 0)
			endTime = time.Now().Add(1 * time.Hour)
		} else {
			// Sort by start time
			sort.Slice(shardGroups, func(i, j int) bool {
				return shardGroups[i].StartTime.Before(shardGroups[j].StartTime)
			})
			startTime = shardGroups[0].StartTime
			// Use last shard group's end time
			lastSG := shardGroups[len(shardGroups)-1]
			endTime = lastSG.EndTime
			logger.Info("using time range from shard groups",
				zap.Time("start", startTime),
				zap.Time("end", endTime),
				zap.Int("shard_count", len(shardGroups)))
		}

		// Override with config if only one is specified
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
	}

	// Use time window from config, default 168h (7 days)
	if task.Mapping.TimeWindow != "" {
		if tw, err := time.ParseDuration(task.Mapping.TimeWindow); err == nil {
			windowDuration = tw
		}
	}

	// Track progress across windows
	var lastCompletedWindowIdx int = -1
	resumeWindowIdx := -1
	lastCompletedBatchIdx := -1
	var lastTimestamp int64
	var totalMigratedRows int64
	// totalWrittenRows accumulates records actually written to the target
	// (after transform-side drops) across all batches for the completion
	// reconciliation against totalMigratedRows (source-reported row count).
	var totalWrittenRows int64
	if lastCheckpoint != nil && lastCheckpoint.Status == types.StatusInProgress {
		// ProcessedRows stores completed window count in batch mode. LastID stores
		// the last completed series-batch index within the current incomplete window.
		lastCompletedWindowIdx = int(lastCheckpoint.ProcessedRows) - 1
		if lastCheckpoint.LastID >= 0 && (lastCheckpoint.LastTimestamp != 0 || lastCheckpoint.TotalMigratedRows != 0) {
			resumeWindowIdx = int(lastCheckpoint.ProcessedRows)
			lastCompletedBatchIdx = int(lastCheckpoint.LastID)
		}
		lastTimestamp = lastCheckpoint.LastTimestamp
		totalMigratedRows = lastCheckpoint.TotalMigratedRows
		logger.Info("resuming batch mode from checkpoint",
			zap.String("task_id", task.ID),
			zap.Int("completed_window_count", int(lastCheckpoint.ProcessedRows)),
			zap.Int("last_completed_batch", lastCompletedBatchIdx+1))
	}

	// Split time range into windows
	windows := SplitTimeWindows(startTime, endTime, windowDuration)
	logger.Info("batch mode: time windows",
		zap.String("task_id", task.ID),
		zap.Int("window_count", len(windows)),
		zap.Duration("window_duration", windowDuration),
		zap.Time("start", startTime),
		zap.Time("end", endTime))

	queryCfg := &types.QueryConfig{
		BatchSize:         e.config.Migration.ChunkSize,
		MaxSeriesPerQuery: batchSize,
	}

	// Process each time window
	for windowIdx, window := range windows {
		if windowIdx <= lastCompletedWindowIdx {
			logger.Debug("skipping already completed window",
				zap.String("task_id", task.ID),
				zap.Int("window_index", windowIdx+1))
			continue
		}

		windowStart := window.Start
		windowEnd := window.End

		logger.Info("processing window",
			zap.String("task_id", task.ID),
			zap.Int("window", windowIdx+1),
			zap.Int("total_windows", len(windows)),
			zap.Time("window_start", windowStart),
			zap.Time("window_end", windowEnd))

		for i, batch := range batches {
			if windowIdx == resumeWindowIdx && i <= lastCompletedBatchIdx {
				logger.Debug("skipping already completed batch",
					zap.String("task_id", task.ID),
					zap.Int("window", windowIdx+1),
					zap.Int("batch_index", i+1))
				continue
			}

			logger.Debug("processing batch",
				zap.String("task_id", task.ID),
				zap.Int("window", windowIdx+1),
				zap.Int("batch_index", i+1),
				zap.Int("batch_size", len(batch)))

			batchCheckpoint := &types.Checkpoint{
				LastTimestamp: lastTimestamp,
			}

			cp, queryErr := sourceAdapter.QueryDataBatch(
				ctx,
				task.Mapping.SourceTable,
				batch,
				windowStart,
				windowEnd,
				batchCheckpoint,
				func(records []types.Record) error {
					if len(records) == 0 {
						return nil
					}
					written, err := e.processBatch(ctx, task.Mapping, records, targetAdapter)
					if err != nil {
						return err
					}
					totalWrittenRows += int64(written)
					return nil
				},
				queryCfg,
			)

			if queryErr != nil {
				return fmt.Errorf("window %d, batch %d/%d failed: %w", windowIdx+1, i+1, len(batches), queryErr)
			}

			if cp != nil {
				// Take the window-wide max rather than overwriting: different
				// series batches cover non-monotonic time ranges, so a later
				// batch can report a lower max timestamp than an earlier one.
				// Overwriting would regress the reported high-water mark.
				if cp.LastTimestamp > lastTimestamp {
					lastTimestamp = cp.LastTimestamp
				}
				totalMigratedRows += cp.ProcessedRows
			}

			batchCP := &types.Checkpoint{
				TaskID:            task.ID,
				SourceTable:       task.Mapping.SourceTable,
				LastID:            int64(i),
				LastTimestamp:     lastTimestamp,
				ProcessedRows:     int64(windowIdx),
				TotalMigratedRows: totalMigratedRows,
				Status:            types.StatusInProgress,
			}
			if lastCheckpoint != nil {
				batchCP.TaskName = lastCheckpoint.TaskName
				batchCP.TargetMeas = lastCheckpoint.TargetMeas
				batchCP.MappingConfig = lastCheckpoint.MappingConfig
			}
			if err := e.checkpointMgr.SaveCheckpoint(ctx, batchCP); err != nil {
				if e.config.Migration.FailOnCheckpointError {
					return fmt.Errorf("failed to save checkpoint: %w", err)
				}
				logger.Warn("checkpoint save failed; persisted cursor is stale — duplication/re-work risk on crash (safe for InfluxDB overwrite semantics)", zap.Error(err))
			}

			// Progress feedback every 10 batches
			if i > 0 && i%10 == 0 {
				logger.Info("batch progress",
					zap.Int("window", windowIdx+1),
					zap.Int("batch", i+1),
					zap.Int("total_batches", len(batches)),
					zap.Int64("total_migrated_rows", totalMigratedRows))
			}
		}

		// Save checkpoint after each window
		windowCP := &types.Checkpoint{
			TaskID:            task.ID,
			SourceTable:       task.Mapping.SourceTable,
			LastID:            -1,
			LastTimestamp:     lastTimestamp,
			ProcessedRows:     int64(windowIdx + 1),
			TotalMigratedRows: totalMigratedRows,
			Status:            types.StatusInProgress,
		}
		if lastCheckpoint != nil {
			windowCP.TaskName = lastCheckpoint.TaskName
			windowCP.TargetMeas = lastCheckpoint.TargetMeas
			windowCP.MappingConfig = lastCheckpoint.MappingConfig
		}
		if err := e.checkpointMgr.SaveCheckpoint(ctx, windowCP); err != nil {
			if e.config.Migration.FailOnCheckpointError {
				return fmt.Errorf("failed to save checkpoint: %w", err)
			}
			logger.Warn("checkpoint save failed; persisted cursor is stale — duplication/re-work risk on crash (safe for InfluxDB overwrite semantics)", zap.Error(err))
		}

		lastCompletedWindowIdx = windowIdx
	}

	logger.Info("completed batch mode task",
		zap.String("task_id", task.ID),
		zap.Int("total_windows", len(windows)),
		zap.Int("total_batches", len(batches)))

	if msg := reconcileRowCounts(totalMigratedRows, totalWrittenRows); msg != "" {
		logger.Warn(msg,
			zap.String("task_id", task.ID),
			zap.Int64("source_rows", totalMigratedRows),
			zap.Int64("written_rows", totalWrittenRows))
	}

	e.saveCompletedTaskCheckpoint(ctx, task, lastCheckpoint, lastTimestamp, int64(len(windows)), totalMigratedRows)

	return nil
}

func (e *MigrationEngine) runTaskShardGroupMode(ctx context.Context, task *MigrationTask) error {
	logger.Info("starting shard-group mode task",
		zap.String("task_id", task.ID),
		zap.String("source_table", task.Mapping.SourceTable),
		zap.String("target_measurement", task.Mapping.TargetMeasurement))

	startCP, err := e.startTaskCheckpoint(ctx, task)
	if err != nil {
		return err
	}
	if startCP != nil && startCP.Status == types.StatusCompleted {
		logger.Info("shard-group mode task already completed, skipping",
			zap.String("task_id", task.ID))
		return nil
	}

	sourceAdapterType, err := e.getSourceAdapterType(task.SourceAdapter)
	if err != nil {
		return err
	}
	sourceAdapter, err := e.sourceRegistry.GetSourceAdapter(sourceAdapterType)
	if err != nil {
		return fmt.Errorf("failed to get source adapter: %w", err)
	}

	sourceConfig := e.getSourceConfig(task.SourceAdapter)
	if err := sourceAdapter.Connect(ctx, sourceConfig); err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceAdapter.Disconnect(ctx)

	targetAdapterType, err := e.getTargetAdapterType(task.TargetAdapter)
	if err != nil {
		return err
	}
	targetAdapter, err := e.targetRegistry.GetTargetAdapter(targetAdapterType)
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
		e.saveCompletedTaskCheckpoint(ctx, task, startCP, 0, 0, 0)
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

	// If start was not configured (still 1970), discover from shard groups
	if task.Mapping.TimeRange.Start == "" && len(shardGroups) > 0 {
		sort.Slice(shardGroups, func(i, j int) bool {
			return shardGroups[i].StartTime.Before(shardGroups[j].StartTime)
		})
		queryStart = shardGroups[0].StartTime
	}

	// Validate time range
	if !queryEnd.After(queryStart) {
		return fmt.Errorf("invalid time range: end time %s must be after start time %s",
			task.Mapping.TimeRange.End, task.Mapping.TimeRange.Start)
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
		e.saveCompletedTaskCheckpoint(ctx, task, startCP, 0, 0, 0)
		return nil
	}

	logger.Info("relevant shard groups in time range",
		zap.String("task_id", task.ID),
		zap.Int("count", len(relevantGroups)))

	// Get parallelism setting
	parallelism := 1
	if e.config.InfluxToInflux.ShardGroupConfig != nil {
		parallelism = e.config.InfluxToInflux.ShardGroupConfig.ShardParallelism
	}
	if parallelism < 1 {
		parallelism = 1
	}

	logger.Info("shard-group parallelism",
		zap.String("task_id", task.ID),
		zap.Int("parallelism", parallelism))

	// For single parallelism, process serially (original behavior)
	if parallelism == 1 {
		for _, sg := range relevantGroups {
			if err := e.migrateShardGroup(ctx, task, sg, sourceAdapter, targetAdapter, queryStart, queryEnd); err != nil {
				return fmt.Errorf("shard group %d migration failed: %w", sg.ID, err)
			}
		}
	} else {
		// For parallelism > 1, use goroutines with semaphore
		semaphore := make(chan struct{}, parallelism)
		var wg sync.WaitGroup
		var firstErr error
		var errMu sync.Mutex

		for _, sg := range relevantGroups {
			wg.Add(1)
			go func(shardGroup *adapter.ShardGroup) {
				defer wg.Done()

				// Acquire semaphore slot
				select {
				case <-ctx.Done():
					errMu.Lock()
					if firstErr == nil {
						firstErr = ctx.Err()
					}
					errMu.Unlock()
					return
				case semaphore <- struct{}{}:
					// Got semaphore slot
				}
				defer func() { <-semaphore }()

				// Check context before processing
				select {
				case <-ctx.Done():
					errMu.Lock()
					if firstErr == nil {
						firstErr = ctx.Err()
					}
					errMu.Unlock()
					return
				default:
				}

				if err := e.migrateShardGroup(ctx, task, shardGroup, sourceAdapter, targetAdapter, queryStart, queryEnd); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("shard group %d migration failed: %w", shardGroup.ID, err)
					}
					errMu.Unlock()
				}
			}(sg)
		}

		wg.Wait()
		if firstErr != nil {
			return firstErr
		}
	}

	logger.Info("shard-group mode task completed",
		zap.String("task_id", task.ID),
		zap.Int("shard_groups_processed", len(relevantGroups)))

	// Aggregate TotalMigratedRows from all shard group checkpoints
	sgCheckpoints, err := e.checkpointMgr.ListShardGroupCheckpoints(ctx, task.ID)
	if err != nil {
		logger.Warn("failed to list shard group checkpoints", zap.Error(err))
	}

	var totalMigratedRows int64
	var lastTimestamp int64
	for _, sgCP := range sgCheckpoints {
		totalMigratedRows += sgCP.TotalProcessedRows
		if sgCP.LastTimestamp > lastTimestamp {
			lastTimestamp = sgCP.LastTimestamp
		}
	}

	lastCheckpoint, _ := e.checkpointMgr.LoadCheckpoint(ctx, task.ID, task.Mapping.SourceTable)
	// NOTE: Source-vs-written row reconciliation is NOT performed here for
	// shard-group mode. The written-row count is accumulated inside
	// migrateTimeWindow per window but is not propagated to this aggregator
	// (shard groups run concurrently and the per-window written count is not
	// persisted in the ShardGroupCheckpoint schema). Adding reconciliation
	// here requires either a TotalWrittenRows field on ShardGroupCheckpoint
	// (a SQLite store schema change, out of scope for this stream) or a
	// shared atomic counter across concurrent shard-group goroutines. This is
	// deferred to a later stream. Single-mode and batch-mode paths DO
	// reconcile; see reconcileRowCounts.
	e.saveCompletedTaskCheckpoint(ctx, task, lastCheckpoint, lastTimestamp, int64(len(relevantGroups)), totalMigratedRows)

	return nil
}

func (e *MigrationEngine) migrateShardGroup(ctx context.Context, task *MigrationTask, sg *adapter.ShardGroup, sourceAdapter adapter.SourceAdapter, targetAdapter adapter.TargetAdapter, queryStart, queryEnd time.Time) error {
	start, end := ShardGroupEffectiveTimeRange(sg, queryStart, queryEnd)

	logger.Info("migrating shard group",
		zap.Int("shard_id", sg.ID),
		zap.String("start", start.Format(time.RFC3339)),
		zap.String("end", end.Format(time.RFC3339)))

	// Get tag keys at shard group level (once per shard group, not per window)
	// Tag keys feed resolveTagKeySet, whose tagKeySet is used by
	// parseV1ValuesWithTagKeys / isTagColumn to distinguish tags from fields
	// when parsing rows read via executeV1ChunkedQuery.
	tagKeys, err := sourceAdapter.DiscoverTagKeys(ctx, task.Mapping.SourceTable)
	if err != nil {
		return fmt.Errorf("failed to discover tag keys for shard group %d: %w", sg.ID, err)
	} else {
		logger.Info("discovered tag keys for shard group",
			zap.Int("shard_id", sg.ID),
			zap.Int("tag_key_count", len(tagKeys)))
	}

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

	// Process each time window - pass tagKeys to all windows
	for windowIdx := 0; windowIdx < len(windows); windowIdx++ {
		window := windows[windowIdx]
		if err := e.migrateTimeWindow(ctx, task, sg, window, sourceAdapter, targetAdapter, tagKeys); err != nil {
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

func (e *MigrationEngine) migrateTimeWindow(ctx context.Context, task *MigrationTask, sg *adapter.ShardGroup, window TimeWindow, sourceAdapter adapter.SourceAdapter, targetAdapter adapter.TargetAdapter, tagKeys []string) error {
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
	batchSize := e.shardGroupSeriesBatchSize()
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
				// processBatch returns the written count, but migrateTimeWindow
				// does not currently aggregate it across windows/shard groups
				// (that requires persisting written rows in the shard-group
				// checkpoint, a schema change deferred to a later stream). The
				// count is discarded here; reconciliation for shard-group mode
				// is documented at the task-completion site.
				_, err := e.processBatch(ctx, task.Mapping, records, targetAdapter)
				return err
			},
			&types.QueryConfig{BatchSize: e.config.Migration.ChunkSize, TagKeys: tagKeys},
		)
		if err != nil {
			return fmt.Errorf("batch %d failed: %w", batchIdx, err)
		}

		if batchCheckpoint != nil {
			// Take the window-wide max rather than overwriting: different
			// series batches cover non-monotonic time ranges, so a later
			// batch can report a lower max timestamp than an earlier one.
			// Overwriting would regress the reported high-water mark.
			if batchCheckpoint.LastTimestamp > lastTimestamp {
				lastTimestamp = batchCheckpoint.LastTimestamp
			}
			totalProcessed += batchCheckpoint.ProcessedRows
		}

		sgCP := &types.ShardGroupCheckpoint{
			TaskID:             task.ID,
			ShardGroupID:       fmt.Sprintf("%d", sg.ID),
			WindowStart:        window.Start.UnixNano(),
			WindowEnd:          window.End.UnixNano(),
			LastCompletedBatch: batchIdx,
			LastTimestamp:      lastTimestamp,
			TotalProcessedRows: totalProcessed,
			Status:             types.StatusInProgress,
		}
		if err := e.checkpointMgr.SaveShardGroupCheckpoint(ctx, sgCP); err != nil {
			if e.config.Migration.FailOnCheckpointError {
				return fmt.Errorf("failed to save shard group checkpoint: %w", err)
			}
			logger.Warn("checkpoint save failed; persisted cursor is stale — duplication/re-work risk on crash (safe for InfluxDB overwrite semantics)", zap.Error(err))
		}
	}

	// Mark window as completed after all batches processed successfully
	sgCP := &types.ShardGroupCheckpoint{
		TaskID:             task.ID,
		ShardGroupID:       fmt.Sprintf("%d", sg.ID),
		WindowStart:        window.Start.UnixNano(),
		WindowEnd:          window.End.UnixNano(),
		LastCompletedBatch: len(batches) - 1,
		LastTimestamp:      lastTimestamp,
		TotalProcessedRows: totalProcessed,
		Status:             types.StatusCompleted,
	}
	if err := e.checkpointMgr.SaveShardGroupCheckpoint(ctx, sgCP); err != nil {
		if e.config.Migration.FailOnCheckpointError {
			return fmt.Errorf("failed to save window completed checkpoint: %w", err)
		}
		// The window's data is already written to the target; a failed
		// completion checkpoint only means the persisted cursor is stale, so
		// on resume this window will be re-migrated. Under InfluxDB overwrite
		// semantics that is safe duplication/re-work, not data loss.
		logger.Warn("checkpoint save failed; persisted cursor is stale — duplication/re-work risk on crash (safe for InfluxDB overwrite semantics)", zap.Error(err))
	}

	return nil
}

// queryWithTimeRange iterates over the overall time range in chunks of
// windowDuration, calling sourceAdapter.QueryData per chunk. writtenRows, when
// non-nil, accumulates the count of records actually written to the target
// (after transform-side drops) so the caller can reconcile source-read vs
// written-to-target rows at task completion.
func (e *MigrationEngine) queryWithTimeRange(ctx context.Context, sourceAdapter adapter.SourceAdapter, table string, mapping *types.MappingConfig, lastCp *types.Checkpoint, targetAdapter adapter.TargetAdapter, taskID string, queryCfg *types.QueryConfig, writtenRows *int64) (*types.Checkpoint, error) {
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

	// Use a local copy to avoid mutating the original mapping
	taskMapping := *mapping
	if taskMapping.TimeWindow == "" {
		taskMapping.TimeWindow = "168h"
	}

	windowDuration, err := time.ParseDuration(taskMapping.TimeWindow)
	if err != nil {
		logger.Warn("invalid time window, using default 168h",
			zap.String("time_window", taskMapping.TimeWindow),
			zap.Error(err))
		windowDuration = 168 * time.Hour
	}

	currentCp := lastCp
	totalProcessed := int64(0)
	var lastTimestamp int64
	var resumeAfter time.Time
	if lastCp != nil {
		totalProcessed = lastCp.ProcessedRows
		lastTimestamp = lastCp.LastTimestamp
		if lastCp.LastTimestamp != 0 {
			resumeAfter = time.Unix(0, lastCp.LastTimestamp)
		}
	}

	for windowStart := startTime; windowStart.Before(endTime); windowStart = windowStart.Add(windowDuration) {
		windowEnd := windowStart.Add(windowDuration)
		if windowEnd.After(endTime) {
			windowEnd = endTime
		}
		if !resumeAfter.IsZero() && !windowEnd.After(resumeAfter) {
			logger.Debug("skipping time-range window completed by checkpoint",
				zap.String("task_id", taskID),
				zap.Time("window_start", windowStart),
				zap.Time("window_end", windowEnd),
				zap.Time("checkpoint_time", resumeAfter))
			continue
		}

		windowMapping := taskMapping
		windowMapping.TimeRange = types.TimeRange{
			Start: windowStart.Format(time.RFC3339),
			End:   windowEnd.Format(time.RFC3339),
		}

		windowQueryCfg := queryConfigWithTimeRange(queryCfg, windowStart, windowEnd)

		cp, err := sourceAdapter.QueryData(ctx, table, currentCp, func(records []types.Record) error {
			if len(records) == 0 {
				return nil
			}

			lastRecord := records[len(records)-1]
			saveTimestamp := lastRecord.Time
			saveProcessed := totalProcessed + int64(len(records))

			written, err := e.processBatch(ctx, &windowMapping, records, targetAdapter)
			if err != nil {
				return err
			}
			if writtenRows != nil {
				*writtenRows += int64(written)
			}

			// Build checkpoint with updated progress fields, preserving existing data
			windowCP := &types.Checkpoint{
				TaskID:        taskID,
				SourceTable:   table,
				LastID:        0,
				LastTimestamp: saveTimestamp,
				ProcessedRows: saveProcessed,
				Status:        types.StatusInProgress,
			}
			if currentCp != nil {
				windowCP.TaskName = currentCp.TaskName
				windowCP.TargetMeas = currentCp.TargetMeas
				windowCP.MappingConfig = currentCp.MappingConfig
			}
			if err := e.checkpointMgr.SaveCheckpoint(ctx, windowCP); err != nil {
				if e.config.Migration.FailOnCheckpointError {
					return fmt.Errorf("failed to save checkpoint: %w", err)
				}
				logger.Warn("checkpoint save failed; persisted cursor is stale — duplication/re-work risk on crash (safe for InfluxDB overwrite semantics)", zap.Error(err))
			}

			totalProcessed = saveProcessed
			lastTimestamp = saveTimestamp
			return nil
		}, windowQueryCfg)

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
		// Note: We create timer fresh each iteration to avoid timer accumulation
		timer := time.NewTimer(e.config.Migration.ChunkInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
			// Timer fired normally, continue to next iteration
		}
		// Explicitly stop timer after use to release resources
		timer.Stop()
	}

	return &types.Checkpoint{
		ProcessedRows: totalProcessed,
		LastTimestamp: lastTimestamp,
	}, nil
}

func (e *MigrationEngine) startTaskCheckpoint(ctx context.Context, task *MigrationTask) (*types.Checkpoint, error) {
	cp, err := e.checkpointMgr.LoadCheckpoint(ctx, task.ID, task.Mapping.SourceTable)
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}
	if cp != nil && cp.Status == types.StatusCompleted {
		return cp, nil
	}
	if cp == nil {
		cp = &types.Checkpoint{
			TaskID:        task.ID,
			TaskName:      task.ID,
			SourceTable:   task.Mapping.SourceTable,
			TargetMeas:    task.Mapping.TargetMeasurement,
			Status:        types.StatusInProgress,
			MappingConfig: *task.Mapping,
		}
		if err := e.checkpointMgr.SaveCheckpoint(ctx, cp); err != nil {
			return nil, fmt.Errorf("failed to create in-progress checkpoint: %w", err)
		}
		return cp, nil
	}
	if err := e.checkpointMgr.MarkTaskInProgress(ctx, task.ID, task.Mapping.SourceTable); err != nil {
		return nil, fmt.Errorf("failed to mark task in progress: %w", err)
	}
	cp.Status = types.StatusInProgress
	return cp, nil
}

// reconcileRowCounts builds a warning message comparing the number of records
// read from the source against the number actually written to the target. It
// returns an empty string when the counts match (or are both zero), so callers
// can log only on a real divergence.
//
// LIMITATION: This reconciliation detects transform-side drops (records that
// became fieldless after FilterNulls/schema-transform and were skipped). It
// CANNOT detect target-side partial-write drops such as InfluxDB's HTTP 204
// "partial write" response that silently drops some points — the target
// adapter currently returns nil for such responses without reporting how many
// points were accepted. Detecting those requires the target adapter to return
// a typed error or an accepted-count (deferred to the B4/engine-typed-error
// stream). Persistence of the source-row count in the checkpoint is also
// deferred: the Checkpoint struct and its SQLite store schema would need a new
// TotalSourceRows field, which is out of scope for this stream.
func reconcileRowCounts(sourceRows, writtenRows int64) string {
	if sourceRows == writtenRows {
		return ""
	}
	delta := sourceRows - writtenRows
	if delta < 0 {
		delta = -delta
	}
	return fmt.Sprintf("row count mismatch: source=%d target=%d, delta=%d — possible transform-side drops; verify target", sourceRows, writtenRows, delta)
}

func (e *MigrationEngine) saveCompletedTaskCheckpoint(ctx context.Context, task *MigrationTask, existingCP *types.Checkpoint, lastTimestamp, processedRows, totalMigratedRows int64) {
	finalCP := &types.Checkpoint{
		TaskID:            task.ID,
		SourceTable:       task.Mapping.SourceTable,
		LastID:            0,
		LastTimestamp:     lastTimestamp,
		ProcessedRows:     processedRows,
		TotalMigratedRows: totalMigratedRows,
		Status:            types.StatusCompleted,
	}
	if existingCP != nil {
		finalCP.TaskName = existingCP.TaskName
		finalCP.TargetMeas = existingCP.TargetMeas
		finalCP.MappingConfig = existingCP.MappingConfig
	} else {
		finalCP.TaskName = task.ID
		finalCP.TargetMeas = task.Mapping.TargetMeasurement
		finalCP.MappingConfig = *task.Mapping
	}
	if err := e.checkpointMgr.SaveCheckpoint(ctx, finalCP); err != nil {
		logger.Warn("failed to save final checkpoint", zap.Error(err))
	}
	if err := e.checkpointMgr.MarkTaskCompleted(ctx, task.ID, task.Mapping.SourceTable); err != nil {
		logger.Warn("failed to mark task completed", zap.Error(err))
	}
}

func (e *MigrationEngine) shardGroupSeriesBatchSize() int {
	if e.config.InfluxToInflux.ShardGroupConfig != nil && e.config.InfluxToInflux.ShardGroupConfig.SeriesBatchSize > 0 {
		return e.config.InfluxToInflux.ShardGroupConfig.SeriesBatchSize
	}
	if e.config.InfluxToInflux.MaxSeriesPerQuery > 0 {
		return e.config.InfluxToInflux.MaxSeriesPerQuery
	}
	return 100
}

func queryConfigWithTimeRange(base *types.QueryConfig, start, end time.Time) *types.QueryConfig {
	cfg := &types.QueryConfig{}
	if base != nil {
		copied := *base
		cfg = &copied
	}
	cfg.StartTime = start
	cfg.EndTime = end
	return cfg
}

// processBatch filters, transforms, and writes a batch of records to the
// target. It returns the number of records actually written (after dropping
// unwritable records) alongside any write error. A record with no fields
// after filtering/transform cannot be encoded as valid InfluxDB line protocol
// (a point requires at least one field), so such records are dropped here and
// counted as a transform-side drop. The returned count lets callers reconcile
// source-read rows against written-to-target rows.
func (e *MigrationEngine) processBatch(ctx context.Context, mapping *types.MappingConfig, records []types.Record, targetAdapter adapter.TargetAdapter) (int, error) {
	if e.rateLimiter != nil {
		if err := e.rateLimiter.WaitContext(ctx, len(records)); err != nil {
			return 0, fmt.Errorf("rate limit wait cancelled: %w", err)
		}
	}

	transformed := make([]types.Record, 0, len(records))
	dropped := 0
	for i := range records {
		filtered := e.transformer.FilterNulls(&records[i])

		// Check for tag/field name collisions (InfluxDB allows this but it causes query issues)
		warnings := e.transformer.ValidateRecord(filtered)
		for _, warning := range warnings {
			logger.Warn("record has potential data ambiguity",
				zap.String("warning", warning),
				zap.String("measurement", mapping.TargetMeasurement))
		}

		// A record with no fields cannot be written to InfluxDB: line protocol
		// requires at least one field per point. Drop it and account for the
		// loss so the source/written row reconciliation can surface the delta.
		if len(filtered.Fields) == 0 {
			dropped++
			continue
		}

		transformed = append(transformed, *filtered)
	}

	if mapping != nil && (len(mapping.Schema.Fields) > 0 || len(mapping.Schema.Tags) > 0) {
		schemaTransformed := make([]types.Record, 0, len(transformed))
		schemaDropped := 0
		for i := range transformed {
			result := e.transformer.Transform(&transformed[i], mapping)
			// The schema transform can also produce a fieldless record (e.g.
			// when none of the source fields map to a configured target
			// field). Drop those for the same line-protocol reason.
			if len(result.Fields) == 0 {
				schemaDropped++
				continue
			}
			schemaTransformed = append(schemaTransformed, *result)
		}
		dropped += schemaDropped
		transformed = schemaTransformed
	}

	if dropped > 0 {
		logger.Warn("dropped records with no fields during transform",
			zap.Int("input_records", len(records)),
			zap.Int("output_records", len(transformed)),
			zap.Int("dropped_records", dropped),
			zap.String("measurement", mapping.TargetMeasurement))
	}

	logger.Debug("processing batch",
		zap.Int("input_records", len(records)),
		zap.Int("output_records", len(transformed)))

	if err := e.writeWithRetry(ctx, mapping.TargetMeasurement, transformed, targetAdapter); err != nil {
		return len(transformed), err
	}
	return len(transformed), nil
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
	baseDelay := e.config.Retry.InitialDelay
	if baseDelay == 0 {
		baseDelay = 1 * time.Second
	}
	maxDelay := e.config.Retry.MaxDelay
	if maxDelay == 0 {
		maxDelay = 60 * time.Second
	}
	backoffMultiplier := e.config.Retry.BackoffMultiplier
	if backoffMultiplier == 0 {
		backoffMultiplier = 2.0
	}

	// Initialize delay to base delay
	delay := baseDelay

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := targetAdapter.WriteBatch(ctx, measurement, records)
		if err == nil {
			return nil
		}

		lastErr = err

		// Permanent (non-retryable) errors — e.g. 4xx auth/permission/malformed
		// line protocol — must not be retried. Retrying would waste time and
		// mask the underlying config bug as a transient failure. Surface the
		// error immediately so the operator can fix the configuration.
		if !isRetryableWriteError(err) {
			logger.Warn("write batch failed with permanent error, not retrying",
				zap.Int("attempt", attempt),
				zap.Int("max_attempts", maxAttempts),
				zap.Error(err))
			return fmt.Errorf("write batch failed with non-retryable error: %w", err)
		}

		logger.Warn("write batch failed, will retry",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", maxAttempts),
			zap.Duration("delay", delay),
			zap.Error(err))

		if attempt < maxAttempts {
			// Calculate jittered delay: base_delay * (0.5 + random[0,1]) * multiplier^(attempt-1)
			// This gives range [0.5x, 1.5x] of exponential delay
			jitterFactor := 0.5 + rand.Float64() // 0.5 to 1.5
			sleepDuration := time.Duration(float64(delay) * jitterFactor)

			// Use timer to allow early cancellation
			timer := time.NewTimer(sleepDuration)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				// Timer fired normally, proceed with retry
			}

			// Calculate new base delay for next iteration (without jitter)
			delay = time.Duration(float64(delay) * backoffMultiplier)
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}

	return fmt.Errorf("write batch failed after %d attempts: %w", maxAttempts, lastErr)
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

func (e *MigrationEngine) getSourceAdapterType(name string) (string, error) {
	for _, src := range e.config.Sources {
		if src.Name == name {
			return src.Type, nil
		}
	}
	return name, nil
}

func (e *MigrationEngine) getTargetAdapterType(name string) (string, error) {
	for _, tgt := range e.config.Targets {
		if tgt.Name == name {
			return tgt.Type, nil
		}
	}
	return name, nil
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
			"url":              src.InfluxDB.URL,
			"token":            src.InfluxDB.Token,
			"org":              src.InfluxDB.Org,
			"bucket":           src.InfluxDB.Bucket,
			"version":          src.InfluxDB.Version,
			"username":         src.InfluxDB.Username,
			"password":         src.InfluxDB.Password,
			"retention_policy": src.InfluxDB.RetentionPolicy,
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
	case "influxdb-v1":
		influxCfg := map[string]interface{}{
			"url":      tgt.InfluxDB.URL,
			"version":  tgt.InfluxDB.Version,
			"database": tgt.Database,
		}
		// V1 target uses basic_auth with username/password
		if tgt.User != "" {
			influxCfg["basic_auth"] = map[string]interface{}{
				"username": tgt.User,
				"password": tgt.Password,
			}
		}
		// Include retention_policy if specified
		if tgt.InfluxDB.RetentionPolicy != "" {
			influxCfg["retention_policy"] = tgt.InfluxDB.RetentionPolicy
		}
		m["influxdb"] = influxCfg
	case "influxdb-v2":
		influxCfg := map[string]interface{}{
			"url":     tgt.InfluxDB.URL,
			"version": tgt.InfluxDB.Version,
			"token":   tgt.InfluxDB.Token,
			"org":     tgt.InfluxDB.Org,
			"bucket":  tgt.InfluxDB.Bucket,
		}
		m["influxdb"] = influxCfg
	case "mysql":
		m["mysql"] = map[string]interface{}{
			"host":     tgt.Host,
			"port":     tgt.Port,
			"user":     tgt.User,
			"password": tgt.Password,
			"database": tgt.Database,
			"charset":  tgt.MySQL.Charset,
		}
	case "tdengine":
		m["tdengine"] = map[string]interface{}{
			"host":     tgt.Host,
			"port":     tgt.Port,
			"user":     tgt.User,
			"password": tgt.Password,
			"database": tgt.Database,
			"version":  tgt.TDengine.Version,
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

func (e *MigrationEngine) Resume(ctx context.Context) (err error) {
	// First, mark all in-progress tasks as interrupted to prevent race with running workers.
	// This ensures that any task still being processed by workers will be properly
	// abandoned rather than having both the worker and Resume() operate on it.
	e.MarkInProgressAsInterrupted(ctx)

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
	e.resetWorkerErrors()
	e.startWorkers(ctx)
	defer func() {
		if workerErr := e.finishWorkers(); err == nil && workerErr != nil {
			err = workerErr
		}
	}()

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

		if err := e.enqueueTask(ctx, task); err != nil {
			return fmt.Errorf("failed to enqueue failed task: %w", err)
		}
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

		if err := e.enqueueTask(ctx, task); err != nil {
			return fmt.Errorf("failed to enqueue interrupted task: %w", err)
		}
	}
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
