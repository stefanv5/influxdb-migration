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

	for task := range e.taskQueue {
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

func (e *MigrationEngine) runTask(ctx context.Context, task *MigrationTask) error {
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

	e.checkpointMgr.MarkTaskInProgress(ctx, task.ID, task.Mapping.SourceTable)

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

	switch task.Mapping.TimeRange.Start {
	case "":
		checkpoint, queryErr := sourceAdapter.QueryData(ctx, sourceTable, lastCheckpoint, func(records []types.Record) error {
			err := e.processBatch(ctx, task.Mapping, records, targetAdapter)
			if err != nil {
				return err
			}
			if len(records) > 0 {
				lastRecord := records[len(records)-1]
				totalProcessed += int64(len(records))
				lastTimestamp = lastRecord.Time
				e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
					0, lastTimestamp, totalProcessed, types.StatusInProgress)
			}
			return nil
		})
		if queryErr != nil {
			e.checkpointMgr.MarkTaskFailed(ctx, task.ID, task.Mapping.SourceTable, queryErr.Error())
			return queryErr
		}
		if checkpoint != nil {
			totalProcessed = checkpoint.ProcessedRows
			lastTimestamp = checkpoint.LastTimestamp
		}
	default:
		checkpoint, queryErr := e.queryWithTimeRange(ctx, sourceAdapter, sourceTable, task.Mapping, lastCheckpoint, targetAdapter, task.ID)
		if queryErr != nil {
			e.checkpointMgr.MarkTaskFailed(ctx, task.ID, task.Mapping.SourceTable, queryErr.Error())
			return queryErr
		}
		if checkpoint != nil {
			totalProcessed = checkpoint.ProcessedRows
			lastTimestamp = checkpoint.LastTimestamp
		}
	}

	e.checkpointMgr.SaveCheckpoint(ctx, task.ID, task.Mapping.SourceTable,
		0, lastTimestamp, totalProcessed, types.StatusCompleted)
	e.checkpointMgr.MarkTaskCompleted(ctx, task.ID, task.Mapping.SourceTable)
	logger.Info("task completed successfully",
		zap.String("task_id", task.ID))

	return nil
}

func (e *MigrationEngine) queryWithTimeRange(ctx context.Context, sourceAdapter adapter.SourceAdapter, table string, mapping *types.MappingConfig, lastCp *types.Checkpoint, targetAdapter adapter.TargetAdapter, taskID string) (*types.Checkpoint, error) {
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
			err := e.processBatch(ctx, &windowMapping, records, targetAdapter)
			if err != nil {
				return err
			}
			if len(records) > 0 {
				lastRecord := records[len(records)-1]
				totalProcessed += int64(len(records))
				lastTimestamp = lastRecord.Time
				e.checkpointMgr.SaveCheckpoint(ctx, taskID, table,
					0, lastTimestamp, totalProcessed, types.StatusInProgress)
			}
			return nil
		})

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

		time.Sleep(e.config.Migration.ChunkInterval)
	}

	return &types.Checkpoint{
		ProcessedRows: totalProcessed,
		LastTimestamp: lastTimestamp,
	}, nil
}

func (e *MigrationEngine) processBatch(ctx context.Context, mapping *types.MappingConfig, records []types.Record, targetAdapter adapter.TargetAdapter) error {
	if e.rateLimiter != nil {
		e.rateLimiter.Wait(len(records))
	}

	transformed := make([]types.Record, 0, len(records))
	for _, record := range records {
		filtered := filterNilValues(&record)
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
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}

			delay *= time.Duration(e.config.Retry.BackoffMultiplier)
			if delay > maxDelay {
				delay = maxDelay
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
