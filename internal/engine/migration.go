package engine

import (
	"context"
	"fmt"
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
		for _, mapping := range taskConfig.Mappings {
			mappingPtr := mapping
			task := &MigrationTask{
				ID:            fmt.Sprintf("%s-%s", taskConfig.Name, mapping.TargetMeasurement),
				SourceAdapter: taskConfig.Source,
				TargetAdapter: taskConfig.Target,
				Mapping:       &mappingPtr,
				Status:        types.StatusPending,
			}

			cp := &types.Checkpoint{
				TaskID:      task.ID,
				TaskName:    taskConfig.Name,
				SourceTable: getSourceTable(&mappingPtr),
				TargetMeas:  mappingPtr.TargetMeasurement,
				Status:      types.StatusPending,
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

func (e *MigrationEngine) worker(ctx context.Context, workerID int) {
	defer e.wg.Done()

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
	if existingCP != nil && existingCP.Status == types.StatusInProgress {
		lastCheckpoint = existingCP
	}

	sourceTable := getSourceTable(task.Mapping)

	switch task.Mapping.TimeRange.Start {
	case "":
		_, err = sourceAdapter.QueryData(ctx, sourceTable, lastCheckpoint, func(records []types.Record) error {
			return e.processBatch(ctx, task.Mapping, records, targetAdapter)
		})
	default:
		_, err = e.queryWithTimeRange(ctx, sourceAdapter, sourceTable, task.Mapping, lastCheckpoint, targetAdapter)
	}

	if err != nil {
		e.checkpointMgr.MarkTaskFailed(ctx, task.ID, task.Mapping.SourceTable, err.Error())
		return err
	}

	e.checkpointMgr.MarkTaskCompleted(ctx, task.ID, task.Mapping.SourceTable)
	logger.Info("task completed successfully",
		zap.String("task_id", task.ID))

	return nil
}

func (e *MigrationEngine) queryWithTimeRange(ctx context.Context, sourceAdapter adapter.SourceAdapter, table string, mapping *types.MappingConfig, lastCp *types.Checkpoint, targetAdapter adapter.TargetAdapter) (*types.Checkpoint, error) {
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
			return e.processBatch(ctx, &windowMapping, records, targetAdapter)
		})

		if err != nil {
			return nil, err
		}

		if cp != nil {
			totalProcessed += cp.ProcessedRows
			currentCp = cp
		}

		time.Sleep(e.config.Migration.ChunkInterval)
	}

	return &types.Checkpoint{
		ProcessedRows: totalProcessed,
		LastTimestamp: currentCp.LastTimestamp,
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

	return targetAdapter.WriteBatch(ctx, mapping.TargetMeasurement, transformed)
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

func (e *MigrationEngine) Resume(ctx context.Context) error {
	failedTasks, err := e.checkpointMgr.GetFailedTasks(ctx)
	if err != nil {
		return fmt.Errorf("failed to get failed tasks: %w", err)
	}

	for _, cp := range failedTasks {
		logger.Info("resuming task",
			zap.String("task_id", cp.TaskID),
			zap.String("source_table", cp.SourceTable))

		task := &MigrationTask{
			ID:      cp.TaskID,
			Mapping: &types.MappingConfig{SourceTable: cp.SourceTable, TargetMeasurement: cp.TargetMeas},
			Status:  types.StatusPending,
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
