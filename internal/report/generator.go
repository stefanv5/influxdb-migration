package report

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/migration-tools/influx-migrator/internal/checkpoint"
	"github.com/migration-tools/influx-migrator/internal/logger"
	"github.com/migration-tools/influx-migrator/pkg/types"
	"go.uber.org/zap"
)

type Generator struct {
	checkpointMgr *checkpoint.Manager
	reportDir     string
}

type Report struct {
	MigrationID  string            `json:"migration_id"`
	TaskName     string            `json:"task_name"`
	Status       string            `json:"status"`
	StartedAt    time.Time         `json:"started_at"`
	CompletedAt  time.Time         `json:"completed_at"`
	DurationSecs int64             `json:"duration_seconds"`
	Summary      Summary           `json:"summary"`
	Errors       []ErrorEntry      `json:"errors"`
	Checkpoints  []CheckpointEntry `json:"checkpoints"`
}

type Summary struct {
	Source           string `json:"source"`
	Target           string `json:"target"`
	SourceDB         string `json:"source_db"`
	TargetDB         string `json:"target_db"`
	SourceRP         string `json:"source_rp,omitempty"`
	TargetRP         string `json:"target_rp,omitempty"`
	SourceType       string `json:"source_type"`
	TargetType       string `json:"target_type"`
	TotalRows        int64  `json:"total_rows"`
	TransferredRows  int64  `json:"transferred_rows"`
	FailedRows       int64  `json:"failed_rows"`
	BatchCount       int    `json:"batch_count"`
	MeasurementCount int    `json:"measurement_count"`
}

type ErrorEntry struct {
	BatchID     int       `json:"batch_id"`
	Table       string    `json:"table"`
	Measurement string    `json:"measurement"`
	Timestamp   time.Time `json:"timestamp"`
	Error       string    `json:"error"`
	Retryable   bool      `json:"retryable"`
}

type CheckpointEntry struct {
	Table         string    `json:"table"`
	Measurement   string    `json:"measurement"`
	TargetMeas    string    `json:"target_measurement"`
	LastID        int64     `json:"last_id"`
	LastTimestamp time.Time `json:"last_timestamp"`
	ProcessedRows int64     `json:"processed_rows"`
	Status        string    `json:"status"`
	SavedAt       time.Time `json:"saved_at"`
}

func NewGenerator(checkpointMgr *checkpoint.Manager, reportDir string) *Generator {
	if err := os.MkdirAll(reportDir, 0755); err != nil {
		logger.Warn("failed to create report dir", zap.Error(err))
	}
	return &Generator{
		checkpointMgr: checkpointMgr,
		reportDir:     reportDir,
	}
}

func (g *Generator) Generate(ctx context.Context, migrationID, taskName string) (*Report, error) {
	return g.GenerateWithDetails(ctx, migrationID, taskName, "", "", "", "", "", "", "", "")
}

func (g *Generator) GenerateWithDetails(ctx context.Context, migrationID, taskName, sourceName, targetName, sourceDB, targetDB, sourceRP, targetRP, sourceType, targetType string) (*Report, error) {
	checkpoints, err := g.checkpointMgr.ListCheckpoints(ctx, migrationID)
	if err != nil {
		return nil, fmt.Errorf("failed to list checkpoints: %w", err)
	}

	var totalRows, transferredRows, failedRows int64
	var latestTime time.Time
	status := "completed"
	measurementSet := make(map[string]bool)

	for _, cp := range checkpoints {
		totalRows += cp.ProcessedRows
		measurementSet[cp.TargetMeas] = true
		if cp.Status == types.StatusCompleted {
			transferredRows += cp.ProcessedRows
		} else if cp.Status == types.StatusFailed {
			failedRows += cp.ProcessedRows
			status = "failed"
		}
		if cp.UpdatedAt.After(latestTime) {
			latestTime = cp.UpdatedAt
		}
	}

	var startedAt time.Time
	if len(checkpoints) > 0 {
		startedAt = checkpoints[0].CreatedAt
	}

	report := &Report{
		MigrationID:  migrationID,
		TaskName:     taskName,
		Status:       status,
		StartedAt:    startedAt,
		CompletedAt:  latestTime,
		DurationSecs: int64(latestTime.Sub(startedAt).Seconds()),
		Summary: Summary{
			Source:           sourceName,
			Target:           targetName,
			SourceDB:         sourceDB,
			TargetDB:         targetDB,
			SourceRP:         sourceRP,
			TargetRP:         targetRP,
			SourceType:       sourceType,
			TargetType:       targetType,
			TotalRows:        totalRows,
			TransferredRows:  transferredRows,
			FailedRows:       failedRows,
			BatchCount:       len(checkpoints),
			MeasurementCount: len(measurementSet),
		},
		Checkpoints: make([]CheckpointEntry, len(checkpoints)),
	}

	for i, cp := range checkpoints {
		report.Checkpoints[i] = CheckpointEntry{
			Table:         cp.SourceTable,
			Measurement:   cp.SourceTable,
			TargetMeas:    cp.TargetMeas,
			LastID:        cp.LastID,
			LastTimestamp: cp.LastTimestamp,
			ProcessedRows: cp.ProcessedRows,
			Status:        string(cp.Status),
			SavedAt:       cp.UpdatedAt,
		}
	}

	return report, nil
}

func (g *Generator) SaveJSON(report *Report) error {
	if g.reportDir == "" {
		return fmt.Errorf("report dir not set")
	}

	filename := fmt.Sprintf("%s_%s.json", report.MigrationID, time.Now().Format("20060102_150405"))
	path := filepath.Join(g.reportDir, filename)

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	return os.WriteFile(path, data, 0644)
}

func (g *Generator) SaveMarkdown(report *Report) error {
	if g.reportDir == "" {
		return fmt.Errorf("report dir not set")
	}

	filename := fmt.Sprintf("%s_%s.md", report.MigrationID, time.Now().Format("20060102_150405"))
	path := filepath.Join(g.reportDir, filename)

	content := fmt.Sprintf(`# Migration Report

## Migration Info

| Field | Value |
|-------|-------|
| Migration ID | %s |
| Task Name | %s |
| Status | %s |
| Started At | %s |
| Completed At | %s |
| Duration | %d seconds |

## Source & Target

| Component | Type | Database | RP |
|-----------|------|----------|-----|
| Source | %s | %s | %s |
| Target | %s | %s | %s |

## Data Summary

| Metric | Value |
|--------|-------|
| Total Rows | %s |
| Transferred Rows | %s |
| Failed Rows | %s |
| Measurements | %d |
| Batches | %d |

## Migration Details

| Source Table | Target Measurement | Status | Rows Processed | Last Timestamp |
|--------------|-------------------|--------|----------------|---------------|
%s
`, report.MigrationID, report.TaskName, report.Status,
		report.StartedAt.Format(time.RFC3339), report.CompletedAt.Format(time.RFC3339), report.DurationSecs,
		report.Summary.SourceType, report.Summary.SourceDB, report.Summary.SourceRP,
		report.Summary.TargetType, report.Summary.TargetDB, report.Summary.TargetRP,
		fmt.Sprintf("%d", report.Summary.TotalRows), fmt.Sprintf("%d", report.Summary.TransferredRows), fmt.Sprintf("%d", report.Summary.FailedRows), report.Summary.MeasurementCount, report.Summary.BatchCount,
		g.formatCheckpointsTable(report.Checkpoints))

	return os.WriteFile(path, []byte(content), 0644)
}

func (g *Generator) formatCheckpointsTable(checkpoints []CheckpointEntry) string {
	var lines []string
	for _, cp := range checkpoints {
		ts := ""
		if !cp.LastTimestamp.IsZero() {
			ts = cp.LastTimestamp.Format(time.RFC3339)
		}
		lines = append(lines, fmt.Sprintf("| %s | %s | %s | %d | %s |",
			cp.Table, cp.TargetMeas, cp.Status, cp.ProcessedRows, ts))
	}
	return strings.Join(lines, "\n")
}

func (g *Generator) SaveHTML(report *Report) error {
	if g.reportDir == "" {
		return fmt.Errorf("report dir not set")
	}

	filename := fmt.Sprintf("%s_%s.html", report.MigrationID, time.Now().Format("20060102_150405"))
	path := filepath.Join(g.reportDir, filename)

	content := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>Migration Report - %s</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #333; }
        table { border-collapse: collapse; width: 100%%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #4CAF50; color: white; }
        .status-completed { color: green; font-weight: bold; }
        .status-failed { color: red; font-weight: bold; }
    </style>
</head>
<body>
    <h1>Migration Report</h1>
    <h2>Summary</h2>
    <table>
        <tr><th>Field</th><th>Value</th></tr>
        <tr><td>Migration ID</td><td>%s</td></tr>
        <tr><td>Task Name</td><td>%s</td></tr>
        <tr><td>Status</td><td class="status-%s">%s</td></tr>
        <tr><td>Started At</td><td>%s</td></tr>
        <tr><td>Completed At</td><td>%s</td></tr>
        <tr><td>Duration</td><td>%d seconds</td></tr>
    </table>
    <h2>Data Summary</h2>
    <table>
        <tr><th>Metric</th><th>Value</th></tr>
        <tr><td>Total Rows</td><td>%d</td></tr>
        <tr><td>Transferred Rows</td><td>%d</td></tr>
        <tr><td>Failed Rows</td><td>%d</td></tr>
    </table>
</body>
</html>`, report.MigrationID,
		report.MigrationID, report.TaskName, report.Status, report.Status,
		report.StartedAt.Format(time.RFC3339), report.CompletedAt.Format(time.RFC3339), report.DurationSecs,
		report.Summary.TotalRows, report.Summary.TransferredRows, report.Summary.FailedRows)

	return os.WriteFile(path, []byte(content), 0644)
}
