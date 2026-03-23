package types

import (
	"testing"
	"time"
)

func TestRecord_AddField(t *testing.T) {
	record := NewRecord()
	record.AddField("cpu_usage", 85.5)
	record.AddField("memory_usage", nil)

	if record.Fields["cpu_usage"] != 85.5 {
		t.Errorf("Expected cpu_usage to be 85.5, got %v", record.Fields["cpu_usage"])
	}

	if _, exists := record.Fields["memory_usage"]; exists {
		t.Error("Expected memory_usage to be nil and not added")
	}
}

func TestRecord_AddTag(t *testing.T) {
	record := NewRecord()
	record.AddTag("host", "server1")
	record.AddTag("region", "")

	if record.Tags["host"] != "server1" {
		t.Errorf("Expected host to be server1, got %v", record.Tags["host"])
	}

	if _, exists := record.Tags["region"]; exists {
		t.Error("Expected region to be empty string and not added")
	}
}

func TestRecord_NilValueHandling(t *testing.T) {
	record := NewRecord()
	record.AddField("field1", nil)
	record.AddTag("tag1", "")

	if len(record.Fields) != 0 {
		t.Errorf("Expected 0 fields, got %d", len(record.Fields))
	}

	if len(record.Tags) != 0 {
		t.Errorf("Expected 0 tags, got %d", len(record.Tags))
	}
}

func TestCheckpoint_Status(t *testing.T) {
	if StatusPending != "pending" {
		t.Errorf("Expected StatusPending to be 'pending', got %s", StatusPending)
	}

	if StatusInProgress != "in_progress" {
		t.Errorf("Expected StatusInProgress to be 'in_progress', got %s", StatusInProgress)
	}

	if StatusCompleted != "completed" {
		t.Errorf("Expected StatusCompleted to be 'completed', got %s", StatusCompleted)
	}

	if StatusFailed != "failed" {
		t.Errorf("Expected StatusFailed to be 'failed', got %s", StatusFailed)
	}
}

func TestCheckpoint_TimeTracking(t *testing.T) {
	now := time.Now()
	cp := &Checkpoint{
		TaskID:        "test-task",
		LastID:        100,
		LastTimestamp: now,
		ProcessedRows: 500,
		Status:        StatusInProgress,
	}

	if cp.LastTimestamp.IsZero() {
		t.Error("Expected LastTimestamp to be set")
	}

	if cp.ProcessedRows != 500 {
		t.Errorf("Expected ProcessedRows to be 500, got %d", cp.ProcessedRows)
	}
}
