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

func TestRecord_TimeInitialized(t *testing.T) {
	record := NewRecord()
	if record.Time != 0 {
		t.Errorf("Expected Time to be 0, got %d", record.Time)
	}
}

func TestTimeToUnixNano(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	ns := TimeToUnixNano(ts)
	expected := ts.UnixNano()
	if ns != expected {
		t.Errorf("Expected %d, got %d", expected, ns)
	}
}

func TestUnixNanoToTime(t *testing.T) {
	ns := int64(1705315800123456789)
	tResult := UnixNanoToTime(ns)
	if tResult.Year() != 2024 {
		t.Errorf("Expected year 2024, got %d", tResult.Year())
	}
	if tResult.Month() != time.Month(1) {
		t.Errorf("Expected month 1, got %d", tResult.Month())
	}
	if tResult.Nanosecond() != 123456789 {
		t.Errorf("Expected nanosecond 123456789, got %d", tResult.Nanosecond())
	}
}

func TestNowNano(t *testing.T) {
	before := time.Now().UnixNano()
	ns := NowNano()
	after := time.Now().UnixNano()
	if ns < before || ns > after {
		t.Errorf("NowNano() returned value outside expected range")
	}
}

func TestIsZeroTime(t *testing.T) {
	if !IsZeroTime(0) {
		t.Error("Expected IsZeroTime(0) to be true")
	}
	if IsZeroTime(1) {
		t.Error("Expected IsZeroTime(1) to be false")
	}
}
