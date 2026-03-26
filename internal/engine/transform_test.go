package engine

import (
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

func TestTransformEngine_ConvertValue(t *testing.T) {
	engine := NewTransformEngine()

	tests := []struct {
		name       string
		input      interface{}
		targetType string
		expected   interface{}
	}{
		{"float64 to float", float64(85.5), "float", float64(85.5)},
		{"float32 to float", float32(85.5), "float", float64(85.5)},
		{"int to float", int(85), "float", float64(85.0)},
		{"int64 to float", int64(85), "float", float64(85.0)},
		{"int to int", int(123), "int", int64(123)},
		{"string to int", "123", "int", int64(123)},
		{"nil value", nil, "float", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.convertValue(tt.input, tt.targetType)
			if result != tt.expected {
				t.Errorf("convertValue(%v, %s) = %v, want %v", tt.input, tt.targetType, result, tt.expected)
			}
		})
	}
}

func TestTransformEngine_FilterNulls(t *testing.T) {
	engine := NewTransformEngine()

	record := types.NewRecord()
	record.Time = time.Now().UnixNano()
	record.AddField("field1", "value1")
	record.AddField("field2", nil)
	record.AddField("field3", 0)
	record.AddField("field4", 42)
	record.AddTag("tag1", "v1")
	record.AddTag("tag2", "")
	record.AddTag("tag3", "v3")

	filtered := engine.FilterNulls(record)

	if len(filtered.Fields) != 3 {
		t.Errorf("Expected 3 fields after filtering, got %d", len(filtered.Fields))
	}

	if len(filtered.Tags) != 2 {
		t.Errorf("Expected 2 tags after filtering, got %d", len(filtered.Tags))
	}

	if _, exists := filtered.Fields["field2"]; exists {
		t.Error("Expected field2 to be filtered out")
	}

	if _, exists := filtered.Tags["tag2"]; exists {
		t.Error("Expected tag2 to be filtered out")
	}

	if _, exists := filtered.Fields["field4"]; !exists {
		t.Error("Expected field4 (zero value) to be kept")
	}
}

func TestTransformEngine_ApplySchemaMapping(t *testing.T) {
	engine := NewTransformEngine()

	mapping := &types.MappingConfig{
		Schema: types.SchemaConfig{
			Tags: []types.FieldMapping{
				{SourceName: "host_id", TargetName: "host", IsTag: true},
				{SourceName: "region", TargetName: "", IsTag: true},
			},
			Fields: []types.FieldMapping{
				{SourceName: "cpu_usage", TargetName: "cpu", DataType: "float"},
				{SourceName: "memory_usage", TargetName: "memory", DataType: "float"},
				{SourceName: "disk_usage", TargetName: "", DataType: "float"},
			},
		},
	}

	record := types.NewRecord()
	record.Time = time.Now().UnixNano()
	record.AddField("host_id", "server1")
	record.AddField("cpu_usage", 85.5)
	record.AddField("disk_usage", 75.0)
	record.AddField("extra_field", "should not appear")

	transformed := engine.ApplySchemaMapping(record, mapping)

	if _, exists := transformed.Tags["host"]; !exists {
		t.Error("Expected tag 'host' to be present")
	}

	if transformed.Tags["host"] != "server1" {
		t.Errorf("Expected tag 'host' to be 'server1', got %s", transformed.Tags["host"])
	}

	if _, exists := transformed.Fields["cpu"]; !exists {
		t.Error("Expected field 'cpu' to be present")
	}

	if _, exists := transformed.Fields["disk_usage"]; !exists {
		t.Error("Expected field 'disk_usage' to be present (empty target means same as source)")
	}

	if _, exists := transformed.Fields["extra_field"]; exists {
		t.Error("Expected extra_field to be filtered out")
	}
}

func TestTransformEngine_ApplySchemaMapping_TagFromField(t *testing.T) {
	engine := NewTransformEngine()

	mapping := &types.MappingConfig{
		Schema: types.SchemaConfig{
			Tags: []types.FieldMapping{
				{SourceName: "location", TargetName: "loc", IsTag: true},
			},
			Fields: []types.FieldMapping{},
		},
	}

	record := types.NewRecord()
	record.Time = time.Now().UnixNano()
	record.AddField("location", "us-east-1")

	transformed := engine.ApplySchemaMapping(record, mapping)

	if _, exists := transformed.Tags["loc"]; !exists {
		t.Error("Expected tag 'loc' to be present")
	}

	if transformed.Tags["loc"] != "us-east-1" {
		t.Errorf("Expected tag 'loc' to be 'us-east-1', got %s", transformed.Tags["loc"])
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected float64
	}{
		{float64(85.5), 85.5},
		{float32(85.5), 85.5},
		{int(85), 85.0},
		{int64(85), 85.0},
		{"invalid", 0},
	}

	for _, tt := range tests {
		result := toFloat64(tt.input)
		if result != tt.expected {
			t.Errorf("toFloat64(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected int64
	}{
		{int64(85), 85},
		{int(85), 85},
		{float64(85.9), 85},
		{"invalid", 0},
	}

	for _, tt := range tests {
		result := toInt64(tt.input)
		if result != tt.expected {
			t.Errorf("toInt64(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestToBool(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected bool
	}{
		{true, true},
		{"true", true},
		{"1", true},
		{int64(1), true},
		{false, false},
		{"false", false},
		{"0", false},
		{int64(0), false},
	}

	for _, tt := range tests {
		result := toBool(tt.input)
		if result != tt.expected {
			t.Errorf("toBool(%v) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestToString(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"string passthrough", "hello", "hello"},
		{"float64 85.5", float64(85.5), "85.5"},
		{"float64 large", float64(1234567.89), "1234567.89"},
		{"float64 negative", float64(-85.5), "-85.5"},
		{"float64 decimal", float64(3.14159), "3.14159"},
		{"int64 123", int64(123), "123"},
		{"int 456", int(456), "456"},
		{"float32", float32(99.9), "99.9"},
		{"unknown type", struct{}{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toString(tt.input)
			if result != tt.expected {
				t.Errorf("toString(%v) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestTransformEngine_Transform(t *testing.T) {
	engine := NewTransformEngine()

	mapping := &types.MappingConfig{
		Schema: types.SchemaConfig{
			Tags: []types.FieldMapping{
				{SourceName: "host_id", TargetName: "host", IsTag: true},
			},
			Fields: []types.FieldMapping{
				{SourceName: "cpu_usage", TargetName: "cpu", DataType: "float"},
				{SourceName: "memory_usage", TargetName: "mem", DataType: "float"},
			},
		},
	}

	record := types.NewRecord()
	record.Time = time.Now().UnixNano()
	record.AddField("cpu_usage", 85.5)
	record.AddField("memory_usage", "4096") // string value, should be converted

	transformed := engine.Transform(record, mapping)

	if transformed.Fields["cpu"] != 85.5 {
		t.Errorf("Expected cpu=85.5, got %v", transformed.Fields["cpu"])
	}
}

func TestTransformEngine_ConvertValue_StringToFloat(t *testing.T) {
	engine := NewTransformEngine()

	// String "85.5" should be successfully parsed to float64
	result := engine.convertValue("85.5", "float")
	if result != float64(85.5) {
		t.Errorf("convertValue(\"85.5\", \"float\") = %v, want 85.5", result)
	}

	// Invalid string should return 0
	result = engine.convertValue("not_a_number", "float")
	if result != float64(0) {
		t.Errorf("convertValue(\"not_a_number\", \"float\") = %v, want 0", result)
	}
}

func TestTransformEngine_ConvertValue_UnknownType(t *testing.T) {
	engine := NewTransformEngine()

	result := engine.convertValue("test", "unknown")
	if result != "test" {
		t.Errorf("Expected unknown type to return value unchanged, got %v", result)
	}
}

func TestTransformEngine_FilterNulls_AllNil(t *testing.T) {
	engine := NewTransformEngine()

	record := types.NewRecord()
	record.Time = time.Now().UnixNano()
	record.AddField("field1", nil)
	record.AddField("field2", nil)
	record.AddTag("tag1", "")
	record.AddTag("tag2", "")

	filtered := engine.FilterNulls(record)

	if len(filtered.Fields) != 0 {
		t.Errorf("Expected 0 fields, got %d", len(filtered.Fields))
	}

	if len(filtered.Tags) != 0 {
		t.Errorf("Expected 0 tags, got %d", len(filtered.Tags))
	}
}
