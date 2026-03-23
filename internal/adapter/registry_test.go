package adapter

import (
	"context"
	"testing"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

// MockSourceAdapter for testing
type MockSourceAdapter struct {
	name             string
	versions         []string
	connectCalled    bool
	disconnectCalled bool
	pingCalled       bool
}

func (m *MockSourceAdapter) Name() string                { return m.name }
func (m *MockSourceAdapter) SupportedVersions() []string { return m.versions }
func (m *MockSourceAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	m.connectCalled = true
	return nil
}
func (m *MockSourceAdapter) Disconnect(ctx context.Context) error {
	m.disconnectCalled = true
	return nil
}
func (m *MockSourceAdapter) Ping(ctx context.Context) error {
	m.pingCalled = true
	return nil
}
func (m *MockSourceAdapter) DiscoverTables(ctx context.Context) ([]string, error) {
	return []string{"table1", "table2"}, nil
}
func (m *MockSourceAdapter) DiscoverSeries(ctx context.Context, measurement string) ([]string, error) {
	return []string{"series1"}, nil
}
func (m *MockSourceAdapter) QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error) (*types.Checkpoint, error) {
	return &types.Checkpoint{ProcessedRows: 100}, nil
}

// MockTargetAdapter for testing
type MockTargetAdapter struct {
	name             string
	versions         []string
	connectCalled    bool
	disconnectCalled bool
	pingCalled       bool
}

func (m *MockTargetAdapter) Name() string                { return m.name }
func (m *MockTargetAdapter) SupportedVersions() []string { return m.versions }
func (m *MockTargetAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	m.connectCalled = true
	return nil
}
func (m *MockTargetAdapter) Disconnect(ctx context.Context) error {
	m.disconnectCalled = true
	return nil
}
func (m *MockTargetAdapter) Ping(ctx context.Context) error {
	m.pingCalled = true
	return nil
}
func (m *MockTargetAdapter) WriteBatch(ctx context.Context, measurement string, records []types.Record) error {
	return nil
}
func (m *MockTargetAdapter) MeasurementExists(ctx context.Context, name string) (bool, error) {
	return true, nil
}
func (m *MockTargetAdapter) CreateMeasurement(ctx context.Context, schema *types.Schema) error {
	return nil
}

func TestNewRegistry(t *testing.T) {
	registry := NewRegistry()
	if registry == nil {
		t.Error("Expected non-nil registry")
	}

	if registry.sourceAdapters == nil {
		t.Error("Expected sourceAdapters map to be initialized")
	}

	if registry.targetAdapters == nil {
		t.Error("Expected targetAdapters map to be initialized")
	}
}

func TestRegisterSource(t *testing.T) {
	registry := NewRegistry()
	adapter := &MockSourceAdapter{name: "mock", versions: []string{"1.0"}}

	registry.RegisterSource("mock", func() SourceAdapter { return adapter })

	if len(registry.sourceAdapters) != 1 {
		t.Errorf("Expected 1 source adapter, got %d", len(registry.sourceAdapters))
	}
}

func TestRegisterTarget(t *testing.T) {
	registry := NewRegistry()
	adapter := &MockTargetAdapter{name: "mock-target", versions: []string{"1.0"}}

	registry.RegisterTarget("mock-target", func() TargetAdapter { return adapter })

	if len(registry.targetAdapters) != 1 {
		t.Errorf("Expected 1 target adapter, got %d", len(registry.targetAdapters))
	}
}

func TestGetSourceAdapter(t *testing.T) {
	registry := NewRegistry()
	adapter := &MockSourceAdapter{name: "mock", versions: []string{"1.0"}}

	registry.RegisterSource("mock", func() SourceAdapter { return adapter })

	src, err := registry.GetSourceAdapter("mock")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if src.Name() != "mock" {
		t.Errorf("Expected name 'mock', got %s", src.Name())
	}
}

func TestGetSourceAdapter_NotFound(t *testing.T) {
	registry := NewRegistry()

	_, err := registry.GetSourceAdapter("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent adapter")
	}
}

func TestGetTargetAdapter(t *testing.T) {
	registry := NewRegistry()
	adapter := &MockTargetAdapter{name: "mock-target", versions: []string{"1.0"}}

	registry.RegisterTarget("mock-target", func() TargetAdapter { return adapter })

	tgt, err := registry.GetTargetAdapter("mock-target")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if tgt.Name() != "mock-target" {
		t.Errorf("Expected name 'mock-target', got %s", tgt.Name())
	}
}

func TestGetTargetAdapter_NotFound(t *testing.T) {
	registry := NewRegistry()

	_, err := registry.GetTargetAdapter("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent adapter")
	}
}

func TestListSourceAdapters(t *testing.T) {
	registry := NewRegistry()

	registry.RegisterSource("adapter1", func() SourceAdapter { return &MockSourceAdapter{} })
	registry.RegisterSource("adapter2", func() SourceAdapter { return &MockSourceAdapter{} })

	names := registry.ListSourceAdapters()
	if len(names) != 2 {
		t.Errorf("Expected 2 adapter names, got %d", len(names))
	}
}

func TestListTargetAdapters(t *testing.T) {
	registry := NewRegistry()

	registry.RegisterTarget("target1", func() TargetAdapter { return &MockTargetAdapter{} })
	registry.RegisterTarget("target2", func() TargetAdapter { return &MockTargetAdapter{} })

	names := registry.ListTargetAdapters()
	if len(names) != 2 {
		t.Errorf("Expected 2 adapter names, got %d", len(names))
	}
}

func TestRegisterSourceAdapter(t *testing.T) {
	// Clear global registry for testing
	globalRegistry = NewRegistry()

	RegisterSourceAdapter("test-source", func() SourceAdapter {
		return &MockSourceAdapter{name: "test-source", versions: []string{"1.0"}}
	})

	src, err := GetRegistry().GetSourceAdapter("test-source")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if src.Name() != "test-source" {
		t.Errorf("Expected name 'test-source', got %s", src.Name())
	}
}

func TestRegisterTargetAdapter(t *testing.T) {
	// Clear global registry for testing
	globalRegistry = NewRegistry()

	RegisterTargetAdapter("test-target", func() TargetAdapter {
		return &MockTargetAdapter{name: "test-target", versions: []string{"1.0"}}
	})

	tgt, err := GetRegistry().GetTargetAdapter("test-target")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if tgt.Name() != "test-target" {
		t.Errorf("Expected name 'test-target', got %s", tgt.Name())
	}
}

func TestSourceAdapterFactory(t *testing.T) {
	factory := func() SourceAdapter {
		return &MockSourceAdapter{name: "factory-test"}
	}

	adapter := factory()
	if adapter.Name() != "factory-test" {
		t.Errorf("Expected 'factory-test', got %s", adapter.Name())
	}
}

func TestTargetAdapterFactory(t *testing.T) {
	factory := func() TargetAdapter {
		return &MockTargetAdapter{name: "factory-target"}
	}

	adapter := factory()
	if adapter.Name() != "factory-target" {
		t.Errorf("Expected 'factory-target', got %s", adapter.Name())
	}
}
