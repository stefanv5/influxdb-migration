package adapter

import (
	"fmt"
	"sync"
)

type AdapterRegistry struct {
	mu               sync.RWMutex
	sourceAdapters   map[string]SourceAdapterFactory
	targetAdapters   map[string]TargetAdapterFactory
}

type SourceAdapterFactory func() SourceAdapter
type TargetAdapterFactory func() TargetAdapter

var globalRegistry *AdapterRegistry

func init() {
	globalRegistry = NewRegistry()
}

func NewRegistry() *AdapterRegistry {
	return &AdapterRegistry{
		sourceAdapters: make(map[string]SourceAdapterFactory),
		targetAdapters: make(map[string]TargetAdapterFactory),
	}
}

func GetRegistry() *AdapterRegistry {
	return globalRegistry
}

func (r *AdapterRegistry) RegisterSource(name string, factory SourceAdapterFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sourceAdapters[name] = factory
}

func (r *AdapterRegistry) RegisterTarget(name string, factory TargetAdapterFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.targetAdapters[name] = factory
}

func (r *AdapterRegistry) GetSourceAdapter(name string) (SourceAdapter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, ok := r.sourceAdapters[name]
	if !ok {
		return nil, fmt.Errorf("unknown source adapter: %s", name)
	}
	return factory(), nil
}

func (r *AdapterRegistry) GetTargetAdapter(name string) (TargetAdapter, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, ok := r.targetAdapters[name]
	if !ok {
		return nil, fmt.Errorf("unknown target adapter: %s", name)
	}
	return factory(), nil
}

func (r *AdapterRegistry) ListSourceAdapters() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.sourceAdapters))
	for name := range r.sourceAdapters {
		names = append(names, name)
	}
	return names
}

func (r *AdapterRegistry) ListTargetAdapters() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.targetAdapters))
	for name := range r.targetAdapters {
		names = append(names, name)
	}
	return names
}

func RegisterSourceAdapter(name string, factory SourceAdapterFactory) {
	globalRegistry.RegisterSource(name, factory)
}

func RegisterTargetAdapter(name string, factory TargetAdapterFactory) {
	globalRegistry.RegisterTarget(name, factory)
}
