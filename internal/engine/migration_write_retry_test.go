package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

// countingTargetAdapter records how many times WriteBatch was invoked and
// returns a configurable error. It is used to assert retry behavior in
// writeWithRetry without exercising the full migration pipeline.
type countingTargetAdapter struct {
	mu          sync.Mutex
	writeCalls  int
	writeErr    error
	writeDelays []time.Duration
}

func (m *countingTargetAdapter) Name() string                { return "counting-target" }
func (m *countingTargetAdapter) SupportedVersions() []string { return []string{"test"} }
func (m *countingTargetAdapter) Connect(ctx context.Context, config map[string]interface{}) error {
	return nil
}
func (m *countingTargetAdapter) Disconnect(ctx context.Context) error { return nil }
func (m *countingTargetAdapter) Ping(ctx context.Context) error       { return nil }

func (m *countingTargetAdapter) WriteBatch(ctx context.Context, measurement string, records []types.Record) error {
	m.mu.Lock()
	m.writeCalls++
	calls := m.writeCalls
	m.mu.Unlock()

	if m.writeErr != nil {
		// Allow per-call error overrides for tests that need the first call to
		// fail and a later call to succeed. When writeErr is set, every call
		// returns it.
		_ = calls
		return m.writeErr
	}
	return nil
}

func (m *countingTargetAdapter) MeasurementExists(ctx context.Context, name string) (bool, error) {
	return true, nil
}

func (m *countingTargetAdapter) CreateMeasurement(ctx context.Context, schema *types.Schema) error {
	return nil
}

func (m *countingTargetAdapter) writeCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeCalls
}

// newWriteRetryEngine builds a MigrationEngine wired to a countingTargetAdapter
// with retry settings tuned for fast, deterministic tests.
func newWriteRetryEngine(t *testing.T, target *countingTargetAdapter, maxAttempts int) *MigrationEngine {
	t.Helper()
	cfg := &types.MigrationConfig{
		Migration: types.MigrationSettings{
			ParallelTasks: 1,
			ChunkSize:     10,
			ChunkInterval: time.Nanosecond,
		},
		Retry: types.RetryConfig{
			MaxAttempts:       maxAttempts,
			InitialDelay:      time.Millisecond,
			MaxDelay:          time.Millisecond,
			BackoffMultiplier: 1.0,
		},
	}
	engine := NewMigrationEngine(cfg, nil)
	return engine
}

func TestIsRetryableWriteError_ClassifiesHTTPStatus(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		want    bool
	}{
		{
			name: "4xx auth error not retryable",
			err:  fmt.Errorf("write failed with status 401: unauthorized"),
			want: false,
		},
		{
			name: "4xx forbidden not retryable",
			err:  fmt.Errorf("write failed with status 403: forbidden"),
			want: false,
		},
		{
			name: "400 malformed line protocol not retryable",
			err:  fmt.Errorf("write failed with status 400: unable to parse"),
			want: false,
		},
		{
			name: "429 rate limited IS retryable",
			err:  fmt.Errorf("write failed with status 429: too many requests"),
			want: true,
		},
		{
			name: "5xx server error retryable",
			err:  fmt.Errorf("write failed with status 500: internal server error"),
			want: true,
		},
		{
			name: "503 service unavailable retryable",
			err:  fmt.Errorf("write failed with status 503: unavailable"),
			want: true,
		},
		{
			name: "generic network error retryable",
			err:  errors.New("connection refused"),
			want: true,
		},
		{
			name: "nil error retryable (caller should not call)",
			err:  nil,
			want: true,
		},
		{
			name: "wrapped 4xx error not retryable",
			err:  fmt.Errorf("batch write: %w", fmt.Errorf("write failed with status 401: unauthorized")),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isRetryableWriteError(tc.err)
			if got != tc.want {
				t.Fatalf("isRetryableWriteError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestWriteWithRetry_DoesNotRetry4xxPermanentError(t *testing.T) {
	target := &countingTargetAdapter{
		writeErr: fmt.Errorf("write failed with status 401: unauthorized"),
	}
	engine := newWriteRetryEngine(t, target, 3)

	err := engine.writeWithRetry(context.Background(), "m0", []types.Record{*types.NewRecord()}, target)
	if err == nil {
		t.Fatal("expected write error for 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected error to preserve 401 context, got %v", err)
	}
	if calls := target.writeCallCount(); calls != 1 {
		t.Fatalf("expected WriteBatch called exactly once for permanent 4xx error, got %d", calls)
	}
}

func TestWriteWithRetry_Retries5xxServerError(t *testing.T) {
	target := &countingTargetAdapter{
		writeErr: fmt.Errorf("write failed with status 500: internal server error"),
	}
	engine := newWriteRetryEngine(t, target, 3)

	err := engine.writeWithRetry(context.Background(), "m0", []types.Record{*types.NewRecord()}, target)
	if err == nil {
		t.Fatal("expected write error after exhausting retries for 5xx")
	}
	if calls := target.writeCallCount(); calls != 3 {
		t.Fatalf("expected WriteBatch called 3 times (max attempts) for retryable 5xx error, got %d", calls)
	}
}

func TestWriteWithRetry_Retries429RateLimit(t *testing.T) {
	target := &countingTargetAdapter{
		writeErr: fmt.Errorf("write failed with status 429: too many requests"),
	}
	engine := newWriteRetryEngine(t, target, 3)

	err := engine.writeWithRetry(context.Background(), "m0", []types.Record{*types.NewRecord()}, target)
	if err == nil {
		t.Fatal("expected write error after exhausting retries for 429")
	}
	if calls := target.writeCallCount(); calls != 3 {
		t.Fatalf("expected WriteBatch called 3 times (max attempts) for retryable 429 error, got %d", calls)
	}
}

func TestWriteWithRetry_StillSucceedsOnTransientThenNil(t *testing.T) {
	// A retryable error on the first call, then success on the second call.
	target := &countingTargetAdapter{}
	engine := newWriteRetryEngine(t, target, 3)

	// First call fails with a retryable 5xx, subsequent calls succeed.
	target.writeErr = fmt.Errorf("write failed with status 503: unavailable")
	// Swap to nil after the first call. We approximate this by clearing the
	// error in a small goroutine-free way: use a per-call override via a
	// wrapper.
	onceTarget := &firstCallFailsTarget{wrapped: target}
	err := engine.writeWithRetry(context.Background(), "m0", []types.Record{*types.NewRecord()}, onceTarget)
	if err != nil {
		t.Fatalf("expected eventual success on retryable error, got %v", err)
	}
	if calls := target.writeCallCount(); calls != 2 {
		t.Fatalf("expected WriteBatch called twice (fail then succeed), got %d", calls)
	}
}

// firstCallFailsTarget fails the first WriteBatch with a retryable error and
// delegates subsequent calls to a wrapped adapter that succeeds.
type firstCallFailsTarget struct {
	wrapped *countingTargetAdapter
	once    sync.Once
}

func (f *firstCallFailsTarget) Name() string                { return f.wrapped.Name() }
func (f *firstCallFailsTarget) SupportedVersions() []string { return f.wrapped.SupportedVersions() }
func (f *firstCallFailsTarget) Connect(ctx context.Context, config map[string]interface{}) error {
	return f.wrapped.Connect(ctx, config)
}
func (f *firstCallFailsTarget) Disconnect(ctx context.Context) error { return f.wrapped.Disconnect(ctx) }
func (f *firstCallFailsTarget) Ping(ctx context.Context) error       { return f.wrapped.Ping(ctx) }
func (f *firstCallFailsTarget) MeasurementExists(ctx context.Context, name string) (bool, error) {
	return f.wrapped.MeasurementExists(ctx, name)
}
func (f *firstCallFailsTarget) CreateMeasurement(ctx context.Context, schema *types.Schema) error {
	return f.wrapped.CreateMeasurement(ctx, schema)
}

func (f *firstCallFailsTarget) WriteBatch(ctx context.Context, measurement string, records []types.Record) error {
	var firstErr error
	f.once.Do(func() {
		// Suppress the wrapped error for the first call only and return a
		// retryable 5xx instead.
		_ = f.wrapped.WriteBatch(ctx, measurement, records)
		firstErr = fmt.Errorf("write failed with status 503: unavailable")
	})
	if firstErr != nil {
		return firstErr
	}
	// Clear the wrapped adapter's error so subsequent calls succeed.
	f.wrapped.mu.Lock()
	f.wrapped.writeErr = nil
	f.wrapped.mu.Unlock()
	return f.wrapped.WriteBatch(ctx, measurement, records)
}
