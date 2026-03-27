package adapter

import (
	"context"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

type SourceAdapter interface {
	Name() string
	SupportedVersions() []string

	Connect(ctx context.Context, config map[string]interface{}) error
	Disconnect(ctx context.Context) error
	Ping(ctx context.Context) error

	DiscoverTables(ctx context.Context) ([]string, error)
	DiscoverSeries(ctx context.Context, measurement string) ([]string, error)

	// DiscoverSchema returns the schema for a given table.
	// Returns a TableSchema with TableName and Columns populated.
	// For adapters that don't support schema discovery (e.g., InfluxDB, TDengine),
	// this returns a minimal schema with just the TableName set.
	DiscoverSchema(ctx context.Context, table string) (*types.TableSchema, error)

	QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)
}

type TargetAdapter interface {
	Name() string
	SupportedVersions() []string

	Connect(ctx context.Context, config map[string]interface{}) error
	Disconnect(ctx context.Context) error
	Ping(ctx context.Context) error

	WriteBatch(ctx context.Context, measurement string, records []types.Record) error
	MeasurementExists(ctx context.Context, name string) (bool, error)
	CreateMeasurement(ctx context.Context, schema *types.Schema) error
}
