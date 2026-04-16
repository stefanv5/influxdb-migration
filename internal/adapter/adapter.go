package adapter

import (
	"context"
	"time"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

// ShardGroup represents a time-bounded shard group in InfluxDB
type ShardGroup struct {
	ID        int
	StartTime time.Time
	EndTime   time.Time
}

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

	// DiscoverShardGroups returns all shard groups from the source
	DiscoverShardGroups(ctx context.Context) ([]*ShardGroup, error)

	// DiscoverSeriesInTimeWindow returns all series that have data within the given time window
	// No LIMIT/OFFSET is used - relies on time window to bound the result set
	DiscoverSeriesInTimeWindow(ctx context.Context, measurement string, startTime, endTime time.Time) ([]string, error)

	// DiscoverTagKeys returns all tag key names for a measurement.
	// Used by executeFluxSelect to distinguish tags from fields in V2 sources.
	// V1 sources return nil/empty slice as they don't need tag/field distinction.
	DiscoverTagKeys(ctx context.Context, measurement string) ([]string, error)

	QueryData(ctx context.Context, table string, lastCheckpoint *types.Checkpoint, batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)

	// QueryDataBatch queries multiple series within a time range for batch processing.
	// startTime: inclusive lower bound
	// endTime: exclusive upper bound (time < endTime)
	QueryDataBatch(ctx context.Context, measurement string, series []string,
		startTime, endTime time.Time, lastCheckpoint *types.Checkpoint,
		batchFunc func([]types.Record) error, cfg *types.QueryConfig) (*types.Checkpoint, error)
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
