package types

import "time"

type MigrationConfig struct {
	Global         GlobalConfig         `mapstructure:"global"`
	Logging        LoggingConfig        `mapstructure:"logging"`
	ConnectionPool ConnectionPoolConfig `mapstructure:"connection_pool"`
	RateLimit      RateLimitConfig      `mapstructure:"rate_limit"`
	Migration      MigrationSettings    `mapstructure:"migration"`
	Retry          RetryConfig          `mapstructure:"retry"`
	Incremental    IncrementalConfig    `mapstructure:"incremental"`
	Sources        []SourceConfig       `mapstructure:"sources"`
	Targets        []TargetConfig       `mapstructure:"targets"`
	Tasks          []TaskConfig         `mapstructure:"tasks"`
}

type GlobalConfig struct {
	Name          string `mapstructure:"name"`
	CheckpointDir string `mapstructure:"checkpoint_dir"`
	ReportDir     string `mapstructure:"report_dir"`
}

type LoggingConfig struct {
	Level  string        `mapstructure:"level"`
	Output string        `mapstructure:"output"`
	File   LogFileConfig `mapstructure:"file"`
}

type LogFileConfig struct {
	Path       string `mapstructure:"path"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
	Compress   bool   `mapstructure:"compress"`
}

type ConnectionPoolConfig struct {
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`
}

type RateLimitConfig struct {
	Enabled         bool    `mapstructure:"enabled"`
	PointsPerSecond float64 `mapstructure:"points_per_second"`
	BurstSize       int     `mapstructure:"burst_size"`
}

type MigrationSettings struct {
	ParallelTasks     int              `mapstructure:"parallel_tasks"`
	ChunkSize         int              `mapstructure:"chunk_size"`
	ChunkInterval     time.Duration    `mapstructure:"chunk_interval"`
	MaxSeriesParallel int              `mapstructure:"max_series_parallel"`
	SourceProtection  SourceProtection `mapstructure:"source_protection"`
}

type SourceProtection struct {
	QueriesPerSecond int `mapstructure:"queries_per_second"`
	RowsPerSecond    int `mapstructure:"rows_per_second"`
}

type RetryConfig struct {
	MaxAttempts       int           `mapstructure:"max_attempts"`
	InitialDelay      time.Duration `mapstructure:"initial_delay"`
	MaxDelay          time.Duration `mapstructure:"max_delay"`
	BackoffMultiplier float64       `mapstructure:"backoff_multiplier"`
}

type IncrementalConfig struct {
	Enabled       bool `mapstructure:"enabled"`
	IntervalHours int  `mapstructure:"interval_hours"`
}

type SourceConfig struct {
	Name     string               `mapstructure:"name"`
	Type     string               `mapstructure:"type"`
	Host     string               `mapstructure:"host"`
	Port     int                  `mapstructure:"port"`
	User     string               `mapstructure:"user"`
	Password string               `mapstructure:"password"`
	Database string               `mapstructure:"database"`
	SSL      SSLConfig            `mapstructure:"ssl"`
	Pool     ConnectionPoolConfig `mapstructure:"pool"`

	MySQL    MySQLConfig    `mapstructure:"mysql"`
	TDengine TDengineConfig `mapstructure:"tdengine"`
	InfluxDB InfluxDBConfig `mapstructure:"influxdb"`
}

type TargetConfig struct {
	Name     string    `mapstructure:"name"`
	Type     string    `mapstructure:"type"`
	Host     string    `mapstructure:"host"`
	Port     int       `mapstructure:"port"`
	User     string    `mapstructure:"user"`
	Password string    `mapstructure:"password"`
	Database string    `mapstructure:"database"`
	SSL      SSLConfig `mapstructure:"ssl"`

	InfluxDB InfluxDBTargetConfig `mapstructure:"influxdb"`
	MySQL    MySQLConfig          `mapstructure:"mysql"`
	TDengine TDengineConfig       `mapstructure:"tdengine"`
}

type SSLConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	SkipVerify bool   `mapstructure:"skip_verify"`
	CaCert     string `mapstructure:"ca_cert"`
	ClientCert string `mapstructure:"client_cert"`
	ClientKey  string `mapstructure:"client_key"`
}

type MySQLConfig struct {
	Charset         string `mapstructure:"charset"`
	ReadFromReplica bool   `mapstructure:"read_from_replica"`
}

type TDengineConfig struct {
	Version string `mapstructure:"version"`
}

type InfluxDBConfig struct {
	Version string `mapstructure:"version"`
	URL     string `mapstructure:"url"`
	Token   string `mapstructure:"token"`
	Org     string `mapstructure:"org"`
	Bucket  string `mapstructure:"bucket"`
}

type InfluxDBTargetConfig struct {
	Version         string `mapstructure:"version"`
	URL             string `mapstructure:"url"`
	Token           string `mapstructure:"token"`
	Org             string `mapstructure:"org"`
	Bucket          string `mapstructure:"bucket"`
	RetentionPolicy string `mapstructure:"retention_policy"`
}

type TaskConfig struct {
	Name     string          `mapstructure:"name"`
	Source   string          `mapstructure:"source"`
	Target   string          `mapstructure:"target"`
	Mappings []MappingConfig `mapstructure:"mappings"`
}

type MappingConfig struct {
	SourceTable       string              `mapstructure:"source_table"`
	TargetMeasurement string              `mapstructure:"target_measurement"`
	Measurement       string              `mapstructure:"measurement"`
	TimestampColumn   string              `mapstructure:"timestamp_column"`
	Schema            SchemaConfig        `mapstructure:"schema"`
	Incremental       IncrementalMapping  `mapstructure:"incremental"`
	TagFilters        map[string][]string `mapstructure:"tag_filters"`
	TimeRange         TimeRange           `mapstructure:"time_range"`
	TimeWindow        string              `mapstructure:"time_window"`
	SubtablePattern   string              `mapstructure:"subtable_pattern"`
}

type SchemaConfig struct {
	Tags   []FieldMapping `mapstructure:"tags"`
	Fields []FieldMapping `mapstructure:"fields"`
}

type IncrementalMapping struct {
	Enabled         bool   `mapstructure:"enabled"`
	TimestampColumn string `mapstructure:"timestamp_column"`
}

type TimeRange struct {
	Start string `mapstructure:"start"`
	End   string `mapstructure:"end"`
}
