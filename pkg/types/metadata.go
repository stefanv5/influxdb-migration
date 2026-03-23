package types

type Database struct {
	Name   string
	Tables []TableMetadata
}

type TableMetadata struct {
	Name        string
	Database    string
	Columns     []ColumnMetadata
	PrimaryKey  string
	TimestampColumn string
}

type ColumnMetadata struct {
	Name     string
	DataType string
	IsTag    bool
	IsPrimaryKey bool
}

type Schema struct {
	Measurement     string
	Tags            []FieldMapping
	Fields          []FieldMapping
	Timestamp       FieldMapping
	RetentionPolicy string
}

type FieldMapping struct {
	SourceName string
	TargetName string
	DataType   string
	IsTag      bool
}
