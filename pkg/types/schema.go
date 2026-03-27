package types

// TableSchema represents the schema of a source table (e.g., MySQL table)
type TableSchema struct {
	TableName        string
	Columns          []Column
	PrimaryKey       string
	TimestampColumn  string
}

// Column represents a column in a source table schema
type Column struct {
	Name     string
	Type     string
	Nullable bool
}
