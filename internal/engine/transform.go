package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/migration-tools/influx-migrator/pkg/types"
)

type TransformEngine struct {
}

func NewTransformEngine() *TransformEngine {
	return &TransformEngine{}
}

func (t *TransformEngine) Transform(record *types.Record, mapping *types.MappingConfig) *types.Record {
	transformed := types.NewRecord()
	transformed.Time = record.Time

	schema := mapping.Schema

	for _, fieldMapping := range schema.Fields {
		if fieldMapping.SourceName == "" {
			continue
		}

		if val, ok := record.Fields[fieldMapping.SourceName]; ok {
			targetName := fieldMapping.TargetName
			if targetName == "" {
				targetName = fieldMapping.SourceName
			}

			converted := t.convertValue(val, fieldMapping.DataType)
			transformed.AddField(targetName, converted)
		}
	}

	for _, tagMapping := range schema.Tags {
		if tagMapping.SourceName == "" {
			continue
		}

		if val, ok := record.Tags[tagMapping.SourceName]; ok {
			targetName := tagMapping.TargetName
			if targetName == "" {
				targetName = tagMapping.SourceName
			}
			transformed.AddTag(targetName, val)
		} else if val, ok := record.Fields[tagMapping.SourceName]; ok {
			targetName := tagMapping.TargetName
			if targetName == "" {
				targetName = tagMapping.SourceName
			}
			if strVal, ok := val.(string); ok {
				transformed.AddTag(targetName, strVal)
			}
		}
	}

	return transformed
}

func (t *TransformEngine) convertValue(value interface{}, targetType string) interface{} {
	if value == nil {
		return nil
	}

	switch targetType {
	case "float":
		return toFloat64(value)
	case "int", "integer":
		return toInt64(value)
	case "string":
		return toString(value)
	case "bool", "boolean":
		return toBool(value)
	default:
		return value
	}
}

func toFloat64(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		return 0
	default:
		return 0
	}
}

func toInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case uint64:
		return int64(v)
	case uint:
		return int64(v)
	case uint32:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		return 0
	default:
		return 0
	}
}

func toString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case bool:
		return strconv.FormatBool(v)
	default:
		return ""
	}
}

func toBool(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		lower := strings.ToLower(v)
		return lower == "true" || lower == "yes" || lower == "on" || lower == "1"
	case int64:
		return v != 0
	case int:
		return v != 0
	case float64:
		return v != 0
	default:
		return false
	}
}

func (t *TransformEngine) FilterNulls(record *types.Record) *types.Record {
	filtered := types.NewRecord()
	filtered.Time = record.Time

	for k, v := range record.Tags {
		if v != "" {
			filtered.Tags[k] = v
		}
	}

	for k, v := range record.Fields {
		if v != nil {
			filtered.Fields[k] = v
		}
	}

	return filtered
}

func (t *TransformEngine) ApplySchemaMapping(record *types.Record, mapping *types.MappingConfig) *types.Record {
	transformed := types.NewRecord()
	transformed.Time = record.Time

	for _, fm := range mapping.Schema.Fields {
		sourceName := fm.SourceName
		targetName := fm.TargetName
		if targetName == "" {
			targetName = sourceName
		}

		if val, exists := record.Fields[sourceName]; exists {
			transformed.AddField(targetName, val)
		}
	}

	for _, tm := range mapping.Schema.Tags {
		sourceName := tm.SourceName
		targetName := tm.TargetName
		if targetName == "" {
			targetName = sourceName
		}

		if val, exists := record.Tags[sourceName]; exists {
			transformed.AddTag(targetName, val)
		} else if val, exists := record.Fields[sourceName]; exists {
			if strVal, ok := val.(string); ok {
				transformed.AddTag(targetName, strVal)
			}
		}
	}

	return transformed
}

// ValidateRecord checks for potential data issues in a record.
// Returns a list of warnings found in the record.
func (t *TransformEngine) ValidateRecord(record *types.Record) []string {
	var warnings []string

	// Check for tag/field name collisions
	tagFields := make(map[string]struct{})
	for k := range record.Tags {
		tagFields[k] = struct{}{}
	}

	for fieldName := range record.Fields {
		if _, hasTag := tagFields[fieldName]; hasTag {
			warnings = append(warnings,
				fmt.Sprintf("field and tag have same name %q: InfluxDB allows this but queries may return unexpected results", fieldName))
		}
	}

	return warnings
}

// ValidateMapping checks the mapping configuration for issues.
// Returns error if validation fails.
func (t *TransformEngine) ValidateMapping(mapping *types.MappingConfig) error {
	if mapping == nil {
		return nil
	}

	// Check for duplicate names in field mappings
	fieldNames := make(map[string]bool)
	for _, fm := range mapping.Schema.Fields {
		targetName := fm.TargetName
		if targetName == "" {
			targetName = fm.SourceName
		}
		if fieldNames[targetName] {
			return fmt.Errorf("duplicate field target name %q in mapping", targetName)
		}
		fieldNames[targetName] = true
	}

	// Check for duplicate names in tag mappings
	tagNames := make(map[string]bool)
	for _, tm := range mapping.Schema.Tags {
		targetName := tm.TargetName
		if targetName == "" {
			targetName = tm.SourceName
		}
		if tagNames[targetName] {
			return fmt.Errorf("duplicate tag target name %q in mapping", targetName)
		}
		tagNames[targetName] = true
	}

	// Check for field/tag name collisions in mapping targets
	for fieldName := range fieldNames {
		if _, hasTag := tagNames[fieldName]; hasTag {
			return fmt.Errorf("field %q and tag %q have the same target name in mapping", fieldName, fieldName)
		}
	}

	return nil
}
