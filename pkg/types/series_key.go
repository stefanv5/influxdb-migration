package types

import "strings"

// SeriesTag preserves the order of tags in an InfluxDB series key.
type SeriesTag struct {
	Key   string
	Value string
}

// ParsedSeriesKey contains the measurement and parsed tags from a series key.
type ParsedSeriesKey struct {
	Measurement string
	Tags        map[string]string
	TagPairs    []SeriesTag
}

// ParseSeriesKey parses "measurement,tag1=value1,tag2=value2" series keys.
// It follows Influx line protocol escaping for commas, equals signs, and backslashes.
func ParseSeriesKey(key string) ParsedSeriesKey {
	parsed := ParsedSeriesKey{
		Tags: make(map[string]string),
	}

	parts := splitEscapedSeriesKey(key, ',')
	if len(parts) == 0 {
		return parsed
	}

	parsed.Measurement = unescapeSeriesKeyComponent(parts[0])
	for _, part := range parts[1:] {
		kv := splitEscapedSeriesKeyN(part, '=', 2)
		if len(kv) != 2 {
			continue
		}
		tag := SeriesTag{
			Key:   unescapeSeriesKeyComponent(kv[0]),
			Value: unescapeSeriesKeyComponent(kv[1]),
		}
		parsed.Tags[tag.Key] = tag.Value
		parsed.TagPairs = append(parsed.TagPairs, tag)
	}

	return parsed
}

func splitEscapedSeriesKey(s string, sep rune) []string {
	return splitEscapedSeriesKeyN(s, sep, -1)
}

func splitEscapedSeriesKeyN(s string, sep rune, n int) []string {
	var parts []string
	var b strings.Builder
	escaped := false

	for _, r := range s {
		if escaped {
			b.WriteRune('\\')
			b.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if r == sep && (n < 0 || len(parts) < n-1) {
			parts = append(parts, b.String())
			b.Reset()
			continue
		}
		b.WriteRune(r)
	}
	if escaped {
		b.WriteRune('\\')
	}
	parts = append(parts, b.String())
	return parts
}

func unescapeSeriesKeyComponent(s string) string {
	var b strings.Builder
	escaped := false
	for _, r := range s {
		if escaped {
			switch r {
			case ',', '=', '\\':
				b.WriteRune(r)
			default:
				b.WriteRune('\\')
				b.WriteRune(r)
			}
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		b.WriteRune(r)
	}
	if escaped {
		b.WriteRune('\\')
	}
	return b.String()
}
