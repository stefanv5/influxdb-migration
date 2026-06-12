package types

import "testing"

func TestParseSeriesKeyEscapedComponents(t *testing.T) {
	parsed := ParseSeriesKey(`cpu\,load,host=server\,1,region=us\=east,path=c:\\data,tag\,key=value`)

	if parsed.Measurement != "cpu,load" {
		t.Fatalf("expected escaped measurement, got %q", parsed.Measurement)
	}

	expected := map[string]string{
		"host":    "server,1",
		"region":  "us=east",
		"path":    `c:\data`,
		"tag,key": "value",
	}
	for k, v := range expected {
		if parsed.Tags[k] != v {
			t.Fatalf("expected tag %s=%q, got %q", k, v, parsed.Tags[k])
		}
	}

	if len(parsed.TagPairs) != 4 {
		t.Fatalf("expected ordered tag pairs, got %d", len(parsed.TagPairs))
	}
	if parsed.TagPairs[0].Key != "host" || parsed.TagPairs[0].Value != "server,1" {
		t.Fatalf("unexpected first tag pair: %#v", parsed.TagPairs[0])
	}
	if parsed.TagPairs[1].Key != "region" || parsed.TagPairs[1].Value != "us=east" {
		t.Fatalf("unexpected second tag pair: %#v", parsed.TagPairs[1])
	}
}

func TestParseSeriesKeyNoTags(t *testing.T) {
	parsed := ParseSeriesKey("cpu")

	if parsed.Measurement != "cpu" {
		t.Fatalf("expected measurement cpu, got %q", parsed.Measurement)
	}
	if len(parsed.Tags) != 0 {
		t.Fatalf("expected no tags, got %#v", parsed.Tags)
	}
	if len(parsed.TagPairs) != 0 {
		t.Fatalf("expected no tag pairs, got %#v", parsed.TagPairs)
	}
}
