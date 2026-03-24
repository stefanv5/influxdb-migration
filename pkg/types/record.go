package types

import (
	"time"
)

type Record struct {
	ID     int64
	Fields map[string]interface{}
	Tags   map[string]string
	Time   int64
}

func NewRecord() *Record {
	return &Record{
		Fields: make(map[string]interface{}),
		Tags:   make(map[string]string),
		Time:   0,
	}
}

func (r *Record) AddField(key string, value interface{}) {
	if value != nil {
		r.Fields[key] = value
	}
}

func (r *Record) AddTag(key, value string) {
	if value != "" {
		r.Tags[key] = value
	}
}

func TimeToUnixNano(t time.Time) int64 {
	return t.UnixNano()
}

func UnixNanoToTime(ns int64) time.Time {
	return time.Unix(0, ns)
}

func NowNano() int64 {
	return time.Now().UnixNano()
}

func IsZeroTime(ns int64) bool {
	return ns == 0
}
