package types

import (
	"time"
)

type Record struct {
	ID     int64
	Fields map[string]interface{}
	Tags   map[string]string
	Time   time.Time
}

func NewRecord() *Record {
	return &Record{
		Fields: make(map[string]interface{}),
		Tags:   make(map[string]string),
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
