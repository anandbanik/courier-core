package deadletter

import (
	"testing"
	"time"
)

func TestObjectKey(t *testing.T) {
	ts := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		prefix     string
		flow       string
		dateLayout string
		id         string
		want       string
	}{
		{"no date layout", "pre/", "myflow", "", "abc123", "pre/myflow/abc123.json"},
		{"nested date folders", "pre/", "myflow", "2006/01/02", "abc123", "pre/myflow/2026/08/06/abc123.json"},
		{"flat date segment", "pre/", "myflow", "2006-01-02", "abc123", "pre/myflow/2026-08-06/abc123.json"},
		{"empty prefix", "", "myflow", "", "abc123", "myflow/abc123.json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := objectKey(tt.prefix, tt.flow, tt.dateLayout, ts, tt.id)
			if got != tt.want {
				t.Errorf("objectKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestJSONBody(t *testing.T) {
	if got := string(jsonBody([]byte(`{"a":1}`))); got != `{"a":1}` {
		t.Errorf("valid JSON: got %q", got)
	}
	if got := string(jsonBody([]byte("not json"))); got != `"not json"` {
		t.Errorf("non-JSON: got %q", got)
	}
}
