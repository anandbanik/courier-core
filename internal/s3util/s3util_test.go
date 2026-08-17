package s3util

import "testing"

func TestShouldProcess(t *testing.T) {
	tests := []struct {
		key             string
		processedPrefix string
		want            bool
	}{
		{"inbox/a.json", "processed/", true},
		{"inbox/", "processed/", false},
		{"inbox/a.txt", "processed/", false},
		{"processed/a.json", "processed/", false},
		{"inbox/a.json", "", true},
	}
	for _, tt := range tests {
		if got := ShouldProcess(tt.key, tt.processedPrefix); got != tt.want {
			t.Errorf("ShouldProcess(%q, %q) = %v, want %v", tt.key, tt.processedPrefix, got, tt.want)
		}
	}
}

func TestProcessedKey(t *testing.T) {
	if got := ProcessedKey("processed/", "inbox/a.json"); got != "processed/inbox/a.json" {
		t.Errorf("got %q", got)
	}
}
