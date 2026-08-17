package s3

import (
	"testing"

	"courier-core/internal/s3util"
)

func TestShouldProcess(t *testing.T) {
	if !s3util.ShouldProcess("inbox/a.json", "processed/") {
		t.Error("expected inbox/a.json to be processed")
	}
	if s3util.ShouldProcess("processed/a.json", "processed/") {
		t.Error("expected processed/a.json to be skipped")
	}
	if s3util.ShouldProcess("inbox/", "processed/") {
		t.Error("expected folder marker to be skipped")
	}
	if s3util.ShouldProcess("inbox/a.txt", "processed/") {
		t.Error("expected non-.json key to be skipped")
	}
}

func TestProcessedKey(t *testing.T) {
	if got := s3util.ProcessedKey("processed/", "inbox/a.json"); got != "processed/inbox/a.json" {
		t.Errorf("got %q", got)
	}
}
