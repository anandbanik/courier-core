package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"courier-core/internal/config"
	"courier-core/internal/message"
	"courier-core/internal/transport"
)

type listBucketResult struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
	Contents              []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

// fakeS3Server fakes just enough of ListObjectsV2 and GetObject to drive the
// S3 source's scan loop, without a live S3.
type fakeS3Server struct {
	mu          sync.Mutex
	keys        []string
	maxKeysSeen []int
}

func (f *fakeS3Server) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("list-type") == "2" {
			maxKeys, _ := strconv.Atoi(q.Get("max-keys"))
			if maxKeys <= 0 {
				maxKeys = 1000
			}

			f.mu.Lock()
			f.maxKeysSeen = append(f.maxKeysSeen, maxKeys)
			f.mu.Unlock()

			start := 0
			if tok := q.Get("continuation-token"); tok != "" {
				start, _ = strconv.Atoi(tok)
			}
			end := start + maxKeys
			if end > len(f.keys) {
				end = len(f.keys)
			}

			var res listBucketResult
			for _, k := range f.keys[start:end] {
				res.Contents = append(res.Contents, struct {
					Key string `xml:"Key"`
				}{Key: k})
			}
			if end < len(f.keys) {
				res.IsTruncated = true
				res.NextContinuationToken = strconv.Itoa(end)
			}

			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(res)
			return
		}

		// GetObject: path is /{bucket}/{key...}
		key := strings.TrimPrefix(r.URL.Path, "/testbucket/")
		_, _ = w.Write([]byte(`{"key":"` + key + `"}`))
	}
}

func newTestSource(t *testing.T, endpoint string, maxKeys int) *Source {
	t.Helper()
	src, err := newSource(context.Background(), config.EndpointConfig{
		Transport: config.TransportS3,
		S3: &config.S3Config{
			Bucket:   "testbucket",
			Region:   "us-east-1",
			Endpoint: endpoint,
			MaxKeys:  maxKeys,
		},
	})
	if err != nil {
		t.Fatalf("newSource: %v", err)
	}
	return src.(*Source)
}

func collectingDeliver(delivered *[]string) transport.SinkFunc {
	return func(_ context.Context, msg *message.Message) (*message.Message, error) {
		*delivered = append(*delivered, msg.Headers["s3-key"])
		return nil, nil
	}
}

func TestPollFullScanTraversesAllPagesWithConfiguredPageSize(t *testing.T) {
	fake := &fakeS3Server{keys: []string{
		"inbox/a.json", "inbox/b.json", "inbox/c.json", "inbox/d.json", "inbox/e.json",
	}}
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	src := newTestSource(t, server.URL, 2)

	var delivered []string
	n, err := src.Poll(context.Background(), 0, collectingDeliver(&delivered))
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if n != 5 {
		t.Fatalf("delivered = %d, want 5", n)
	}
	for _, mk := range fake.maxKeysSeen {
		if mk != 2 {
			t.Fatalf("max-keys on the wire = %d, want configured page size 2", mk)
		}
	}
	if len(fake.maxKeysSeen) < 3 {
		t.Fatalf("expected at least 3 pages for 5 keys at page size 2, got %d requests", len(fake.maxKeysSeen))
	}
}

func TestPollBoundedCapStopsEarly(t *testing.T) {
	fake := &fakeS3Server{keys: []string{
		"inbox/a.json", "inbox/b.json", "inbox/c.json", "inbox/d.json", "inbox/e.json",
	}}
	server := httptest.NewServer(fake.handler())
	defer server.Close()

	src := newTestSource(t, server.URL, 1000)

	var delivered []string
	n, err := src.Poll(context.Background(), 2, collectingDeliver(&delivered))
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if n != 2 {
		t.Fatalf("delivered = %d, want 2 (bounded cap)", n)
	}
}

func TestNewSourceDefaultsMaxKeys(t *testing.T) {
	src := newTestSource(t, "http://localhost:0", 0)
	if src.maxKeys != defaultMaxKeys {
		t.Fatalf("maxKeys = %d, want default %d", src.maxKeys, defaultMaxKeys)
	}
}
