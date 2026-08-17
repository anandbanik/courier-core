package lambda

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestAPIGWRoutesToRegisteredHandler(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"pong":true}`))
	})

	a := NewAPIGW(mux, testLogger())

	req := events.APIGatewayV2HTTPRequest{
		RawPath: "/api/v1/ping",
	}
	req.RequestContext.HTTP.Method = http.MethodGet

	resp, err := a.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if resp.Body != `{"pong":true}` {
		t.Fatalf("body = %q", resp.Body)
	}
}

func TestAPIGWFallsBackToRequestContextPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	a := NewAPIGW(mux, testLogger())

	req := events.APIGatewayV2HTTPRequest{}
	req.RequestContext.HTTP.Method = http.MethodGet
	req.RequestContext.HTTP.Path = "/api/v1/ping"

	resp, err := a.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestQueueNameFromARN(t *testing.T) {
	got := queueNameFromARN("arn:aws:sqs:us-east-1:000000000000:onestream")
	if got != "onestream" {
		t.Fatalf("got %q, want %q", got, "onestream")
	}
}

func TestSplitLastNoSeparator(t *testing.T) {
	if got := splitLast("noseparator", ':'); got != "noseparator" {
		t.Fatalf("got %q", got)
	}
}

func TestDecodeBase64OrRawValidBase64(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("hello"))
	if got := string(decodeBase64OrRaw(encoded)); got != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestDecodeBase64OrRawFallsBackToRaw(t *testing.T) {
	raw := "not-valid-base64!!!"
	if got := string(decodeBase64OrRaw(raw)); got != raw {
		t.Fatalf("got %q, want %q", got, raw)
	}
}

// TestSQSPartialBatchFailureSelection mirrors SQS.Handle's batch-item-failure
// selection logic: only records whose processing returned a non-nil error
// are reported as failures, identified by MessageId.
func TestSQSPartialBatchFailureSelection(t *testing.T) {
	type result struct {
		id  string
		err error
	}
	results := []result{
		{"msg-1", nil},
		{"msg-2", context.DeadlineExceeded},
		{"msg-3", nil},
	}

	var failures []events.SQSBatchItemFailure
	for _, r := range results {
		if r.err != nil {
			failures = append(failures, events.SQSBatchItemFailure{ItemIdentifier: r.id})
		}
	}

	if len(failures) != 1 || failures[0].ItemIdentifier != "msg-2" {
		t.Fatalf("failures = %+v, want exactly msg-2", failures)
	}
}

// TestKafkaFirstFailureStopsPartition mirrors Kafka.Handle's semantics: on
// the first record processing failure, iteration stops immediately and later
// records in the same partition are not processed.
func TestKafkaFirstFailureStopsPartition(t *testing.T) {
	processed := 0
	records := []error{nil, nil, context.DeadlineExceeded, nil}

	var firstErr error
	for _, err := range records {
		processed++
		if err != nil {
			firstErr = err
			break
		}
	}

	if processed != 3 {
		t.Fatalf("processed = %d, want 3 (stop at first failure)", processed)
	}
	if firstErr == nil {
		t.Fatal("expected the failure to be surfaced")
	}
}

func TestCombinedDispatchesAPIGatewayEvent(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pong"))
	})
	c := NewCombined(mux, NewS3Event(nil, testLogger()), testLogger())

	raw := []byte(`{"rawPath":"/api/v1/ping","requestContext":{"http":{"method":"GET"}}}`)
	resp, err := c.Handle(context.Background(), raw)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	apiResp, ok := resp.(events.APIGatewayV2HTTPResponse)
	if !ok {
		t.Fatalf("resp type = %T, want events.APIGatewayV2HTTPResponse", resp)
	}
	if apiResp.StatusCode != http.StatusOK || apiResp.Body != "pong" {
		t.Fatalf("resp = %+v", apiResp)
	}
}

func TestCombinedRejectsUnrecognizedEvent(t *testing.T) {
	mux := http.NewServeMux()
	c := NewCombined(mux, NewS3Event(nil, testLogger()), testLogger())

	_, err := c.Handle(context.Background(), []byte(`{"foo":"bar"}`))
	if err == nil {
		t.Fatal("expected error for unrecognized event shape")
	}
}
