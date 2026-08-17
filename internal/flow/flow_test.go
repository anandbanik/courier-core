package flow

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"courier-core/internal/deadletter"
	"courier-core/internal/message"
	"courier-core/internal/transform"
	"courier-core/internal/transport"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mockPushSource is a minimal transport.PushSource.
type mockPushSource struct{}

func (mockPushSource) Kind() string { return "mock-push" }
func (mockPushSource) IsPush() bool { return true }
func (mockPushSource) Close() error { return nil }
func (mockPushSource) Build(_ context.Context, body []byte, headers map[string]string) (*message.Message, error) {
	msg := message.New(body)
	msg.Headers = headers
	return msg, nil
}

// mockPullSource is a minimal transport.PullSource; Run/Poll are unused by
// the tests below (which call f.deliverPull directly) but are required to
// satisfy the interface.
type mockPullSource struct{}

func (mockPullSource) Kind() string                                      { return "mock-pull" }
func (mockPullSource) IsPush() bool                                      { return false }
func (mockPullSource) Close() error                                      { return nil }
func (mockPullSource) Run(_ context.Context, _ transport.SinkFunc) error { return nil }
func (mockPullSource) Poll(_ context.Context, _ int, _ transport.SinkFunc) (int, error) {
	return 0, nil
}

// mockSink always succeeds.
type mockSink struct {
	sent []*message.Message
}

func (*mockSink) Kind() string { return "mock" }
func (*mockSink) Close() error { return nil }
func (s *mockSink) Send(_ context.Context, msg *message.Message) (*message.Message, error) {
	s.sent = append(s.sent, msg)
	return message.New([]byte("ok")), nil
}

// flakySink fails failTimes times then succeeds; tracks attempts.
type flakySink struct {
	failTimes int
	attempts  int
}

func (*flakySink) Kind() string { return "flaky" }
func (*flakySink) Close() error { return nil }
func (s *flakySink) Send(_ context.Context, _ *message.Message) (*message.Message, error) {
	s.attempts++
	if s.attempts <= s.failTimes {
		return nil, errors.New("flaky: simulated failure")
	}
	return message.New([]byte("ok")), nil
}

// mockDeadLetter records writes; succeeds unless failWrite is true.
type mockDeadLetter struct {
	failWrite bool
	writes    int
}

func (d *mockDeadLetter) Write(_ context.Context, _ *message.Message, _ error) error {
	d.writes++
	if d.failWrite {
		return errors.New("dead-letter write failed")
	}
	return nil
}

func newTestFlow(sink transport.Sink, retry retryPolicy, dl *mockDeadLetter, stopOnDLFailure bool) *Flow {
	var dlWriter deadletter.Writer
	if dl != nil {
		dlWriter = dl
	}
	return New("testflow", "v1", "/api/v1/testflow", mockPushSource{}, transform.Passthrough{}, sink,
		retry, dlWriter, stopOnDLFailure, "", "", "", nil, testLogger())
}

func fastRetry() retryPolicy {
	return retryPolicy{maxAttempts: 3, initial: time.Millisecond, max: time.Millisecond, multiplier: 2.0}
}

func TestPipelineRetriesThenSucceeds(t *testing.T) {
	sink := &flakySink{failTimes: 2}
	f := newTestFlow(sink, fastRetry(), nil, false)

	_, err := f.pipeline(context.Background(), message.New([]byte("hi")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sink.attempts != 3 {
		t.Fatalf("attempts = %d, want 3", sink.attempts)
	}
}

func TestPipelineExhaustsRetriesThenNacks(t *testing.T) {
	sink := &flakySink{failTimes: 100}
	f := newTestFlow(sink, fastRetry(), nil, false)

	msg := message.New([]byte("hi"))
	var nackErr error
	nacked := false
	msg.Metadata.Nack = func(err error) {
		nacked = true
		nackErr = err
	}

	_, err := f.pipeline(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error")
	}
	if sink.attempts != 3 {
		t.Fatalf("attempts = %d, want 3", sink.attempts)
	}
	if !nacked {
		t.Fatal("expected Nack to be called")
	}
	if nackErr == nil {
		t.Fatal("expected Nack to receive the error")
	}
}

func TestDeliverPullFailsNoDeadLetter(t *testing.T) {
	sink := &flakySink{failTimes: 100}
	f := newTestFlow(sink, retryPolicy{maxAttempts: 1, initial: time.Millisecond, max: time.Millisecond, multiplier: 2}, nil, false)

	_, err := f.deliverPull(context.Background(), message.New([]byte("hi")))
	if err == nil {
		t.Fatal("expected original error to be returned")
	}
}

func TestDeliverPullFailsDeadLetterSucceeds(t *testing.T) {
	sink := &flakySink{failTimes: 100}
	dl := &mockDeadLetter{}
	f := newTestFlow(sink, retryPolicy{maxAttempts: 1, initial: time.Millisecond, max: time.Millisecond, multiplier: 2}, dl, false)

	msg := message.New([]byte("hi"))
	acked := false
	msg.Metadata.Ack = func() error { acked = true; return nil }

	resp, err := f.deliverPull(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != nil {
		t.Fatalf("expected nil response, got %v", resp)
	}
	if !acked {
		t.Fatal("expected Ack to be called after successful dead-letter write")
	}
	if dl.writes != 1 {
		t.Fatalf("dead-letter writes = %d, want 1", dl.writes)
	}
}

func TestDeliverPullFailsDeadLetterFailsNoStop(t *testing.T) {
	sink := &flakySink{failTimes: 100}
	dl := &mockDeadLetter{failWrite: true}
	f := newTestFlow(sink, retryPolicy{maxAttempts: 1, initial: time.Millisecond, max: time.Millisecond, multiplier: 2}, dl, false)

	msg := message.New([]byte("hi"))
	acked := false
	msg.Metadata.Ack = func() error { acked = true; return nil }

	resp, err := f.deliverPull(context.Background(), msg)
	if err != nil {
		t.Fatalf("expected nil error (loop continues), got %v", err)
	}
	if resp != nil {
		t.Fatalf("expected nil response, got %v", resp)
	}
	if acked {
		t.Fatal("expected Ack NOT to be called when dead-letter write fails")
	}
}

func TestDeliverPullFailsDeadLetterFailsStopOnFailure(t *testing.T) {
	sink := &flakySink{failTimes: 100}
	dl := &mockDeadLetter{failWrite: true}
	f := newTestFlow(sink, retryPolicy{maxAttempts: 1, initial: time.Millisecond, max: time.Millisecond, multiplier: 2}, dl, true)

	_, err := f.deliverPull(context.Background(), message.New([]byte("hi")))
	if err == nil {
		t.Fatal("expected the original sink error to be returned")
	}
}

func TestPipelineWithDeadLetterHandledOnDLSuccess(t *testing.T) {
	sink := &flakySink{failTimes: 100}
	dl := &mockDeadLetter{}
	f := newTestFlow(sink, retryPolicy{maxAttempts: 1, initial: time.Millisecond, max: time.Millisecond, multiplier: 2}, dl, false)

	_, err := f.PipelineWithDeadLetter(context.Background(), message.New([]byte("hi")))
	if err != nil {
		t.Fatalf("expected nil error (handled), got %v", err)
	}
}

func TestPipelineWithDeadLetterSurfacesOnDLFailure(t *testing.T) {
	sink := &flakySink{failTimes: 100}
	dl := &mockDeadLetter{failWrite: true}
	f := newTestFlow(sink, retryPolicy{maxAttempts: 1, initial: time.Millisecond, max: time.Millisecond, multiplier: 2}, dl, false)

	_, err := f.PipelineWithDeadLetter(context.Background(), message.New([]byte("hi")))
	if err == nil {
		t.Fatal("expected error to surface to caller when dead-letter write fails")
	}
}

func TestPipelineAcksOnSuccess(t *testing.T) {
	sink := &mockSink{}
	f := newTestFlow(sink, retryPolicy{maxAttempts: 1, initial: time.Millisecond, max: time.Millisecond, multiplier: 2}, nil, false)

	msg := message.New([]byte("hi"))
	acked := false
	msg.Metadata.Ack = func() error { acked = true; return nil }

	if _, err := f.pipeline(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !acked {
		t.Fatal("expected Ack to be called on success")
	}
}
