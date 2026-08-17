package flow

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"courier-core/internal/config"
	"courier-core/internal/deadletter"
	"courier-core/internal/message"
	"courier-core/internal/transform"
	"courier-core/internal/transport"
)

var errUnknownSource = errors.New("flow has an unknown source type")

type Flow struct {
	Name    string
	Version string
	Path    string

	source          transport.Source
	transform       transform.Transform
	sink            transport.Sink
	retry           retryPolicy
	deadLetter      deadletter.Writer
	stopOnDLFailure bool
	log             *slog.Logger

	// Lambda trigger routing keys (set from LambdaTriggerConfig).
	lambdaARN    string // SQS queue ARN
	lambdaBucket string // S3 bucket name
	lambdaTopic  string // Kafka topic name

	// sourceS3Config is the raw S3Config for this flow's source, set only when
	// the source transport is S3. Used by the S3 Lambda event adapter.
	sourceS3Config *config.S3Config

	processed atomic.Int64
	lastErr   atomic.Pointer[string]
	lastAt    atomic.Pointer[time.Time]
	running   atomic.Bool
}

func New(name, version, path string, src transport.Source, tr transform.Transform, sink transport.Sink,
	retry retryPolicy, dl deadletter.Writer, stopOnDLFailure bool,
	lambdaARN, lambdaBucket, lambdaTopic string, sourceS3Config *config.S3Config, log *slog.Logger) *Flow {
	return &Flow{
		Name:            name,
		Version:         version,
		Path:            path,
		source:          src,
		transform:       tr,
		sink:            sink,
		retry:           retry,
		deadLetter:      dl,
		stopOnDLFailure: stopOnDLFailure,
		lambdaARN:       lambdaARN,
		lambdaBucket:    lambdaBucket,
		lambdaTopic:     lambdaTopic,
		sourceS3Config:  sourceS3Config,
		log:             log.With("flow", name, "version", version),
	}
}

// NewWithRetryConfig builds a Flow the same way New does, but accepts the
// exported *config.RetryConfig (resolved internally via newRetryPolicy)
// instead of the unexported retryPolicy type. Used by callers outside this
// package — e.g. internal/lambda tests that need a *Flow backed by mock
// sources/sinks without going through BuildManager.
func NewWithRetryConfig(name, version, path string, src transport.Source, tr transform.Transform, sink transport.Sink,
	retry *config.RetryConfig, dl deadletter.Writer, stopOnDLFailure bool,
	lambdaARN, lambdaBucket, lambdaTopic string, sourceS3Config *config.S3Config, log *slog.Logger) *Flow {
	return New(name, version, path, src, tr, sink, newRetryPolicy(retry), dl, stopOnDLFailure,
		lambdaARN, lambdaBucket, lambdaTopic, sourceS3Config, log)
}

func (f *Flow) SourceKind() string { return f.source.Kind() }
func (f *Flow) SinkKind() string   { return f.sink.Kind() }

func (f *Flow) SourceS3Config() *config.S3Config { return f.sourceS3Config }

func (f *Flow) LambdaARN() string    { return f.lambdaARN }
func (f *Flow) LambdaBucket() string { return f.lambdaBucket }
func (f *Flow) LambdaTopic() string  { return f.lambdaTopic }

func (f *Flow) apiTriggerFullScan() bool {
	return f.sourceS3Config != nil && f.sourceS3Config.APITrigger != nil && f.sourceS3Config.APITrigger.Enabled
}

func (f *Flow) pollLoopDisabled() bool {
	return f.apiTriggerFullScan() && f.sourceS3Config.APITrigger.DisablePollLoop
}

func (f *Flow) recordErr(err error) {
	s := err.Error()
	f.lastErr.Store(&s)
}

// pipeline is shared by push and pull delivery paths.
func (f *Flow) pipeline(ctx context.Context, msg *message.Message) (*message.Message, error) {
	msg.Metadata.FlowName = f.Name
	out, err := f.transform.Apply(ctx, msg)
	if err != nil {
		f.recordErr(err)
		return nil, err
	}
	resp, err := f.sendWithRetry(ctx, out)
	if err != nil {
		f.recordErr(err)
		if msg.Metadata.Nack != nil {
			msg.Metadata.Nack(err)
		}
		return nil, err
	}
	if msg.Metadata.Ack != nil {
		if ackErr := msg.Metadata.Ack(); ackErr != nil {
			f.log.Warn("ack failed", "err", ackErr)
		}
	}
	f.processed.Add(1)
	now := time.Now()
	f.lastAt.Store(&now)
	return resp, nil
}

// Pipeline is a thin wrapper exposing pipeline to Lambda adapters.
func (f *Flow) Pipeline(ctx context.Context, msg *message.Message) (*message.Message, error) {
	return f.pipeline(ctx, msg)
}

// PipelineWithDeadLetter is used by the pull-style Lambda adapters (SQS, S3
// event, Kafka) in place of deliverPull. A nil error means "handled" — sink
// succeeded, or it failed but was successfully parked in the dead-letter
// target. A non-nil error means the pipeline failed and the message was not
// parked, so the caller reports the failure and lets AWS's own retry
// mechanism take over.
func (f *Flow) PipelineWithDeadLetter(ctx context.Context, msg *message.Message) (*message.Message, error) {
	resp, err := f.pipeline(ctx, msg)
	if err == nil {
		return resp, nil
	}
	if f.deadLetter == nil {
		return nil, err // no DLQ configured: surface to AWS
	}
	if dlErr := f.writeDeadLetter(ctx, msg, err); dlErr != nil {
		f.log.Error("dead-letter write failed", "err", dlErr)
		return nil, err // not parked -> AWS retries
	}
	if msg.Metadata.Ack != nil {
		if ackErr := msg.Metadata.Ack(); ackErr != nil {
			f.log.Warn("ack after dead-letter failed", "err", ackErr)
		}
	}
	return nil, nil // handled (parked in dead-letter)
}

// sendWithRetry retries the sink send with geometric backoff. attempt counts
// the first try (maxAttempts=1 means no retrying at all).
func (f *Flow) sendWithRetry(ctx context.Context, msg *message.Message) (*message.Message, error) {
	backoff := f.retry.initial
	var lastErr error
	for attempt := 1; attempt <= f.retry.maxAttempts; attempt++ {
		resp, err := f.sink.Send(ctx, msg)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if attempt == f.retry.maxAttempts {
			break
		}
		f.log.Warn("sink send failed; retrying", "attempt", attempt, "maxAttempts", f.retry.maxAttempts, "backoff", backoff, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = f.retry.next(backoff)
	}
	return nil, lastErr
}

// deliverPull dead-letters on failure and controls ack timing for pull sources.
func (f *Flow) deliverPull(ctx context.Context, msg *message.Message) (*message.Message, error) {
	resp, err := f.pipeline(ctx, msg)
	if err == nil {
		return resp, nil
	}
	if f.deadLetter == nil {
		return nil, err // no DLQ configured: preserve original fatal behaviour
	}
	if dlErr := f.writeDeadLetter(ctx, msg, err); dlErr != nil {
		f.log.Error("dead-letter write failed", "err", dlErr)
		if f.stopOnDLFailure {
			return nil, err // stop the loop as configured
		}
		return nil, nil // continue; message left unacked -> broker redelivers
	}
	// S3 write succeeded — only now ack so the broker removes the message.
	if msg.Metadata.Ack != nil {
		if ackErr := msg.Metadata.Ack(); ackErr != nil {
			f.log.Warn("ack after dead-letter failed", "err", ackErr)
		}
	}
	return nil, nil
}

func (f *Flow) writeDeadLetter(ctx context.Context, msg *message.Message, cause error) error {
	if f.deadLetter == nil {
		return nil
	}
	return f.deadLetter.Write(ctx, msg, cause)
}

func (f *Flow) Run(ctx context.Context) error {
	ps, ok := f.source.(transport.PullSource)
	if !ok {
		return nil // push sources: no-op
	}
	if f.pollLoopDisabled() {
		return nil // source.s3.apiTrigger.enabled + disablePollLoop: REST-trigger-only
	}
	f.running.Store(true)
	defer f.running.Store(false)
	return ps.Run(ctx, f.deliverPull)
}

func (f *Flow) Close() error {
	_ = f.source.Close()
	return f.sink.Close()
}

func (f *Flow) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			f.handleStatus(w)
		case http.MethodPost:
			f.handlePost(w, r)
		}
	})
}

func (f *Flow) handleStatus(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(f.status())
}

func (f *Flow) handlePost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	switch src := f.source.(type) {
	case transport.PushSource:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err)
			return
		}
		msg, err := src.Build(ctx, body, flattenHeaders(r.Header))
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err)
			return
		}
		resp, err := f.pipeline(ctx, msg)
		if err != nil {
			if dlErr := f.writeDeadLetter(ctx, msg, err); dlErr != nil {
				f.log.Warn("dead-letter write failed", "err", dlErr)
			}
			writeJSONError(w, http.StatusBadGateway, err)
			return
		}
		if resp != nil && len(resp.Body) > 0 {
			if ct, ok := resp.Headers["content-type"]; ok {
				w.Header().Set("Content-Type", ct)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(resp.Body)
			return
		}
		w.WriteHeader(http.StatusAccepted)

	case transport.PullSource:
		max := 10
		if f.apiTriggerFullScan() {
			max = 0
		}
		processed, err := src.Poll(ctx, max, f.deliverPull)
		if err != nil {
			writeJSONError(w, http.StatusBadGateway, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"triggered": true, "processed": processed})

	default:
		writeJSONError(w, http.StatusInternalServerError, errUnknownSource)
	}
}

func writeJSONError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

// flattenHeaders picks the first value per header name (multi-valued headers
// are collapsed to their first value).
func flattenHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k := range h {
		out[k] = h.Get(k)
	}
	return out
}

func (f *Flow) status() map[string]any {
	m := map[string]any{
		"flow":      f.Name,
		"version":   f.Version,
		"source":    f.source.Kind(),
		"sink":      f.sink.Kind(),
		"push":      f.source.IsPush(),
		"running":   f.running.Load(),
		"processed": f.processed.Load(),
	}
	if at := f.lastAt.Load(); at != nil {
		m["lastProcessedAt"] = at.Format(time.RFC3339)
	}
	if errStr := f.lastErr.Load(); errStr != nil {
		m["lastError"] = *errStr
	}
	return m
}
