package transport

import (
	"context"

	"courier-core/internal/message"
)

// SinkFunc is what a source delivers into. The flow supplies this closure, so a
// source never knows about transforms or sinks directly.
type SinkFunc func(ctx context.Context, msg *message.Message) (*message.Message, error)

// Source is the common base for all sources.
type Source interface {
	Kind() string
	IsPush() bool
	Close() error
}

// PushSource is request-driven (API). It builds a Message from an inbound
// request; it does not run a background loop.
type PushSource interface {
	Source
	Build(ctx context.Context, body []byte, headers map[string]string) (*message.Message, error)
}

// PullSource runs a background loop, delivering each received Message.
type PullSource interface {
	Source
	Run(ctx context.Context, deliver SinkFunc) error
	Poll(ctx context.Context, max int, deliver SinkFunc) (processed int, err error)
}

// Sink consumes a Message and optionally returns a response Message.
type Sink interface {
	Kind() string
	Send(ctx context.Context, msg *message.Message) (*message.Message, error)
	Close() error
}
