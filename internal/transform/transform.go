package transform

import (
	"context"
	"fmt"

	"courier-core/internal/message"
)

type Transform interface {
	Apply(ctx context.Context, in *message.Message) (*message.Message, error)
}

// Passthrough returns the message unchanged. Default when no transform is configured.
type Passthrough struct{}

func (Passthrough) Apply(_ context.Context, in *message.Message) (*message.Message, error) {
	return in, nil
}

// chain applies a sequence of transforms, threading each output into the next.
type chain []Transform

func (c chain) Apply(ctx context.Context, in *message.Message) (*message.Message, error) {
	msg := in
	for i, t := range c {
		out, err := t.Apply(ctx, msg)
		if err != nil {
			return nil, fmt.Errorf("transform %d: %w", i, err)
		}
		msg = out
	}
	return msg, nil
}
