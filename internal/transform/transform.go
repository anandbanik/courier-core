package transform

import (
	"context"
	"fmt"

	"courier-core/internal/config"
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

// Build constructs a Transform from a single config. nil/empty/"passthrough" -> Passthrough.
// "template" -> newTemplate(cfg.Params); "jq" -> newJQ(cfg.Params); else error "unknown transform type %q".
func Build(cfg *config.TransformConfig) (Transform, error) {
	if cfg == nil || cfg.Type == "" || cfg.Type == "passthrough" {
		return Passthrough{}, nil
	}
	switch cfg.Type {
	case "template":
		return newTemplate(cfg.Params)
	case "jq":
		return newJQ(cfg.Params)
	default:
		return nil, fmt.Errorf("unknown transform type %q", cfg.Type)
	}
}

// BuildChain: empty -> Passthrough; single entry -> Build(that entry) directly (no chain wrapper);
// multiple entries -> chain{...} threading output->input.
func BuildChain(cfgs []*config.TransformConfig) (Transform, error) {
	if len(cfgs) == 0 {
		return Passthrough{}, nil
	}
	if len(cfgs) == 1 {
		return Build(cfgs[0])
	}
	c := make(chain, 0, len(cfgs))
	for _, cfg := range cfgs {
		t, err := Build(cfg)
		if err != nil {
			return nil, err
		}
		c = append(c, t)
	}
	return c, nil
}
