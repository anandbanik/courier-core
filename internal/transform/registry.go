package transform

import (
	"fmt"

	"courier-core/internal/config"
)

// Factory builds a Transform from a flow's transform params. Validation
// belongs here rather than in Apply so bad config fails at startup.
type Factory func(params map[string]string) (Transform, error)

// Registry maps a config transform type to the Factory that builds it.
// Mirrors transport.Registry: instance-based and populated explicitly in the
// composition root (internal/app), not via package init().
type Registry struct {
	factories map[string]Factory
}

// NewRegistry returns a Registry with the built-in transforms already present.
// Project-specific transforms are added on top via Register — see
// internal/transform/custom.
func NewRegistry() *Registry {
	r := &Registry{factories: make(map[string]Factory)}
	r.Register("passthrough", func(map[string]string) (Transform, error) { return Passthrough{}, nil })
	r.Register("template", newTemplate)
	r.Register("jq", newJQ)
	return r
}

// Register adds a factory under typ. Unlike transport.RegisterSource, which
// silently overwrites, a duplicate name panics: a custom transform quietly
// shadowing a built-in like "jq" would be very hard to trace, and registration
// only happens at startup, so this fails the process rather than a request.
func (r *Registry) Register(typ string, f Factory) {
	if _, exists := r.factories[typ]; exists {
		panic(fmt.Sprintf("transform: duplicate registration for type %q", typ))
	}
	r.factories[typ] = f
}

// Build constructs a Transform from a single config. nil/empty/"passthrough"
// -> Passthrough; otherwise the registered factory for cfg.Type, or an
// "unknown transform type %q" error if nothing is registered under that name.
func (r *Registry) Build(cfg *config.TransformConfig) (Transform, error) {
	if cfg == nil || cfg.Type == "" {
		return Passthrough{}, nil
	}
	f, ok := r.factories[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("unknown transform type %q", cfg.Type)
	}
	return f(cfg.Params)
}

// BuildChain: empty -> Passthrough; single entry -> Build(that entry) directly
// (no chain wrapper); multiple entries -> chain{...} threading output->input.
func (r *Registry) BuildChain(cfgs []*config.TransformConfig) (Transform, error) {
	if len(cfgs) == 0 {
		return Passthrough{}, nil
	}
	if len(cfgs) == 1 {
		return r.Build(cfgs[0])
	}
	c := make(chain, 0, len(cfgs))
	for _, cfg := range cfgs {
		t, err := r.Build(cfg)
		if err != nil {
			return nil, err
		}
		c = append(c, t)
	}
	return c, nil
}
