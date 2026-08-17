package transport

import (
	"context"
	"fmt"

	"courier-core/internal/config"
)

type SourceFactory func(ctx context.Context, cfg config.EndpointConfig) (Source, error)
type SinkFactory func(ctx context.Context, cfg config.EndpointConfig) (Sink, error)

type Registry struct {
	sources map[string]SourceFactory
	sinks   map[string]SinkFactory
}

func NewRegistry() *Registry {
	return &Registry{
		sources: make(map[string]SourceFactory),
		sinks:   make(map[string]SinkFactory),
	}
}

func (r *Registry) RegisterSource(kind string, f SourceFactory) {
	r.sources[kind] = f
}

func (r *Registry) RegisterSink(kind string, f SinkFactory) {
	r.sinks[kind] = f
}

func (r *Registry) BuildSource(ctx context.Context, cfg config.EndpointConfig) (Source, error) {
	f, ok := r.sources[cfg.Transport]
	if !ok {
		return nil, fmt.Errorf("no source transport registered for %q", cfg.Transport)
	}
	return f(ctx, cfg)
}

func (r *Registry) BuildSink(ctx context.Context, cfg config.EndpointConfig) (Sink, error) {
	f, ok := r.sinks[cfg.Transport]
	if !ok {
		return nil, fmt.Errorf("no sink transport registered for %q", cfg.Transport)
	}
	return f(ctx, cfg)
}
