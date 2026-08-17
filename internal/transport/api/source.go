package api

import (
	"context"
	"time"

	"courier-core/internal/config"
	"courier-core/internal/message"
	"courier-core/internal/transport"
)

// Source is a stateless push source — it holds no connections.
type Source struct{}

func newSource(_ context.Context, _ config.EndpointConfig) (transport.Source, error) {
	return Source{}, nil
}

func (Source) Kind() string { return "api" }
func (Source) IsPush() bool { return true }
func (Source) Close() error { return nil }

func (Source) Build(_ context.Context, body []byte, headers map[string]string) (*message.Message, error) {
	msg := message.New(body)
	msg.Headers = headers
	msg.Metadata.Source = "api"
	msg.Metadata.ReceivedAt = time.Now()
	return msg, nil
}
