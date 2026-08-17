package kafka

import (
	"courier-core/internal/config"
	"courier-core/internal/transport"
)

func Register(r *transport.Registry) {
	r.RegisterSource(config.TransportKafka, newSource)
	r.RegisterSink(config.TransportKafka, newSink)
}
