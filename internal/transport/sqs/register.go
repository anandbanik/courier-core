package sqs

import (
	"courier-core/internal/config"
	"courier-core/internal/transport"
)

func Register(r *transport.Registry) {
	r.RegisterSource(config.TransportSQS, newSource)
	r.RegisterSink(config.TransportSQS, newSink)
}
