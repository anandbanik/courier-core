package api

import (
	"courier-core/internal/config"
	"courier-core/internal/transport"
)

func Register(r *transport.Registry) {
	r.RegisterSource(config.TransportAPI, newSource)
	r.RegisterSink(config.TransportAPI, newSink)
}
