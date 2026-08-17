package s3

import (
	"courier-core/internal/config"
	"courier-core/internal/transport"
)

// Register registers only a source factory — s3 is a source-only transport.
func Register(r *transport.Registry) {
	r.RegisterSource(config.TransportS3, newSource)
}
