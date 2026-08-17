// Package custom holds project-specific transforms — the place to insert
// custom code that reshapes a message payload as it moves through a flow.
//
// To add one: implement transform.Transform in this package (see enrich.go for
// a worked example and the conventions every transform follows), add a Register
// line below choosing the name flows will use as the config "type", then
// reference that name from a flow:
//
//	transforms:
//	  - type: jq
//	    params: {expression: "{id: .orderId}"}
//	  - type: myTransform
//	    params: {someKey: someValue}
//
// Built-in and custom transforms mix freely in one chain, and a flow gets its
// own instances built from its own params. A transform's output feeds the next
// entry in the chain, or the sink if it is the last one — the chain wiring in
// internal/transform handles that, so a transform only has to return its
// message.
//
// The import direction is custom -> transform, never the reverse.
package custom

import "courier-core/internal/transform"

// Register adds this package's transforms to the registry. Called from
// internal/app during startup, which covers both cmd/service and cmd/lambda.
func Register(r *transform.Registry) {
	r.Register("enrich", newEnrich)
}
