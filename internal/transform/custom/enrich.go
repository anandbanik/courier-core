package custom

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"courier-core/internal/message"
	"courier-core/internal/transform"
)

// enrichTransform merges static fields from config into a JSON object body,
// alongside the flow name and message ID drawn from the message itself.
//
// It doubles as the reference implementation for custom transforms. Note the
// two conventions it follows:
//
//   - Config is validated in the factory (newEnrich), not in Apply, so a bad
//     flow definition fails at startup rather than on the first message.
//   - Apply copies the message struct instead of mutating the input. That is
//     what preserves ID, Headers, and Metadata — including the Ack/Nack hooks
//     the flow relies on for at-least-once delivery.
type enrichTransform struct {
	fields []enrichField
}

// enrichField is a resolved params entry. Params are held in sorted order so
// the output field order is stable across runs (Go map iteration is not).
type enrichField struct {
	key   string
	value string
}

func newEnrich(params map[string]string) (transform.Transform, error) {
	if len(params) == 0 {
		return nil, fmt.Errorf("enrich transform requires at least one params entry")
	}

	fields := make([]enrichField, 0, len(params))
	for k, v := range params {
		if k == "" {
			return nil, fmt.Errorf("enrich transform: params has an empty key")
		}
		fields = append(fields, enrichField{key: k, value: v})
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].key < fields[j].key })

	return &enrichTransform{fields: fields}, nil
}

func (t *enrichTransform) Apply(_ context.Context, in *message.Message) (*message.Message, error) {
	var body map[string]any
	if err := json.Unmarshal(in.Body, &body); err != nil {
		return nil, fmt.Errorf("enrich transform: body is not a JSON object: %w", err)
	}
	if body == nil {
		// Valid JSON "null" unmarshals into a nil map.
		return nil, fmt.Errorf("enrich transform: body is not a JSON object")
	}

	for _, f := range t.fields {
		body[f.key] = f.value
	}
	body["flowName"] = in.Metadata.FlowName
	body["messageId"] = in.ID

	out := *in
	outBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("enrich transform: %w", err)
	}
	out.Body = outBody
	return &out, nil
}
