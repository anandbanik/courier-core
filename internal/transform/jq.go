package transform

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/itchyny/gojq"

	"courier-core/internal/message"
)

type jqTransform struct {
	query      *gojq.Query
	expression string
}

func newJQ(params map[string]string) (Transform, error) {
	expr, ok := params["expression"]
	if !ok || expr == "" {
		return nil, fmt.Errorf("jq transform requires params.expression")
	}
	query, err := gojq.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("jq transform: %w", err)
	}
	return &jqTransform{query: query, expression: expr}, nil
}

func (t *jqTransform) Apply(ctx context.Context, in *message.Message) (*message.Message, error) {
	var input any
	if err := json.Unmarshal(in.Body, &input); err != nil {
		return nil, fmt.Errorf("jq transform: %w", err)
	}

	iter := t.query.Run(input)
	v, ok := iter.Next()
	if !ok {
		return nil, fmt.Errorf("jq transform: expression %q produced no output", t.expression)
	}
	if err, isErr := v.(error); isErr {
		return nil, fmt.Errorf("jq transform: %w", err)
	}

	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("jq transform: %w", err)
	}

	out := *in
	out.Body = body
	return &out, nil
}
