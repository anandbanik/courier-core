package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"text/template"

	"courier-core/internal/message"
)

type templateContext struct {
	Body    any
	Headers map[string]string
}

type templateTransform struct {
	tmpl *template.Template
}

func newTemplate(params map[string]string) (Transform, error) {
	src, ok := params["template"]
	if !ok || src == "" {
		return nil, fmt.Errorf("template transform requires params.template")
	}
	tmpl, err := template.New("transform").Parse(src)
	if err != nil {
		return nil, fmt.Errorf("template transform: %w", err)
	}
	return &templateTransform{tmpl: tmpl}, nil
}

func (t *templateTransform) Apply(_ context.Context, in *message.Message) (*message.Message, error) {
	var body any
	if err := json.Unmarshal(in.Body, &body); err != nil {
		body = string(in.Body)
	}

	tctx := templateContext{Body: body, Headers: in.Headers}

	var buf bytes.Buffer
	if err := t.tmpl.Execute(&buf, tctx); err != nil {
		return nil, fmt.Errorf("template transform: %w", err)
	}

	out := *in
	out.Body = buf.Bytes()
	return &out, nil
}
