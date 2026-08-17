package transform

import (
	"context"
	"encoding/json"
	"testing"

	"courier-core/internal/config"
	"courier-core/internal/message"
)

func TestBuildNilIsPassthrough(t *testing.T) {
	tr, err := Build(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := tr.(Passthrough); !ok {
		t.Fatalf("got %T, want Passthrough", tr)
	}
}

func TestBuildUnknownType(t *testing.T) {
	_, err := Build(&config.TransformConfig{Type: "nope"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTemplateTransform(t *testing.T) {
	tr, err := Build(&config.TransformConfig{
		Type:   "template",
		Params: map[string]string{"template": `hello {{.Body.name}}`},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	in := message.New([]byte(`{"name":"world"}`))
	out, err := tr.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if string(out.Body) != "hello world" {
		t.Fatalf("got %q", string(out.Body))
	}
}

func TestTemplateTransformMissingTemplate(t *testing.T) {
	_, err := Build(&config.TransformConfig{Type: "template"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestJQIdentity(t *testing.T) {
	tr, err := Build(&config.TransformConfig{Type: "jq", Params: map[string]string{"expression": "."}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	in := message.New([]byte(`{"x":7}`))
	out, err := tr.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	var got map[string]int
	if err := json.Unmarshal(out.Body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["x"] != 7 {
		t.Fatalf("got %v", got)
	}
}

func TestJQFieldExtraction(t *testing.T) {
	tr, err := Build(&config.TransformConfig{Type: "jq", Params: map[string]string{"expression": ".x"}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	in := message.New([]byte(`{"x":42}`))
	out, err := tr.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if string(out.Body) != "42" {
		t.Fatalf("got %q", string(out.Body))
	}
}

func TestJQReshape(t *testing.T) {
	tr, err := Build(&config.TransformConfig{Type: "jq", Params: map[string]string{"expression": "{a:.x}"}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	in := message.New([]byte(`{"x":7}`))
	out, err := tr.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if string(out.Body) != `{"a":7}` {
		t.Fatalf("got %q", string(out.Body))
	}
}

func TestJQPreservesHeadersIDMetadata(t *testing.T) {
	tr, err := Build(&config.TransformConfig{Type: "jq", Params: map[string]string{"expression": ".x"}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	in := message.New([]byte(`{"x":1}`))
	in.Headers["k"] = "v"
	out, err := tr.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out.ID != in.ID {
		t.Fatalf("ID changed: %q vs %q", out.ID, in.ID)
	}
	if out.Headers["k"] != "v" {
		t.Fatalf("headers not preserved: %v", out.Headers)
	}
}

func TestJQMissingExpression(t *testing.T) {
	_, err := Build(&config.TransformConfig{Type: "jq"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestJQInvalidExpression(t *testing.T) {
	_, err := Build(&config.TransformConfig{Type: "jq", Params: map[string]string{"expression": "..."}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestJQNonJSONBody(t *testing.T) {
	tr, err := Build(&config.TransformConfig{Type: "jq", Params: map[string]string{"expression": "."}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	in := message.New([]byte("not json"))
	_, err = tr.Apply(context.Background(), in)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestJQNoOutput(t *testing.T) {
	tr, err := Build(&config.TransformConfig{Type: "jq", Params: map[string]string{"expression": "empty"}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	in := message.New([]byte(`{"x":1}`))
	_, err = tr.Apply(context.Background(), in)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBuildChainNilIsPassthrough(t *testing.T) {
	tr, err := BuildChain(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := tr.(Passthrough); !ok {
		t.Fatalf("got %T, want Passthrough", tr)
	}
}

func TestBuildChainSingleEntryNotWrapped(t *testing.T) {
	tr, err := BuildChain([]*config.TransformConfig{{Type: "jq", Params: map[string]string{"expression": "."}}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := tr.(*jqTransform); !ok {
		t.Fatalf("got %T, want *jqTransform (no chain wrapper)", tr)
	}
}

func TestBuildChainMultipleEntriesThreaded(t *testing.T) {
	tr, err := BuildChain([]*config.TransformConfig{
		{Type: "jq", Params: map[string]string{"expression": "{a:.x}"}},
		{Type: "jq", Params: map[string]string{"expression": "{b:.a}"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	in := message.New([]byte(`{"x":7}`))
	out, err := tr.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if string(out.Body) != `{"b":7}` {
		t.Fatalf("got %q", string(out.Body))
	}
}

func TestBuildChainErrorPropagatesFromConstruction(t *testing.T) {
	_, err := BuildChain([]*config.TransformConfig{
		{Type: "jq", Params: map[string]string{"expression": "."}},
		{Type: "nope"},
	})
	if err == nil {
		t.Fatal("expected build error to propagate")
	}
}
