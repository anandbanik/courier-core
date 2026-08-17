package transform

import (
	"context"
	"errors"
	"testing"

	"courier-core/internal/config"
	"courier-core/internal/message"
)

// suffixTransform stands in for a custom transform in registry tests: it
// appends its configured suffix to the body so chain ordering is observable.
type suffixTransform struct{ suffix string }

func (t suffixTransform) Apply(_ context.Context, in *message.Message) (*message.Message, error) {
	out := *in
	out.Body = append(append([]byte{}, in.Body...), t.suffix...)
	return &out, nil
}

func newSuffix(params map[string]string) (Transform, error) {
	s, ok := params["suffix"]
	if !ok || s == "" {
		return nil, errors.New("suffix transform requires params.suffix")
	}
	return suffixTransform{suffix: s}, nil
}

func TestRegistryBuiltinsRegistered(t *testing.T) {
	r := NewRegistry()
	for _, typ := range []string{"passthrough", "template", "jq"} {
		if _, ok := r.factories[typ]; !ok {
			t.Errorf("built-in %q not registered", typ)
		}
	}
}

func TestRegistryPassthroughByType(t *testing.T) {
	tr, err := NewRegistry().Build(&config.TransformConfig{Type: "passthrough"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, ok := tr.(Passthrough); !ok {
		t.Fatalf("got %T, want Passthrough", tr)
	}
}

func TestRegistryCustomTypeResolvable(t *testing.T) {
	r := NewRegistry()
	r.Register("suffix", newSuffix)

	tr, err := r.Build(&config.TransformConfig{Type: "suffix", Params: map[string]string{"suffix": "!"}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	out, err := tr.Apply(context.Background(), message.New([]byte("hi")))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if string(out.Body) != "hi!" {
		t.Fatalf("got %q, want %q", string(out.Body), "hi!")
	}
}

// Two custom transforms coexist, and each flow's registry entry is built with
// that flow's own params — the same type used twice yields separate instances.
func TestRegistryMultipleCustomTypes(t *testing.T) {
	r := NewRegistry()
	r.Register("suffix", newSuffix)
	r.Register("suffix2", newSuffix)

	a, err := r.Build(&config.TransformConfig{Type: "suffix", Params: map[string]string{"suffix": "-a"}})
	if err != nil {
		t.Fatalf("build a: %v", err)
	}
	b, err := r.Build(&config.TransformConfig{Type: "suffix2", Params: map[string]string{"suffix": "-b"}})
	if err != nil {
		t.Fatalf("build b: %v", err)
	}
	if a.(suffixTransform).suffix == b.(suffixTransform).suffix {
		t.Fatal("instances share params; want independent construction")
	}
}

func TestRegistryDuplicateRegistrationPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	r := NewRegistry()
	r.Register("jq", newSuffix) // shadowing a built-in must fail loudly
}

func TestRegistryUnknownTypeError(t *testing.T) {
	_, err := NewRegistry().Build(&config.TransformConfig{Type: "nope"})
	if err == nil {
		t.Fatal("expected error")
	}
	if want := `unknown transform type "nope"`; err.Error() != want {
		t.Fatalf("got %q, want %q", err.Error(), want)
	}
}

func TestRegistryFactoryErrorPropagates(t *testing.T) {
	r := NewRegistry()
	r.Register("suffix", newSuffix)

	// params.suffix missing -> factory error, surfaced at startup.
	_, err := r.BuildChain([]*config.TransformConfig{
		{Type: "jq", Params: map[string]string{"expression": "."}},
		{Type: "suffix"},
	})
	if err == nil {
		t.Fatal("expected factory error to propagate")
	}
}

// A built-in and a custom transform mixed in one chain: the custom step must
// see the built-in's output, not the original body.
func TestRegistryChainMixesBuiltinAndCustom(t *testing.T) {
	r := NewRegistry()
	r.Register("suffix", newSuffix)

	tr, err := r.BuildChain([]*config.TransformConfig{
		{Type: "jq", Params: map[string]string{"expression": ".x"}},
		{Type: "suffix", Params: map[string]string{"suffix": "!"}},
	})
	if err != nil {
		t.Fatalf("build chain: %v", err)
	}
	out, err := tr.Apply(context.Background(), message.New([]byte(`{"x":42}`)))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if string(out.Body) != "42!" {
		t.Fatalf("got %q, want %q", string(out.Body), "42!")
	}
}
