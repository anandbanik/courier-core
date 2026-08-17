package custom

import (
	"context"
	"encoding/json"
	"testing"

	"courier-core/internal/config"
	"courier-core/internal/message"
	"courier-core/internal/transform"
)

func TestRegisterAddsEnrich(t *testing.T) {
	r := transform.NewRegistry()
	Register(r)

	if _, err := r.Build(&config.TransformConfig{
		Type:   "enrich",
		Params: map[string]string{"env": "prod"},
	}); err != nil {
		t.Fatalf("enrich not resolvable after Register: %v", err)
	}
}

func TestEnrichMergesFields(t *testing.T) {
	tr, err := newEnrich(map[string]string{"source": "courier", "env": "prod"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	in := message.New([]byte(`{"id":"A1","qty":3}`))
	in.Metadata.FlowName = "enrichedOrders"

	out, err := tr.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(out.Body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for k, want := range map[string]any{
		"id":        "A1",
		"qty":       float64(3),
		"source":    "courier",
		"env":       "prod",
		"flowName":  "enrichedOrders",
		"messageId": in.ID,
	} {
		if got[k] != want {
			t.Errorf("field %q: got %v, want %v", k, got[k], want)
		}
	}
}

func TestEnrichPreservesIDHeadersAndDoesNotMutateInput(t *testing.T) {
	tr, err := newEnrich(map[string]string{"env": "prod"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	in := message.New([]byte(`{"x":1}`))
	in.Headers["k"] = "v"
	originalBody := string(in.Body)

	out, err := tr.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}

	if out.ID != in.ID {
		t.Errorf("ID changed: %q vs %q", out.ID, in.ID)
	}
	if out.Headers["k"] != "v" {
		t.Errorf("headers not preserved: %v", out.Headers)
	}
	if string(in.Body) != originalBody {
		t.Errorf("input body mutated: %q, want %q", string(in.Body), originalBody)
	}
}

// Ack/Nack survive the copy — the flow relies on them for at-least-once delivery.
func TestEnrichPreservesAckHooks(t *testing.T) {
	tr, err := newEnrich(map[string]string{"env": "prod"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	acked := false
	in := message.New([]byte(`{"x":1}`))
	in.Metadata.Ack = func() error { acked = true; return nil }

	out, err := tr.Apply(context.Background(), in)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out.Metadata.Ack == nil {
		t.Fatal("Ack hook lost")
	}
	if err := out.Metadata.Ack(); err != nil || !acked {
		t.Fatal("Ack hook not callable through the copy")
	}
}

func TestEnrichRequiresParams(t *testing.T) {
	if _, err := newEnrich(nil); err == nil {
		t.Fatal("expected error for empty params")
	}
}

func TestEnrichRejectsNonObjectBody(t *testing.T) {
	tr, err := newEnrich(map[string]string{"env": "prod"})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for name, body := range map[string]string{
		"not json":  "not json",
		"json null": "null",
		"array":     `[1,2]`,
		"scalar":    `42`,
	} {
		if _, err := tr.Apply(context.Background(), message.New([]byte(body))); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
