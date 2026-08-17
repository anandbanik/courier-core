package config

import (
	"strings"
	"testing"
)

func TestExpandEnvDefaultUnused(t *testing.T) {
	t.Setenv("EIP_TEST_VAR", "from-env")
	out, err := expandEnv([]byte(`val: ${EIP_TEST_VAR:-fallback}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := string(out); got != "val: from-env" {
		t.Fatalf("got %q, want %q", got, "val: from-env")
	}
}

func TestExpandEnvUsesDefaultWhenUnset(t *testing.T) {
	out, err := expandEnv([]byte(`val: ${EIP_TEST_VAR_UNSET:-fallback}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := string(out); got != "val: fallback" {
		t.Fatalf("got %q, want %q", got, "val: fallback")
	}
}

func TestExpandEnvMissingNoDefaultErrors(t *testing.T) {
	_, err := expandEnv([]byte(`val: ${EIP_TEST_VAR_MISSING}`))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "EIP_TEST_VAR_MISSING") {
		t.Fatalf("error %q does not name missing var", err.Error())
	}
}

func TestExpandEnvMultipleMissingNamedInOneError(t *testing.T) {
	_, err := expandEnv([]byte(`a: ${MISSING_ONE}\nb: ${MISSING_TWO}`))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "MISSING_ONE") || !strings.Contains(err.Error(), "MISSING_TWO") {
		t.Fatalf("error %q does not name both missing vars", err.Error())
	}
}

func TestExpandEnvNoPlaceholderPassesThrough(t *testing.T) {
	in := "val: plain-string"
	out, err := expandEnv([]byte(in))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(out) != in {
		t.Fatalf("got %q, want %q", string(out), in)
	}
}
