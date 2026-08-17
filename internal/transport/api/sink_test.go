package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"courier-core/internal/config"
	"courier-core/internal/message"
)

func TestSinkPostsBodyVerbatimAndAppliesHeaders(t *testing.T) {
	var gotBody []byte
	var gotHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotHeader = r.Header.Get("X-Configured")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	sinkIface, err := newSink(context.Background(), config.EndpointConfig{
		API: &config.APIConfig{
			URL:     server.URL,
			Headers: map[string]string{"X-Configured": "yes"},
		},
	})
	if err != nil {
		t.Fatalf("newSink: %v", err)
	}
	sink := sinkIface.(*Sink)

	in := message.New([]byte(`{"hello":"world"}`))
	out, err := sink.Send(context.Background(), in)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	if string(gotBody) != `{"hello":"world"}` {
		t.Fatalf("downstream received body %q", string(gotBody))
	}
	if gotHeader != "yes" {
		t.Fatalf("downstream did not receive configured header, got %q", gotHeader)
	}
	if string(out.Body) != `{"ok":true}` {
		t.Fatalf("response body = %q", string(out.Body))
	}
	if out.Headers["content-type"] != "application/json" {
		t.Fatalf("response content-type = %q", out.Headers["content-type"])
	}
}

func TestSinkNon2xxIsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer server.Close()

	sinkIface, err := newSink(context.Background(), config.EndpointConfig{
		API: &config.APIConfig{URL: server.URL},
	})
	if err != nil {
		t.Fatalf("newSink: %v", err)
	}
	sink := sinkIface.(*Sink)

	_, err = sink.Send(context.Background(), message.New([]byte("hi")))
	if err == nil {
		t.Fatal("expected error for non-2xx response")
	}
}
