package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"courier-core/internal/config"
	"courier-core/internal/message"
	"courier-core/internal/transport"
)

const defaultTimeout = 30 * time.Second

type Sink struct {
	url     string
	method  string
	headers map[string]string
	client  *http.Client
}

// oauthLoggingTransport logs the OAuth token endpoint's HTTP status at debug level.
type oauthLoggingTransport struct {
	base http.RoundTripper
}

func (t oauthLoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err == nil {
		slog.Debug("oauth token request", "url", req.URL.String(), "status", resp.StatusCode)
	}
	return resp, err
}

func newSink(ctx context.Context, cfg config.EndpointConfig) (transport.Sink, error) {
	c := cfg.API
	if c == nil || c.URL == "" {
		return nil, fmt.Errorf("api sink requires url")
	}

	method := c.Method
	if method == "" {
		method = http.MethodPost
	}

	timeout := defaultTimeout
	if c.TimeoutSeconds > 0 {
		timeout = time.Duration(c.TimeoutSeconds) * time.Second
	}

	var httpClient *http.Client
	if c.OAuth != nil {
		ccCfg := &clientcredentials.Config{
			ClientID:     c.OAuth.ClientID,
			ClientSecret: c.OAuth.ClientSecret,
			TokenURL:     c.OAuth.TokenURL,
			Scopes:       c.OAuth.Scopes,
		}
		base := &http.Client{Timeout: timeout, Transport: oauthLoggingTransport{base: http.DefaultTransport}}
		httpClient = ccCfg.Client(context.WithValue(ctx, oauth2.HTTPClient, base))
		httpClient.Timeout = timeout
	} else {
		httpClient = &http.Client{Timeout: timeout}
	}

	return &Sink{
		url:     c.URL,
		method:  method,
		headers: c.Headers,
		client:  httpClient,
	}, nil
}

func (Sink) Kind() string    { return "api" }
func (s *Sink) Close() error { return nil }

func (s *Sink) Send(ctx context.Context, msg *message.Message) (*message.Message, error) {
	req, err := http.NewRequestWithContext(ctx, s.method, s.url, bytes.NewReader(msg.Body))
	if err != nil {
		return nil, fmt.Errorf("http sink: %w", err)
	}

	for k, v := range msg.Headers {
		req.Header.Set(k, v)
	}
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http sink: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("http sink: %w", err)
	}

	slog.Info("http sink response", "status", resp.StatusCode)
	slog.Debug("http sink body", "body", truncate(body, 512))

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http sink: status %d: %s", resp.StatusCode, truncate(body, 512))
	}

	out := message.New(body)
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		out.Headers["content-type"] = ct
	}
	return out, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n])
}
