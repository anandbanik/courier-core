package lambda

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/aws/aws-lambda-go/events"

	"courier-core/internal/flow"
)

type APIGW struct {
	mux *http.ServeMux
	log *slog.Logger
}

func NewAPIGW(mux *http.ServeMux, log *slog.Logger) *APIGW {
	return &APIGW{mux: mux, log: log}
}

func (a *APIGW) Handle(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	path := req.RawPath
	if path == "" {
		path = req.RequestContext.HTTP.Path
	}
	url := path
	if req.RawQueryString != "" {
		url += "?" + req.RawQueryString
	}

	httpReq, err := http.NewRequestWithContext(ctx, req.RequestContext.HTTP.Method, url, strings.NewReader(req.Body))
	if err != nil {
		return events.APIGatewayV2HTTPResponse{}, err
	}
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	rr := httptest.NewRecorder()
	a.mux.ServeHTTP(rr, httpReq)

	headers := make(map[string]string, len(rr.Header()))
	for k := range rr.Header() {
		headers[k] = rr.Header().Get(k)
	}

	return events.APIGatewayV2HTTPResponse{
		StatusCode: rr.Code,
		Headers:    headers,
		Body:       rr.Body.String(),
	}, nil
}

// FindFlowForPath is a linear scan matching f.Path == path, exported for
// callers that want to locate a flow before building a mux.
func FindFlowForPath(mgr *flow.Manager, path string) *flow.Flow {
	for _, f := range mgr.Flows() {
		if f.Path == path {
			return f
		}
	}
	return nil
}
