package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
)

type Combined struct {
	apigw   *APIGW
	s3event *S3Event
	log     *slog.Logger
}

func NewCombined(mux *http.ServeMux, s3 *S3Event, log *slog.Logger) *Combined {
	return &Combined{
		apigw:   NewAPIGW(mux, log),
		s3event: s3,
		log:     log,
	}
}

type probe struct {
	RequestContext struct {
		HTTP struct {
			Method string `json:"method"`
		} `json:"http"`
	} `json:"requestContext"`
	Records []struct {
		EventSource string `json:"eventSource"`
	} `json:"Records"`
}

func (c *Combined) Handle(ctx context.Context, raw json.RawMessage) (any, error) {
	var p probe
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("combined: %w", err)
	}

	switch {
	case p.RequestContext.HTTP.Method != "":
		var req events.APIGatewayV2HTTPRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, fmt.Errorf("combined: %w", err)
		}
		return c.apigw.Handle(ctx, req)

	case len(p.Records) > 0 && p.Records[0].EventSource == "aws:s3":
		var event events.S3Event
		if err := json.Unmarshal(raw, &event); err != nil {
			return nil, fmt.Errorf("combined: %w", err)
		}
		return nil, c.s3event.Handle(ctx, event)

	default:
		return nil, fmt.Errorf("combined: unrecognized event shape")
	}
}
