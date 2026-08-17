// Package app is the composition root shared by cmd/service and cmd/lambda:
// it loads config, builds the transport registry and flow manager, and
// constructs (but does not start) the HTTP server.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"courier-core/internal/config"
	"courier-core/internal/flow"
	"courier-core/internal/httpserver"
	"courier-core/internal/transform"
	"courier-core/internal/transform/custom"
	"courier-core/internal/transport"
	"courier-core/internal/transport/api"
	"courier-core/internal/transport/kafka"
	s3transport "courier-core/internal/transport/s3"
	"courier-core/internal/transport/sqs"
)

type App struct {
	Config  *config.Config
	Manager *flow.Manager
	Server  *http.Server
}

func New(ctx context.Context, cfgPath string, log *slog.Logger) (*App, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("app: load config: %w", err)
	}

	reg := transport.NewRegistry()
	api.Register(reg)
	kafka.Register(reg)
	sqs.Register(reg)
	s3transport.Register(reg)

	treg := transform.NewRegistry()
	custom.Register(treg)

	mgr, err := flow.BuildManager(ctx, cfg, reg, treg, log)
	if err != nil {
		return nil, fmt.Errorf("app: build manager: %w", err)
	}

	server := httpserver.New(cfg.Server, mgr.Flows(), log)

	return &App{
		Config:  cfg,
		Manager: mgr,
		Server:  server,
	}, nil
}
