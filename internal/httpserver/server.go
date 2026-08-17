package httpserver

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"courier-core/internal/config"
	"courier-core/internal/flow"
)

func New(cfg config.ServerConfig, flows []*flow.Flow, log *slog.Logger) *http.Server {
	mux := NewMux(cfg, flows, log)
	return &http.Server{
		Addr:         cfg.Addr,
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}
}

func NewMux(cfg config.ServerConfig, flows []*flow.Flow, log *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	for _, f := range flows {
		h := f.Handler()
		mux.Handle("GET "+f.Path, h)
		mux.Handle("POST "+f.Path, h)
		log.Info("registered flow endpoint", "path", f.Path, "source", f.SourceKind())
	}
	return mux
}
