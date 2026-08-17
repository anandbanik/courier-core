package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"courier-core/internal/app"
)

func main() {
	cfgPath := os.Getenv("EIP_CONFIG")
	if cfgPath == "" {
		cfgPath = "configs/example.yaml"
	}

	level := slog.LevelInfo
	if debugEnabled(os.Getenv("EIP_DEBUG")) {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	log.Debug("Log Level", "debug", level == slog.LevelDebug)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.New(ctx, cfgPath, log)
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}

	a.Manager.Start(ctx)

	go func() {
		log.Info("http server listening", "addr", a.Server.Addr)
		if err := a.Server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.Config.Server.ShutdownTimeout)
	defer cancel()

	if err := a.Server.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", "err", err)
	}
	a.Manager.Wait()
	if err := a.Manager.Close(); err != nil {
		log.Error("transport close error", "err", err)
	}
	log.Info("stopped")
}

// debugEnabled parses a boolean-ish EIP_DEBUG value (e.g. "true", "1").
// Empty or unparseable values are treated as false.
func debugEnabled(v string) bool {
	b, _ := strconv.ParseBool(v)
	return b
}
