package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	awslambda "github.com/aws/aws-lambda-go/lambda"

	"courier-core/internal/app"
	"courier-core/internal/httpserver"
	lambdahandler "courier-core/internal/lambda"
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
	ctx := context.Background()

	a, err := app.New(ctx, cfgPath, log)
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}

	trigger := os.Getenv("EIP_LAMBDA_TRIGGER")
	switch trigger {
	case "apigw":
		mux := httpserver.NewMux(a.Config.Server, a.Manager.Flows(), log)
		awslambda.Start(lambdahandler.NewAPIGW(mux, log).Handle)
	case "sqs":
		awslambda.Start(lambdahandler.NewSQS(a.Manager, log).Handle)
	case "s3":
		awslambda.Start(lambdahandler.NewS3Event(a.Manager, log).Handle)
	case "kafka":
		awslambda.Start(lambdahandler.NewKafka(a.Manager, log).Handle)
	case "apigw+s3":
		mux := httpserver.NewMux(a.Config.Server, a.Manager.Flows(), log)
		s3 := lambdahandler.NewS3Event(a.Manager, log)
		awslambda.Start(lambdahandler.NewCombined(mux, s3, log).Handle)
	default:
		log.Error("EIP_LAMBDA_TRIGGER must be one of: apigw, sqs, s3, kafka, apigw+s3", "got", trigger)
		os.Exit(1)
	}
}

// debugEnabled parses a boolean-ish EIP_DEBUG value (e.g. "true", "1").
// Empty or unparseable values are treated as false.
func debugEnabled(v string) bool {
	b, _ := strconv.ParseBool(v)
	return b
}
