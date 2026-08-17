package deadletter

import (
	"context"
	"fmt"

	"courier-core/internal/config"
	"courier-core/internal/message"
)

// Writer persists msg + failure cause. Called only after all sink retries are
// exhausted; the caller (flow) handles Ack/Nack after Write returns.
type Writer interface {
	Write(ctx context.Context, msg *message.Message, cause error) error
}

// Build: nil cfg -> (nil, nil) (no dead-letter for this flow).
// cfg.S3 != nil -> newS3Writer(ctx, cfg.S3).
// else -> error "deadLetter: no target configured (expected s3 block)".
func Build(ctx context.Context, cfg *config.DeadLetterConfig) (Writer, error) {
	if cfg == nil {
		return nil, nil
	}
	if cfg.S3 != nil {
		return newS3Writer(ctx, cfg.S3)
	}
	return nil, fmt.Errorf("deadLetter: no target configured (expected s3 block)")
}
