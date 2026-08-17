package lambda

import (
	"context"
	"encoding/base64"
	"log/slog"

	"github.com/aws/aws-lambda-go/events"

	"courier-core/internal/flow"
	"courier-core/internal/message"
)

type Kafka struct {
	mgr *flow.Manager
	log *slog.Logger
}

func NewKafka(mgr *flow.Manager, log *slog.Logger) *Kafka {
	return &Kafka{mgr: mgr, log: log}
}

func (k *Kafka) Handle(ctx context.Context, event events.KafkaEvent) error {
	for _, records := range event.Records {
		for _, record := range records {
			if err := k.processRecord(ctx, record); err != nil {
				return err
			}
		}
	}
	return nil
}

func (k *Kafka) processRecord(ctx context.Context, record events.KafkaRecord) error {
	f := k.mgr.FindFlowByKafkaTopic(record.Topic)
	if f == nil {
		k.log.Warn("kafka lambda: no flow for topic", "topic", record.Topic)
		return nil
	}

	body := decodeBase64OrRaw(record.Value)
	key := decodeBase64OrRaw(record.Key)

	msg := message.New(body)
	msg.Key = key
	msg.Metadata.Source = "kafka"
	msg.Metadata.ReceivedAt = record.Timestamp.UTC()

	for _, hdrMap := range record.Headers {
		for k, v := range hdrMap {
			msg.Headers[k] = string(v)
		}
	}

	_, err := f.PipelineWithDeadLetter(ctx, msg)
	return err
}

// decodeBase64OrRaw base64-decodes s; on decode failure, falls back to
// treating the raw string as the body/key directly.
func decodeBase64OrRaw(s string) []byte {
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b
	}
	return []byte(s)
}
