package kafka

import (
	"context"
	"errors"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"courier-core/internal/config"
	"courier-core/internal/message"
	"courier-core/internal/transport"
)

type Source struct {
	reader *kafkago.Reader
}

func newSource(_ context.Context, cfg config.EndpointConfig) (transport.Source, error) {
	c := cfg.Kafka
	readerCfg := kafkago.ReaderConfig{
		Brokers: c.Brokers,
		Topic:   c.Topic,
		GroupID: c.GroupID,
	}
	if c.GroupID == "" {
		readerCfg.StartOffset = kafkago.LastOffset
	}
	return &Source{reader: kafkago.NewReader(readerCfg)}, nil
}

func (*Source) Kind() string   { return "kafka" }
func (*Source) IsPush() bool   { return false }
func (s *Source) Close() error { return s.reader.Close() }

func (s *Source) Run(ctx context.Context, deliver transport.SinkFunc) error {
	for {
		if err := s.consumeOne(ctx, deliver); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
}

func (s *Source) Poll(ctx context.Context, max int, deliver transport.SinkFunc) (int, error) {
	processed := 0
	for i := 0; i < max; i++ {
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := s.consumeOne(pollCtx, deliver)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func (s *Source) consumeOne(ctx context.Context, deliver transport.SinkFunc) error {
	m, err := s.reader.FetchMessage(ctx)
	if err != nil {
		return err
	}

	msg := message.New(m.Value)
	msg.Key = m.Key
	msg.Headers = headersToMap(m.Headers)
	msg.Metadata.Source = "kafka"
	msg.Metadata.ReceivedAt = time.Now()
	msg.Metadata.Ack = func() error {
		return s.reader.CommitMessages(context.Background(), m)
	}

	_, err = deliver(ctx, msg)
	return err
}

func headersToMap(hdrs []kafkago.Header) map[string]string {
	m := make(map[string]string, len(hdrs))
	for _, h := range hdrs {
		m[h.Key] = string(h.Value)
	}
	return m
}
