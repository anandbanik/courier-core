package kafka

import (
	"context"

	kafkago "github.com/segmentio/kafka-go"

	"courier-core/internal/config"
	"courier-core/internal/message"
	"courier-core/internal/transport"
)

type Sink struct {
	writer *kafkago.Writer
}

func newSink(_ context.Context, cfg config.EndpointConfig) (transport.Sink, error) {
	c := cfg.Kafka
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(c.Brokers...),
		Topic:        c.Topic,
		Balancer:     &kafkago.LeastBytes{},
		RequiredAcks: requiredAcks(c.RequiredAcks),
	}
	return &Sink{writer: writer}, nil
}

func requiredAcks(v string) kafkago.RequiredAcks {
	switch v {
	case "none":
		return kafkago.RequireNone
	case "all":
		return kafkago.RequireAll
	default:
		return kafkago.RequireOne
	}
}

func (*Sink) Kind() string   { return "kafka" }
func (s *Sink) Close() error { return s.writer.Close() }

func (s *Sink) Send(ctx context.Context, msg *message.Message) (*message.Message, error) {
	kmsg := kafkago.Message{
		Key:     msg.Key,
		Value:   msg.Body,
		Headers: mapToHeaders(msg.Headers),
	}
	if err := s.writer.WriteMessages(ctx, kmsg); err != nil {
		return nil, err
	}
	return nil, nil
}

func mapToHeaders(m map[string]string) []kafkago.Header {
	hdrs := make([]kafkago.Header, 0, len(m))
	for k, v := range m {
		hdrs = append(hdrs, kafkago.Header{Key: k, Value: []byte(v)})
	}
	return hdrs
}
