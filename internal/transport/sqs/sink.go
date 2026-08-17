package sqs

import (
	"context"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"courier-core/internal/config"
	"courier-core/internal/message"
	"courier-core/internal/transport"
)

type Sink struct {
	client   *sqs.Client
	queueURL string
}

func newSink(ctx context.Context, cfg config.EndpointConfig) (transport.Sink, error) {
	c := cfg.SQS
	client, err := newClient(ctx, c.Region, c.Endpoint)
	if err != nil {
		return nil, err
	}
	return &Sink{client: client, queueURL: c.QueueURL}, nil
}

func (*Sink) Kind() string { return "sqs" }
func (*Sink) Close() error { return nil }

func (s *Sink) Send(ctx context.Context, msg *message.Message) (*message.Message, error) {
	_, err := s.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    awssdk.String(s.queueURL),
		MessageBody: awssdk.String(string(msg.Body)),
	})
	if err != nil {
		return nil, err
	}
	return nil, nil
}
