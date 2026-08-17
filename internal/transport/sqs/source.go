package sqs

import (
	"context"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"courier-core/internal/config"
	"courier-core/internal/message"
	"courier-core/internal/transport"
)

const (
	defaultWaitTime    = 20
	defaultMaxMessages = 10
)

type Source struct {
	client   *sqs.Client
	queueURL string
	waitTime int32
	maxMsgs  int32
	visTO    int32
}

func newSource(ctx context.Context, cfg config.EndpointConfig) (transport.Source, error) {
	c := cfg.SQS
	client, err := newClient(ctx, c.Region, c.Endpoint)
	if err != nil {
		return nil, err
	}
	return &Source{
		client:   client,
		queueURL: c.QueueURL,
		waitTime: orDefault(c.WaitTimeSeconds, defaultWaitTime),
		maxMsgs:  orDefault(c.MaxMessages, defaultMaxMessages),
		visTO:    c.VisibilityTimeout,
	}, nil
}

func orDefault(v, def int32) int32 {
	if v == 0 {
		return def
	}
	return v
}

func (*Source) Kind() string { return "sqs" }
func (*Source) IsPush() bool { return false }
func (*Source) Close() error { return nil }

func (s *Source) Run(ctx context.Context, deliver transport.SinkFunc) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := s.receive(ctx, s.maxMsgs, s.waitTime, deliver); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
}

func (s *Source) Poll(ctx context.Context, max int, deliver transport.SinkFunc) (int, error) {
	m := int32(max)
	if m < 1 {
		m = 1
	}
	if m > 10 {
		m = 10
	}
	return s.receive(ctx, m, 0, deliver)
}

func (s *Source) receive(ctx context.Context, maxMsgs, waitTime int32, deliver transport.SinkFunc) (int, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            awssdk.String(s.queueURL),
		MaxNumberOfMessages: maxMsgs,
		WaitTimeSeconds:     waitTime,
	}
	if s.visTO > 0 {
		input.VisibilityTimeout = s.visTO
	}

	out, err := s.client.ReceiveMessage(ctx, input)
	if err != nil {
		return 0, err
	}

	n := 0
	for _, m := range out.Messages {
		msg := message.New([]byte(awssdk.ToString(m.Body)))
		msg.Metadata.Source = "sqs"
		msg.Metadata.ReceivedAt = time.Now()
		receiptHandle := awssdk.ToString(m.ReceiptHandle)
		msg.Metadata.Ack = func() error {
			_, err := s.client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
				QueueUrl:      awssdk.String(s.queueURL),
				ReceiptHandle: awssdk.String(receiptHandle),
			})
			return err
		}

		if _, err := deliver(ctx, msg); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
