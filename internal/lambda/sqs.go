package lambda

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-lambda-go/events"

	"courier-core/internal/flow"
	"courier-core/internal/message"
)

type SQS struct {
	mgr *flow.Manager
	log *slog.Logger
}

func NewSQS(mgr *flow.Manager, log *slog.Logger) *SQS {
	return &SQS{mgr: mgr, log: log}
}

func (s *SQS) Handle(ctx context.Context, event events.SQSEvent) (events.SQSEventResponse, error) {
	var failures []events.SQSBatchItemFailure

	for _, record := range event.Records {
		f := s.mgr.FindFlowBySQSARN(record.EventSourceARN)
		if f == nil {
			f = s.mgr.FindFlowByName(queueNameFromARN(record.EventSourceARN))
		}
		if f == nil {
			s.log.Warn("sqs lambda: no flow for queue", "arn", record.EventSourceARN)
			continue
		}

		if err := s.processRecord(ctx, f, record); err != nil {
			failures = append(failures, events.SQSBatchItemFailure{ItemIdentifier: record.MessageId})
		}
	}

	return events.SQSEventResponse{BatchItemFailures: failures}, nil
}

func (s *SQS) processRecord(ctx context.Context, f *flow.Flow, record events.SQSMessage) error {
	msg := message.New([]byte(record.Body))
	msg.Metadata.Source = "sqs"
	msg.Metadata.ReceivedAt = time.Now()

	for k, attr := range record.MessageAttributes {
		if attr.StringValue != nil {
			msg.Headers[k] = *attr.StringValue
		}
	}

	_, err := f.PipelineWithDeadLetter(ctx, msg)
	return err
}

// queueNameFromARN extracts the substring after the last ':' in an ARN.
func queueNameFromARN(arn string) string {
	return splitLast(arn, ':')
}

func splitLast(s string, sep byte) string {
	idx := lastIndex(s, sep)
	if idx < 0 {
		return s
	}
	return s[idx+1:]
}

func lastIndex(s string, sep byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == sep {
			return i
		}
	}
	return -1
}
