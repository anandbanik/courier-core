package deadletter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"courier-core/internal/config"
	"courier-core/internal/message"
)

type s3Writer struct {
	client     *s3.Client
	bucket     string
	prefix     string
	dateLayout string
}

func newS3Writer(ctx context.Context, c *config.S3Config) (*s3Writer, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(c.Region))
	if err != nil {
		return nil, fmt.Errorf("deadletter s3: %w", err)
	}

	var opts []func(*s3.Options)
	if c.Endpoint != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider("test", "test", "")
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = awssdk.String(c.Endpoint)
		})
	}

	client := s3.NewFromConfig(cfg, opts...)
	return &s3Writer{
		client:     client,
		bucket:     c.Bucket,
		prefix:     c.Prefix,
		dateLayout: c.DateLayout,
	}, nil
}

type deadLetterEnvelope struct {
	ID         string            `json:"id"`
	Flow       string            `json:"flow"`
	Source     string            `json:"source"`
	ReceivedAt time.Time         `json:"receivedAt"`
	Error      string            `json:"error"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       json.RawMessage   `json:"body"`
}

func (w *s3Writer) Write(ctx context.Context, msg *message.Message, cause error) error {
	env := deadLetterEnvelope{
		ID:         msg.ID,
		Flow:       msg.Metadata.FlowName,
		Source:     msg.Metadata.Source,
		ReceivedAt: msg.Metadata.ReceivedAt,
		Error:      cause.Error(),
		Headers:    msg.Headers,
		Body:       jsonBody(msg.Body),
	}

	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("deadletter s3: marshal: %w", err)
	}

	key := objectKey(w.prefix, msg.Metadata.FlowName, w.dateLayout, msg.Metadata.ReceivedAt, msg.ID)

	_, err = w.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      awssdk.String(w.bucket),
		Key:         awssdk.String(key),
		Body:        bytes.NewReader(data),
		ContentType: awssdk.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("deadletter s3: put object: %w", err)
	}
	return nil
}

// jsonBody embeds body as raw JSON when it's valid JSON; otherwise wraps it
// as a JSON string so the envelope always marshals cleanly even for
// non-JSON payloads.
func jsonBody(body []byte) json.RawMessage {
	if json.Valid(body) {
		return json.RawMessage(body)
	}
	wrapped, err := json.Marshal(string(body))
	if err != nil {
		return json.RawMessage("null")
	}
	return json.RawMessage(wrapped)
}

// objectKey builds the dead-letter S3 key:
// {prefix}{flow}/[{date}/]{id}.json
// The date segment is included only when dateLayout != "".
func objectKey(prefix, flow, dateLayout string, receivedAt time.Time, id string) string {
	key := prefix + flow + "/"
	if dateLayout != "" {
		key += receivedAt.UTC().Format(dateLayout) + "/"
	}
	key += id + ".json"
	return key
}
