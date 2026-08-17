package s3

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"courier-core/internal/config"
	"courier-core/internal/message"
	"courier-core/internal/s3util"
	"courier-core/internal/transport"
)

const (
	defaultProcessedPrefix     = "processed/"
	defaultPollIntervalSeconds = 30
	defaultMaxKeys             = int32(1000)
)

type Source struct {
	client          *awss3.Client
	bucket          string
	prefix          string
	processedPrefix string
	pollInterval    time.Duration
	maxKeys         int32
}

func newSource(ctx context.Context, cfg config.EndpointConfig) (transport.Source, error) {
	c := cfg.S3
	client, err := newClient(ctx, c.Region, c.Endpoint)
	if err != nil {
		return nil, err
	}

	processedPrefix := c.ProcessedPrefix
	if processedPrefix == "" {
		processedPrefix = defaultProcessedPrefix
	}

	pollSeconds := c.PollIntervalSeconds
	if pollSeconds <= 0 {
		pollSeconds = defaultPollIntervalSeconds
	}

	maxKeys := defaultMaxKeys
	if c.MaxKeys > 0 {
		maxKeys = int32(c.MaxKeys)
	}

	return &Source{
		client:          client,
		bucket:          c.Bucket,
		prefix:          c.Prefix,
		processedPrefix: processedPrefix,
		pollInterval:    time.Duration(pollSeconds) * time.Second,
		maxKeys:         maxKeys,
	}, nil
}

func (*Source) Kind() string { return "s3" }
func (*Source) IsPush() bool { return false }
func (*Source) Close() error { return nil }

// Run scans the whole prefix each cycle, then sleeps pollInterval. Scan
// errors are logged, not fatal — a transient S3 outage should not
// permanently kill the flow.
func (s *Source) Run(ctx context.Context, deliver transport.SinkFunc) error {
	for {
		if _, err := s.scan(ctx, 0, deliver); err != nil {
			slog.Error("s3 source scan failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.pollInterval):
		}
	}
}

func (s *Source) Poll(ctx context.Context, max int, deliver transport.SinkFunc) (int, error) {
	delivered := 0
	wrapped := func(ctx context.Context, msg *message.Message) (*message.Message, error) {
		resp, err := deliver(ctx, msg)
		if err == nil {
			delivered++
		}
		return resp, err
	}
	_, err := s.scan(ctx, max, wrapped)
	return delivered, err
}

func (s *Source) scan(ctx context.Context, max int, deliver transport.SinkFunc) (int, error) {
	delivered := 0
	var continuationToken *string

	for {
		out, err := s.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            awssdk.String(s.bucket),
			Prefix:            awssdk.String(s.prefix),
			MaxKeys:           awssdk.Int32(s.maxKeys),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return delivered, fmt.Errorf("s3 source: list objects: %w", err)
		}

		for _, obj := range out.Contents {
			if max > 0 && delivered >= max {
				return delivered, nil
			}
			key := awssdk.ToString(obj.Key)
			if !s3util.ShouldProcess(key, s.processedPrefix) {
				continue
			}

			body, err := s.getObject(ctx, key)
			if err != nil {
				slog.Warn("s3 source: get object failed, skipping", "key", key, "err", err)
				continue
			}

			msg := message.New(body)
			msg.Headers["s3-key"] = key
			msg.Metadata.Source = "s3"
			msg.Metadata.ReceivedAt = time.Now()
			destKey := s3util.ProcessedKey(s.processedPrefix, key)
			msg.Metadata.Ack = func() error {
				return s.moveObject(ctx, key, destKey)
			}

			if _, err := deliver(ctx, msg); err != nil {
				slog.Warn("s3 source: deliver failed, leaving object in place", "key", key, "err", err)
				continue
			}
			delivered++
		}

		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		continuationToken = out.NextContinuationToken
	}

	return delivered, nil
}

func (s *Source) getObject(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: awssdk.String(s.bucket),
		Key:    awssdk.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

// moveObject copies srcKey to dstKey then deletes srcKey (copy-then-delete
// achieves the at-least-once move).
func (s *Source) moveObject(ctx context.Context, srcKey, dstKey string) error {
	_, err := s.client.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:     awssdk.String(s.bucket),
		CopySource: awssdk.String(s.bucket + "/" + srcKey),
		Key:        awssdk.String(dstKey),
	})
	if err != nil {
		return fmt.Errorf("s3 source: copy object: %w", err)
	}
	_, err = s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: awssdk.String(s.bucket),
		Key:    awssdk.String(srcKey),
	})
	if err != nil {
		return fmt.Errorf("s3 source: delete object: %w", err)
	}
	return nil
}
