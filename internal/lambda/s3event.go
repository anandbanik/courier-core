package lambda

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-lambda-go/events"
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"courier-core/internal/flow"
	"courier-core/internal/message"
	"courier-core/internal/s3util"
)

type S3Event struct {
	mgr *flow.Manager
	log *slog.Logger
}

func NewS3Event(mgr *flow.Manager, log *slog.Logger) *S3Event {
	return &S3Event{mgr: mgr, log: log}
}

func (a *S3Event) Handle(ctx context.Context, event events.S3Event) error {
	for _, record := range event.Records {
		if err := a.processRecord(ctx, record); err != nil {
			a.log.Error("s3 lambda: object retained", "err", err)
		}
	}
	return nil
}

func (a *S3Event) processRecord(ctx context.Context, record events.S3EventRecord) error {
	bucket := record.S3.Bucket.Name
	key := record.S3.Object.Key

	f := a.mgr.FindFlowByS3Bucket(bucket)
	if f == nil {
		a.log.Warn("s3 lambda: no flow for bucket", "bucket", bucket)
		return nil
	}

	s3cfg := f.SourceS3Config()
	if s3cfg == nil {
		return fmt.Errorf("s3 lambda: flow %q has no s3 source config", f.Name)
	}

	processedPrefix := s3cfg.ProcessedPrefix
	if processedPrefix == "" {
		processedPrefix = "processed/"
	}

	if !s3util.ShouldProcess(key, processedPrefix) {
		return nil
	}

	client, err := buildS3Client(ctx, s3cfg.Region, s3cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("s3 lambda: %w", err)
	}

	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: awssdk.String(bucket),
		Key:    awssdk.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 lambda: get object: %w", err)
	}
	body, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil {
		return fmt.Errorf("s3 lambda: read object: %w", err)
	}

	msg := message.New(body)
	msg.Headers["s3-key"] = key
	msg.Metadata.Source = "s3"
	msg.Metadata.ReceivedAt = time.Now()

	destKey := s3util.ProcessedKey(processedPrefix, key)
	msg.Metadata.Ack = func() error {
		return moveS3Object(ctx, client, bucket, key, destKey)
	}

	_, err = f.PipelineWithDeadLetter(ctx, msg)
	return err
}

func moveS3Object(ctx context.Context, client *awss3.Client, bucket, srcKey, dstKey string) error {
	_, err := client.CopyObject(ctx, &awss3.CopyObjectInput{
		Bucket:     awssdk.String(bucket),
		CopySource: awssdk.String(bucket + "/" + srcKey),
		Key:        awssdk.String(dstKey),
	})
	if err != nil {
		return err
	}
	_, err = client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: awssdk.String(bucket),
		Key:    awssdk.String(srcKey),
	})
	return err
}

func buildS3Client(ctx context.Context, region, endpoint string) (*awss3.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	var opts []func(*awss3.Options)
	if endpoint != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider("test", "test", "")
		opts = append(opts, func(o *awss3.Options) {
			o.BaseEndpoint = awssdk.String(endpoint)
			o.UsePathStyle = true
		})
	}
	return awss3.NewFromConfig(cfg, opts...), nil
}
