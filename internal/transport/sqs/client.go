package sqs

import (
	"context"
	"fmt"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func newClient(ctx context.Context, region, endpoint string) (*sqs.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("sqs client: %w", err)
	}

	var opts []func(*sqs.Options)
	if endpoint != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider("test", "test", "")
		opts = append(opts, func(o *sqs.Options) {
			o.BaseEndpoint = awssdk.String(endpoint)
		})
	}

	return sqs.NewFromConfig(cfg, opts...), nil
}
