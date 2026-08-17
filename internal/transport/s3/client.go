package s3

import (
	"context"
	"fmt"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func newClient(ctx context.Context, region, endpoint string) (*awss3.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("s3 client: %w", err)
	}

	var opts []func(*awss3.Options)
	if endpoint != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider("test", "test", "")
		opts = append(opts, func(o *awss3.Options) {
			o.BaseEndpoint = awssdk.String(endpoint)
			o.UsePathStyle = true // required by localstack/minio
		})
	}

	return awss3.NewFromConfig(cfg, opts...), nil
}
