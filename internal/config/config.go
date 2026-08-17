package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level YAML shape.
type Config struct {
	Server ServerConfig `yaml:"server"`
	Flows  []FlowConfig `yaml:"flows"`
	Routes []FlowConfig `yaml:"routes"`
}

type ServerConfig struct {
	Addr            string        `yaml:"addr"`
	APIPrefix       string        `yaml:"apiPrefix"`
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout"`
	ReadTimeout     time.Duration `yaml:"readTimeout"`
	WriteTimeout    time.Duration `yaml:"writeTimeout"`
	IdleTimeout     time.Duration `yaml:"idleTimeout"`
}

// AllFlows concatenates Flows then Routes into one slice. The split is
// purely organizational convention in YAML (flows = primarily API-sourced
// standalone endpoints; routes = pull-source-to-sink pipes), not a
// code-level distinction.
func (c *Config) AllFlows() []FlowConfig {
	all := make([]FlowConfig, 0, len(c.Flows)+len(c.Routes))
	all = append(all, c.Flows...)
	all = append(all, c.Routes...)
	return all
}

type FlowConfig struct {
	Name          string               `yaml:"name"`
	Version       string               `yaml:"version"`
	Source        EndpointConfig       `yaml:"source"`
	Sink          EndpointConfig       `yaml:"sink"`
	Transform     *TransformConfig     `yaml:"transform,omitempty"`
	Transforms    []*TransformConfig   `yaml:"transforms,omitempty"`
	Retry         *RetryConfig         `yaml:"retry,omitempty"`
	DeadLetter    *DeadLetterConfig    `yaml:"deadLetter,omitempty"`
	LambdaTrigger *LambdaTriggerConfig `yaml:"lambdaTrigger,omitempty"`
}

// TransformChain: transforms (list) > transform (singular) > nil (passthrough).
// Setting both is rejected by Validate.
func (f FlowConfig) TransformChain() []*TransformConfig {
	if len(f.Transforms) > 0 {
		return f.Transforms
	}
	if f.Transform != nil {
		return []*TransformConfig{f.Transform}
	}
	return nil
}

// LambdaTriggerConfig routes inbound Lambda events (SQS/S3/Kafka triggers)
// to the right flow at runtime; API Gateway flows route by HTTP path
// instead, so they don't need this block.
type LambdaTriggerConfig struct {
	SQSQueueARN string `yaml:"sqsQueueArn,omitempty"`
	S3Bucket    string `yaml:"s3Bucket,omitempty"`
	KafkaTopic  string `yaml:"kafkaTopic,omitempty"`
}

type DeadLetterConfig struct {
	S3            *S3Config `yaml:"s3,omitempty"`
	StopOnFailure bool      `yaml:"stopOnFailure,omitempty"`
}

// S3Config is reused for both an S3 source (uses Bucket, Region, Endpoint,
// Prefix, ProcessedPrefix, PollIntervalSeconds, MaxKeys, APITrigger) and a
// dead-letter target (uses Bucket, Region, Endpoint, Prefix, DateLayout) —
// same struct, different subset of fields relevant per role.
type S3Config struct {
	Bucket              string              `yaml:"bucket"`
	Region              string              `yaml:"region"`
	Endpoint            string              `yaml:"endpoint,omitempty"`
	Prefix              string              `yaml:"prefix,omitempty"`
	DateLayout          string              `yaml:"dateLayout,omitempty"`
	ProcessedPrefix     string              `yaml:"processedPrefix,omitempty"`
	PollIntervalSeconds int                 `yaml:"pollIntervalSeconds,omitempty"`
	MaxKeys             int                 `yaml:"maxKeys,omitempty"`
	APITrigger          *S3APITriggerConfig `yaml:"apiTrigger,omitempty"`
}

// S3APITriggerConfig opts an S3 source flow into an on-demand, uncapped scan
// triggered by a REST POST rather than (or in addition to) the background
// poll loop.
type S3APITriggerConfig struct {
	Enabled         bool `yaml:"enabled"`
	DisablePollLoop bool `yaml:"disablePollLoop,omitempty"`
}

type RetryConfig struct {
	MaxAttempts     int           `yaml:"maxAttempts,omitempty"`
	InitialInterval time.Duration `yaml:"initialInterval,omitempty"`
	MaxInterval     time.Duration `yaml:"maxInterval,omitempty"`
	Multiplier      float64       `yaml:"multiplier,omitempty"`
}

type EndpointConfig struct {
	Transport string       `yaml:"transport"`
	API       *APIConfig   `yaml:"api,omitempty"`
	Kafka     *KafkaConfig `yaml:"kafka,omitempty"`
	SQS       *SQSConfig   `yaml:"sqs,omitempty"`
	S3        *S3Config    `yaml:"s3,omitempty"`
}

type APIConfig struct {
	URL            string            `yaml:"url,omitempty"`
	Method         string            `yaml:"method,omitempty"`
	Headers        map[string]string `yaml:"headers,omitempty"`
	TimeoutSeconds int               `yaml:"timeoutSeconds,omitempty"`
	OAuth          *OAuthConfig      `yaml:"oauth,omitempty"`
}

type OAuthConfig struct {
	TokenURL     string   `yaml:"tokenUrl"`
	ClientID     string   `yaml:"clientId"`
	ClientSecret string   `yaml:"clientSecret"`
	Scopes       []string `yaml:"scopes,omitempty"`
}

type KafkaConfig struct {
	Brokers      []string `yaml:"brokers"`
	Topic        string   `yaml:"topic"`
	GroupID      string   `yaml:"groupId,omitempty"`
	RequiredAcks string   `yaml:"requiredAcks,omitempty"` // none|one|all
}

type SQSConfig struct {
	QueueURL          string `yaml:"queueUrl"`
	Region            string `yaml:"region"`
	Endpoint          string `yaml:"endpoint,omitempty"`
	WaitTimeSeconds   int32  `yaml:"waitTimeSeconds,omitempty"`
	MaxMessages       int32  `yaml:"maxMessages,omitempty"`
	VisibilityTimeout int32  `yaml:"visibilityTimeout,omitempty"`
}

type TransformConfig struct {
	Type   string            `yaml:"type"`
	Params map[string]string `yaml:"params,omitempty"`
}

// Transport kind string constants.
const (
	TransportAPI   = "api"
	TransportKafka = "kafka"
	TransportSQS   = "sqs"
	TransportS3    = "s3"
)

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	expanded, err := expandEnv(data)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(expanded, &c); err != nil {
		return nil, err
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Server.Addr == "" {
		c.Server.Addr = ":8080"
	}
	if c.Server.APIPrefix == "" {
		c.Server.APIPrefix = "/api"
	}
	if c.Server.ShutdownTimeout == 0 {
		c.Server.ShutdownTimeout = 15 * time.Second
	}
	if c.Server.ReadTimeout == 0 {
		c.Server.ReadTimeout = 15 * time.Second
	}
	if c.Server.WriteTimeout == 0 {
		c.Server.WriteTimeout = 60 * time.Second
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = 120 * time.Second
	}
}
