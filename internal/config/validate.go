package config

import "fmt"

func (c *Config) Validate() error {
	all := c.AllFlows()
	seen := make(map[string]bool, len(all))

	for _, f := range all {
		if f.Name == "" {
			return fmt.Errorf("flow has empty name")
		}
		if f.Version == "" {
			return fmt.Errorf("flow %q has empty version", f.Name)
		}
		key := f.Version + "/" + f.Name
		if seen[key] {
			return fmt.Errorf("duplicate flow %q", key)
		}
		seen[key] = true

		if f.Transform != nil && len(f.Transforms) > 0 {
			return fmt.Errorf("flow %q: set either transform or transforms, not both", f.Name)
		}

		if err := validateEndpoint(f.Name, "source", f.Source); err != nil {
			return err
		}
		if err := validateEndpoint(f.Name, "sink", f.Sink); err != nil {
			return err
		}
		if err := validateDeadLetter(f.Name, f.DeadLetter); err != nil {
			return err
		}
	}
	return nil
}

func validateEndpoint(name, role string, e EndpointConfig) error {
	switch e.Transport {
	case "":
		return fmt.Errorf("flow %q: %s missing transport", name, role)
	case TransportAPI:
		if role == "sink" {
			if e.API == nil || e.API.URL == "" {
				return fmt.Errorf("flow %q: api sink requires url", name)
			}
		}
		if e.API != nil && e.API.OAuth != nil {
			o := e.API.OAuth
			if o.TokenURL == "" || o.ClientID == "" || o.ClientSecret == "" {
				return fmt.Errorf("flow %q: api %s oauth requires tokenUrl, clientId, clientSecret", name, role)
			}
		}
	case TransportKafka:
		if e.Kafka == nil || len(e.Kafka.Brokers) == 0 || e.Kafka.Topic == "" {
			return fmt.Errorf("flow %q: kafka %s requires brokers and topic", name, role)
		}
	case TransportSQS:
		if e.SQS == nil || e.SQS.QueueURL == "" || e.SQS.Region == "" {
			return fmt.Errorf("flow %q: sqs %s requires queueUrl and region", name, role)
		}
	case TransportS3:
		if role == "sink" {
			return fmt.Errorf("flow %q: s3 is a source-only transport", name)
		}
		if e.S3 == nil || e.S3.Bucket == "" || e.S3.Region == "" {
			return fmt.Errorf("flow %q: s3 %s requires bucket and region", name, role)
		}
		if e.S3.MaxKeys < 0 {
			return fmt.Errorf("flow %q: s3 %s maxKeys must be >= 0", name, role)
		}
		if e.S3.APITrigger != nil && !e.S3.APITrigger.Enabled && e.S3.APITrigger.DisablePollLoop {
			return fmt.Errorf("flow %q: s3 apiTrigger.disablePollLoop requires apiTrigger.enabled", name)
		}
	default:
		return fmt.Errorf("flow %q: %s unknown transport %q", name, role, e.Transport)
	}
	return nil
}

func validateDeadLetter(name string, dl *DeadLetterConfig) error {
	if dl == nil {
		return nil
	}
	if dl.S3 == nil {
		return fmt.Errorf("deadLetter: no target configured (expected s3 block)")
	}
	if dl.S3.Bucket == "" || dl.S3.Region == "" {
		return fmt.Errorf("flow %q: deadLetter s3 requires bucket and region", name)
	}
	return nil
}
