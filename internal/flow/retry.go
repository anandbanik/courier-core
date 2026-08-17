package flow

import (
	"time"

	"courier-core/internal/config"
)

type retryPolicy struct {
	maxAttempts int
	initial     time.Duration
	max         time.Duration
	multiplier  float64
}

// newRetryPolicy builds a retryPolicy from a config.RetryConfig. Defaults:
// maxAttempts=1, initial=100ms, max=5s, multiplier=2.0. A nil RetryConfig
// block yields this default policy (retrying disabled). Each non-zero field
// in the config overrides its corresponding default.
func newRetryPolicy(cfg *config.RetryConfig) retryPolicy {
	p := retryPolicy{
		maxAttempts: 1,
		initial:     100 * time.Millisecond,
		max:         5 * time.Second,
		multiplier:  2.0,
	}
	if cfg == nil {
		return p
	}
	if cfg.MaxAttempts != 0 {
		p.maxAttempts = cfg.MaxAttempts
	}
	if cfg.InitialInterval != 0 {
		p.initial = cfg.InitialInterval
	}
	if cfg.MaxInterval != 0 {
		p.max = cfg.MaxInterval
	}
	if cfg.Multiplier != 0 {
		p.multiplier = cfg.Multiplier
	}
	return p
}

func (p retryPolicy) next(cur time.Duration) time.Duration {
	n := time.Duration(float64(cur) * p.multiplier)
	if n > p.max {
		return p.max
	}
	return n
}
