package flow

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"courier-core/internal/config"
	"courier-core/internal/deadletter"
	"courier-core/internal/transform"
	"courier-core/internal/transport"
)

type Manager struct {
	flows []*Flow
	log   *slog.Logger
	wg    sync.WaitGroup
}

// BuildManager iterates cfg.AllFlows() and constructs a Flow for each,
// building its source, sink, transform chain, and dead-letter writer via the
// registry. Any error aborts BuildManager immediately (fail-fast startup).
func BuildManager(ctx context.Context, cfg *config.Config, reg *transport.Registry, log *slog.Logger) (*Manager, error) {
	m := &Manager{log: log}

	for _, fc := range cfg.AllFlows() {
		src, err := reg.BuildSource(ctx, fc.Source)
		if err != nil {
			return nil, err
		}
		sink, err := reg.BuildSink(ctx, fc.Sink)
		if err != nil {
			return nil, err
		}
		tr, err := transform.BuildChain(fc.TransformChain())
		if err != nil {
			return nil, err
		}
		dl, err := deadletter.Build(ctx, fc.DeadLetter)
		if err != nil {
			return nil, err
		}
		stopOnDLFailure := fc.DeadLetter != nil && fc.DeadLetter.StopOnFailure

		var lambdaARN, lambdaBucket, lambdaTopic string
		if fc.LambdaTrigger != nil {
			lambdaARN = fc.LambdaTrigger.SQSQueueARN
			lambdaBucket = fc.LambdaTrigger.S3Bucket
			lambdaTopic = fc.LambdaTrigger.KafkaTopic
		}

		path := cfg.Server.APIPrefix + "/" + fc.Version + "/" + fc.Name

		f := New(fc.Name, fc.Version, path, src, tr, sink,
			newRetryPolicy(fc.Retry), dl, stopOnDLFailure,
			lambdaARN, lambdaBucket, lambdaTopic, fc.Source.S3, log)

		m.flows = append(m.flows, f)
	}

	return m, nil
}

func (m *Manager) Flows() []*Flow { return m.flows }

func (m *Manager) FindFlowByName(name string) *Flow {
	for _, f := range m.flows {
		if f.Name == name {
			return f
		}
	}
	return nil
}

// FindFlowBySQSARN does an exact match against f.lambdaARN only.
func (m *Manager) FindFlowBySQSARN(arn string) *Flow {
	for _, f := range m.flows {
		if f.lambdaARN == arn {
			return f
		}
	}
	return nil
}

// FindFlowByS3Bucket does an exact match against f.lambdaBucket.
func (m *Manager) FindFlowByS3Bucket(bucket string) *Flow {
	for _, f := range m.flows {
		if f.lambdaBucket == bucket {
			return f
		}
	}
	return nil
}

// FindFlowByKafkaTopic does an exact match against f.lambdaTopic.
func (m *Manager) FindFlowByKafkaTopic(topic string) *Flow {
	for _, f := range m.flows {
		if f.lambdaTopic == topic {
			return f
		}
	}
	return nil
}

// Start launches one goroutine per flow; errors other than context.Canceled
// are logged.
func (m *Manager) Start(ctx context.Context) {
	for _, f := range m.flows {
		m.wg.Add(1)
		go func(f *Flow) {
			defer m.wg.Done()
			if err := f.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				m.log.Error("flow loop stopped", "flow", f.Name, "err", err)
			}
		}(f)
	}
}

// Wait blocks until every flow goroutine has returned.
func (m *Manager) Wait() {
	m.wg.Wait()
}

// Close calls Close on every flow, returning the last non-nil error encountered.
func (m *Manager) Close() error {
	var err error
	for _, f := range m.flows {
		if cerr := f.Close(); cerr != nil {
			err = cerr
		}
	}
	return err
}
