package cron

import (
	"context"
	"time"
)

// Config is the job configuration parameters
type Config struct {
	// Name of Job
	Name string

	// Spec is the cron scheduling spec
	Spec string

	// Number of concurrent instances allowed to run
	Concurrent int64

	// Timeout is the maximum allowed runtime
	Timeout time.Duration
}

// A wrapper that turns a func() into a cron.Job
type FuncJob func(ctx context.Context)

func (f FuncJob) Run(ctx context.Context) { f(ctx) }

// Job is an interface for submitted cron jobs.
type Job interface {
	Run(ctx context.Context)
}
