package common

import (
	"context"
	"sync"
)

type Runner struct {
	plugins []ScraperPlugin
	wg      sync.WaitGroup
}

func NewRunner(plugins []ScraperPlugin) *Runner {
	return &Runner{plugins: plugins}
}

func (r *Runner) StartAll(ctx context.Context) {
	for _, plugin := range r.plugins {
		p := plugin
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			p.Run(ctx)
		}()
	}
}

func (r *Runner) StopAll() {
	for _, plugin := range r.plugins {
		plugin.Stop()
	}
}

func (r *Runner) Wait() {
	r.wg.Wait()
}
