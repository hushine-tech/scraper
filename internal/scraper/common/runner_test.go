package common

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type fakePlugin struct {
	stopped atomic.Bool
}

func (f *fakePlugin) Run(ctx context.Context) {
	<-ctx.Done()
}

func (f *fakePlugin) Stop() {
	f.stopped.Store(true)
}

func (f *fakePlugin) Kind() string   { return "kline" }
func (f *fakePlugin) Market() string { return "spot" }
func (f *fakePlugin) Symbol() string { return "btcusdt" }

func TestRunnerGracefulStop(t *testing.T) {
	p := &fakePlugin{}
	r := NewRunner([]ScraperPlugin{p})

	ctx, cancel := context.WithCancel(context.Background())
	r.StartAll(ctx)
	time.Sleep(10 * time.Millisecond)
	cancel()
	r.StopAll()
	r.Wait()

	if !p.stopped.Load() {
		t.Fatal("expected plugin Stop to be called")
	}
}
