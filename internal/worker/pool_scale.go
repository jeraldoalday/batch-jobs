package worker

import (
	"context"
	"time"
)

// DefaultPoolName labels a Pool in metrics when Name is left unset.
const DefaultPoolName = "default"

// autoscaling reports whether the pool resizes itself. Scale.Max is the
// switch: without an explicit ceiling there is no safe upper bound to grow
// toward, so the pool stays the size it was started at.
func (p *Pool) autoscaling() bool {
	return p.Scale.Max > 0
}

// initialSize is how many workers Start spawns: Concurrency, pulled into
// the policy's bounds when autoscaling is enabled so a pool never starts
// outside the range its own scaler would keep it in.
func (p *Pool) initialSize() int {
	if !p.autoscaling() {
		return p.Concurrency
	}

	policy := p.Scale.normalized()
	switch {
	case p.Concurrency < policy.Min:
		return policy.Min
	case p.Concurrency > policy.Max:
		return policy.Max
	default:
		return p.Concurrency
	}
}

// autoscale samples the pool's workload every ScaleInterval and resizes it
// to whatever the Scaler asks for, until ctx is cancelled.
func (p *Pool) autoscale(ctx context.Context) {
	scaler := &Scaler{Policy: p.Scale}

	ticker := time.NewTicker(p.ScaleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.rescale(scaler)
		}
	}
}

// rescale takes one sample and applies the scaler's decision to it. It is
// separated from autoscale's timing loop so tests can drive a single
// decision without waiting on a ticker.
func (p *Pool) rescale(scaler *Scaler) {
	sample := Sample{
		Workers: p.Workers(),
		Busy:    p.Busy(),
		Backlog: p.Queue.Len(),
	}

	target := scaler.Next(sample)
	if target == sample.Workers {
		return
	}

	p.scaleTo(target)
	p.Metrics.ObservePoolScale(p.Name, sample.Workers, target)
	p.Logger.Info("worker: pool resized",
		"from", sample.Workers, "to", target,
		"busy", sample.Busy, "backlog", sample.Backlog)
}

// Workers returns how many worker goroutines the pool currently has.
func (p *Pool) Workers() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

// Busy returns how many workers are currently executing a handler. The
// remainder are blocked in Dequeue waiting for work.
func (p *Pool) Busy() int {
	return int(p.busy.Load())
}

// scaleTo grows or shrinks the pool to exactly n workers. Growing spawns
// goroutines immediately; shrinking asks the surplus workers to stop
// pulling new jobs, but does not wait for them, because one of them may be
// halfway through a handler that should be allowed to finish.
func (p *Pool) scaleTo(n int) {
	if n < 0 {
		n = 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for len(p.workers) < n {
		p.spawnLocked()
	}
	for len(p.workers) > n {
		p.retireLocked()
	}
}

// spawnLocked starts one worker goroutine. Callers must hold p.mu.
func (p *Pool) spawnLocked() {
	dequeueCtx, retire := context.WithCancel(p.poolCtx)

	w := &workerHandle{id: p.nextID, retire: retire}
	p.nextID++
	p.workers = append(p.workers, w)

	p.wg.Add(1)
	go func() {
		// Releasing the context is what keeps a long-lived pool that has
		// scaled up and down many times from leaking one child context
		// per worker it ever started.
		defer retire()
		defer p.wg.Done()
		p.loop(p.poolCtx, dequeueCtx, w.id)
	}()
}

// retireLocked asks the most recently spawned worker to stop. It is removed
// from the pool's roster at once, so the next sizing decision sees the
// smaller pool, even though its goroutine may live on briefly to finish the
// job it is currently running. Callers must hold p.mu.
func (p *Pool) retireLocked() {
	last := len(p.workers) - 1
	if last < 0 {
		return
	}

	w := p.workers[last]
	p.workers = p.workers[:last]
	w.retire()
}
