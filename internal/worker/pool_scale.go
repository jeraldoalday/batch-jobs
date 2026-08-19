package worker

import "context"

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
