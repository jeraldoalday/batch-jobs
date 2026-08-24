package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/jeraldoalday/batch-jobs/internal/job"
)

// ErrNoHandler is returned by a Mux for a job whose queue has no registered
// handler and for which no fallback was set. It is wrapped by Permanent
// before it reaches the pool: a job nothing knows how to run will not
// become runnable by being tried again, so retrying it would only burn the
// job's attempts and delay the operator noticing.
var ErrNoHandler = errors.New("worker: no handler registered for queue")

// Mux dispatches each job to the Handler registered for its Queue,
// so one Pool can serve several logical queues -- the consumer-side
// counterpart to queue.Router, which fans the producer side out to
// several backing queues.
//
// Without it, a process that wants to run "emails" and "reports" with
// different code either runs a Pool per queue (each with its own workers
// and its own idle goroutines) or hand-writes a switch on j.Queue in a
// single handler. Mux is that switch, made safe for concurrent
// registration and introspectable by Queues.
//
// The zero value is ready to use. A Mux must not be copied after first use.
type Mux struct {
	mu       sync.RWMutex
	handlers map[string]Handler
	fallback Handler
}

// NewMux returns an empty Mux. It is equivalent to new(Mux) and exists so
// call sites read as constructions rather than declarations.
func NewMux() *Mux { return &Mux{} }

// Handle registers h as the handler for jobs whose Queue is name. It
// panics if name is empty or h is nil, or if name already has a handler:
// silently replacing one would make a duplicated registration -- two
// packages both claiming "emails" during init, say -- look like it worked
// while half the program's jobs went to code nobody expected.
func (m *Mux) Handle(name string, h Handler) {
	if name == "" {
		panic("worker: Mux.Handle called with an empty queue name")
	}
	if h == nil {
		panic("worker: Mux.Handle called with a nil handler for queue " + name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.handlers[name]; exists {
		panic("worker: Mux.Handle called twice for queue " + name)
	}
	if m.handlers == nil {
		m.handlers = make(map[string]Handler)
	}
	m.handlers[name] = h
}

// HandleFunc is Handle for a plain function, matching net/http's shape so
// the registration reads the same way.
func (m *Mux) HandleFunc(name string, h func(ctx context.Context, j *job.Job) error) {
	m.Handle(name, Handler(h))
}

// Fallback registers h as the handler for jobs whose queue has no handler
// of its own, replacing any previous fallback. Passing nil clears it,
// restoring the default ErrNoHandler behaviour.
//
// It is for processes that route dynamically -- one handler that looks the
// work up by payload, or a catch-all that parks unknown jobs somewhere for
// inspection -- and is deliberately separate from Handle so a typo in a
// queue name cannot silently land on a catch-all that was meant only for
// truly unknown work.
func (m *Mux) Fallback(h Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fallback = h
}

// Handler returns the handler registered for name, or the fallback if
// there is none, along with whether one was found at all.
func (m *Mux) Handler(name string) (Handler, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if h, ok := m.handlers[name]; ok {
		return h, true
	}
	if m.fallback != nil {
		return m.fallback, true
	}
	return nil, false
}

// Queues returns the queue names that have a handler registered, sorted.
// The fallback, which has no name, is not included.
func (m *Mux) Queues() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.handlers))
	for name := range m.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Serve implements Handler, so a *Mux can be given to Pool.Handler
// directly:
//
//	mux := worker.NewMux()
//	mux.Handle("emails", sendEmail)
//	pool := &worker.Pool{Queue: q, Handler: mux.Serve}
func (m *Mux) Serve(ctx context.Context, j *job.Job) error {
	h, ok := m.Handler(j.Queue)
	if !ok {
		return Permanent(fmt.Errorf("%w: %q", ErrNoHandler, j.Queue))
	}
	return h(ctx, j)
}
