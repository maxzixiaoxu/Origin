// Package handlers holds the work the queue actually performs.
package handlers

import (
	"context"
	"sort"
	"sync"

	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
)

// Handler executes one job.
//
// The returned bytes become the job's stored result and are shown in the
// dashboard, so they should be small structured JSON -- dimensions, timings,
// output keys -- never the output itself.
//
// The context carries the job's deadline AND is cancelled if the worker loses
// the job's lease. Handlers must respect it. A handler that ignores
// cancellation keeps burning CPU on work another worker has already taken over,
// which is how one slow host turns into fleet-wide duplicated effort.
type Handler interface {
	Handle(ctx context.Context, job jobtypes.Envelope) ([]byte, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, job jobtypes.Envelope) ([]byte, error)

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, job jobtypes.Envelope) ([]byte, error) {
	return f(ctx, job)
}

// Registry maps job types to handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register adds a handler, panicking on a duplicate type.
//
// Panicking is right here: registration happens once at startup from static
// code, so a duplicate is a programming error the developer should see
// immediately, not a runtime condition to be handled. Silently overwriting
// would mean jobs quietly routing to whichever handler registered last.
func (r *Registry) Register(jobType string, h Handler) {
	if jobType == "" {
		panic("handlers: cannot register an empty job type")
	}
	if h == nil {
		panic("handlers: cannot register a nil handler for " + jobType)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[jobType]; exists {
		panic("handlers: duplicate registration for job type " + jobType)
	}
	r.handlers[jobType] = h
}

// RegisterFunc is Register for a plain function.
func (r *Registry) RegisterFunc(jobType string, f HandlerFunc) {
	r.Register(jobType, f)
}

// Lookup returns the handler for a job type.
func (r *Registry) Lookup(jobType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[jobType]
	return h, ok
}

// Types lists registered job types, sorted, for startup logging.
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// Dispatch runs the handler for a job.
//
// An unknown job type is a PERMANENT error, not a retryable one. This is the
// single most important classification decision in the package: a worker that
// does not recognise a job type will never recognise it, so retrying five times
// with backoff just delays the inevitable while occupying a slot. Sending it
// straight to the dead-letter queue surfaces the deploy mismatch immediately --
// which is what it usually is, a job type shipped to Rails before the workers
// that handle it.
//
// The counter-argument is real and worth knowing: during a rolling deploy, old
// workers legitimately see new job types. Routing those to dead-letter means
// replaying them afterwards. That is the accepted cost, because the alternative
// -- retrying until max_attempts on every worker -- burns the job's entire
// retry budget during the rollout and dead-letters it anyway, just later and
// with a less obvious error.
func (r *Registry) Dispatch(ctx context.Context, job jobtypes.Envelope) ([]byte, error) {
	h, ok := r.Lookup(job.Type)
	if !ok {
		return nil, jobtypes.Permanentf(
			"no handler registered for job type %q (registered: %v)",
			job.Type, r.Types())
	}
	return h.Handle(ctx, job)
}

// MustDecodePayload unmarshals a job payload, classifying failure as permanent.
//
// A payload that will not parse now will not parse on retry either -- the bytes
// are immutable once the job is created.
func MustDecodePayload(job jobtypes.Envelope, dst any, unmarshal func([]byte, any) error) error {
	if err := unmarshal(job.Payload, dst); err != nil {
		return jobtypes.Permanentf("job %s has an unreadable payload: %v", job.ID, err)
	}
	return nil
}
