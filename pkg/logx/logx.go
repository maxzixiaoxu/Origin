// Package logx provides structured JSON logging with request-scoped context
// propagation.
//
// The point of this package is a single question an operator should always be
// able to answer: "what happened to this one job?" A job crosses an HTTP
// request in Rails, a gRPC call into the queue service, a row in Postgres, and
// finally a handler goroutine in some worker container. Those are four
// processes and at least three log streams. Carrying a trace ID through all of
// them, and attaching it automatically rather than at each call site, is what
// makes that question a one-line grep instead of a correlation exercise.
package logx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Header/metadata key used to carry the trace ID across process boundaries.
// Rails emits X-Request-Id by default, so reusing that name means the Rails
// side needs no special configuration and the IDs line up with Rails' own logs.
const TraceHeader = "X-Request-Id"

// MetadataKey is the lowercase gRPC metadata equivalent. gRPC normalises
// metadata keys to lowercase, so the constant is spelled that way to avoid a
// silent miss when reading it back.
const MetadataKey = "x-request-id"

type ctxKey int

const (
	ctxKeyTrace ctxKey = iota
	ctxKeyJob
	ctxKeyWorker
)

// NewTraceID returns a random 128-bit hex ID. Used when a request arrives with
// no upstream trace header, so every unit of work has one regardless of origin.
func NewTraceID() string {
	var b [16]byte
	// crypto/rand.Read never returns an error in practice on supported
	// platforms, and a trace ID is not a security boundary — degrading to a
	// zero-ish ID is strictly better than failing a request over logging.
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// WithTrace returns a context carrying the given trace ID. An empty id is
// ignored so callers can pass a possibly-absent header through unconditionally.
func WithTrace(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyTrace, id)
}

// TraceFrom extracts the trace ID, or "" if none is set.
func TraceFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyTrace).(string)
	return id
}

// EnsureTrace returns a context guaranteed to carry a trace ID, generating one
// if absent, along with the ID itself.
func EnsureTrace(ctx context.Context) (context.Context, string) {
	if id := TraceFrom(ctx); id != "" {
		return ctx, id
	}
	id := NewTraceID()
	return WithTrace(ctx, id), id
}

// WithJob tags a context with the job it belongs to. Every log line emitted
// while executing a handler then carries job_id without the handler author
// having to remember to add it.
func WithJob(ctx context.Context, jobID string) context.Context {
	if jobID == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyJob, jobID)
}

// JobFrom extracts the job ID, or "" if none is set.
func JobFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyJob).(string)
	return id
}

// WithWorker tags a context with the worker identity handling it.
func WithWorker(ctx context.Context, workerID string) context.Context {
	if workerID == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyWorker, workerID)
}

// WorkerFrom extracts the worker ID, or "" if none is set.
func WorkerFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyWorker).(string)
	return id
}

// contextHandler decorates every record with the identifiers stored in its
// context.
//
// Doing this in the handler rather than at call sites is the whole design: it
// is impossible for a log line inside a job handler to be missing its job_id,
// because the handler adds it unconditionally. Correlation that depends on
// developers remembering to pass a field is correlation that has gaps exactly
// where the interesting failures are.
type contextHandler struct{ slog.Handler }

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := TraceFrom(ctx); id != "" {
		r.AddAttrs(slog.String("trace_id", id))
	}
	if id := JobFrom(ctx); id != "" {
		r.AddAttrs(slog.String("job_id", id))
	}
	if id := WorkerFrom(ctx); id != "" {
		r.AddAttrs(slog.String("worker_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs and WithGroup must re-wrap, otherwise logger.With(...) would
// silently strip the context enrichment and only some lines would carry IDs.
func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{h.Handler.WithGroup(name)}
}

// Options configures logger construction.
type Options struct {
	// Level is one of debug, info, warn, error (case-insensitive).
	Level string
	// Service is attached to every line as service=<name>, so a single
	// aggregated stream from docker compose stays separable.
	Service string
	// Text selects human-readable output instead of JSON. Useful when tailing
	// logs locally; JSON is the default because it is what a log pipeline wants.
	Text bool
	// Output defaults to stderr.
	Output io.Writer
}

// ParseLevel maps a level name to a slog.Level, defaulting to Info for
// unrecognised input rather than failing startup over a typo in an env var.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New builds a logger that emits structured records enriched from context.
func New(opts Options) *slog.Logger {
	out := opts.Output
	if out == nil {
		out = os.Stderr
	}

	handlerOpts := &slog.HandlerOptions{Level: ParseLevel(opts.Level)}

	var base slog.Handler
	if opts.Text {
		base = slog.NewTextHandler(out, handlerOpts)
	} else {
		base = slog.NewJSONHandler(out, handlerOpts)
	}

	logger := slog.New(contextHandler{base})
	if opts.Service != "" {
		logger = logger.With(slog.String("service", opts.Service))
	}
	return logger
}

// SetDefault installs the logger as the process default so library code that
// reaches for slog.Info gets the same enrichment and destination.
func SetDefault(l *slog.Logger) { slog.SetDefault(l) }
