package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/maxzixiaoxu/Origin/pkg/logx"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/broker"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

// REST gateway for the Rails admin.
//
// Rails could speak gRPC, but the Ruby gRPC toolchain is fiddly to build in a
// container for very little gain: the admin app makes a handful of calls per
// request and none are latency-critical. Workers get gRPC because they call
// constantly; Rails gets JSON because it is simpler to operate and trivial to
// poke with curl during a demo.
//
// This surface is deliberately WRITE-ONLY plus single-job reads. Rails does not
// list or filter jobs through here -- it queries Postgres directly with
// read-only ActiveRecord models, because reimplementing pagination, sorting,
// and filtering as Go endpoints would be a lot of work to end up with something
// worse than ActiveRecord. Go owns every write; Rails owns the read path.

// RESTOptions configures the gateway.
type RESTOptions struct {
	Broker   *broker.Broker
	Log      *slog.Logger
	Instance string
	LeaderFn func() map[string]bool
}

// RESTHandler serves the admin JSON API.
type RESTHandler struct {
	broker   *broker.Broker
	log      *slog.Logger
	instance string
	leaderFn func() map[string]bool
}

// NewRESTHandler builds the gateway.
func NewRESTHandler(opts RESTOptions) (*RESTHandler, error) {
	if opts.Broker == nil {
		return nil, errors.New("api: Broker is required")
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &RESTHandler{
		broker:   opts.Broker,
		log:      log,
		instance: opts.Instance,
		leaderFn: opts.LeaderFn,
	}, nil
}

// Routes returns the HTTP handler.
func (h *RESTHandler) Routes() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.Recoverer)
	// RequestID before RealIP so every log line has one even for a request
	// rejected early.
	r.Use(middleware.RequestID)
	r.Use(h.traceMiddleware)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", h.healthz)
	r.Get("/readyz", h.readyz)

	r.Route("/v1", func(r chi.Router) {
		r.Post("/jobs", h.enqueue)
		r.Post("/jobs/batch", h.enqueueBatch)
		r.Get("/jobs/{id}", h.getJob)
		r.Post("/jobs/{id}/cancel", h.cancelJob)
		r.Post("/jobs/{id}/retry", h.retryJob)

		r.Get("/queues", h.listQueues)
		r.Put("/queues/{name}", h.upsertQueue)
		r.Post("/queues/{name}/pause", h.pauseQueue)
		r.Post("/queues/{name}/resume", h.resumeQueue)

		r.Get("/depth", h.depth)
	})

	return r
}

// traceMiddleware adopts the caller's X-Request-Id, or mints one.
//
// Rails sets this header on every outbound call, so a single id ties the
// browser request, the Rails log, this service's log, the job row, and
// eventually the worker that runs it. Generating one when absent means the
// chain is never broken by a caller that forgot.
func (h *RESTHandler) traceMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.Header.Get(logx.TraceHeader)
		ctx, traceID := logx.EnsureTrace(logx.WithTrace(r.Context(), traceID))

		// Echoed back so the caller can correlate from its own side too.
		w.Header().Set(logx.TraceHeader, traceID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// --- responses ------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if body != nil {
		_ = json.NewEncoder(w).Encode(body)
	}
}

// errorBody is the single error shape every failure returns, so the Rails
// client has one thing to parse rather than guessing per endpoint.
type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

func (h *RESTHandler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := http.StatusInternalServerError, "internal"

	switch {
	case errors.Is(err, store.ErrNotFound):
		status, code = http.StatusNotFound, "not_found"
	case errors.Is(err, broker.ErrLeaseLost):
		status, code = http.StatusConflict, "lease_lost"
	}

	if status >= 500 {
		h.log.ErrorContext(r.Context(), "request failed",
			"method", r.Method, "path", r.URL.Path, "error", err)
	}

	writeJSON(w, status, errorBody{Error: err.Error(), Code: code})
}

func (h *RESTHandler) badRequest(w http.ResponseWriter, msg string) {
	writeJSON(w, http.StatusBadRequest, errorBody{Error: msg, Code: "bad_request"})
}

// decodeBody reads a JSON body with a size limit.
//
// Unbounded decoding of a request body is a denial-of-service in one line: a
// client streaming an endless body would consume memory until the process died.
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) error {
	const maxBody = 4 << 20 // 4 MiB

	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	dec := json.NewDecoder(r.Body)
	// Reject unknown fields so a typo in a client payload is an error rather
	// than a silently ignored setting -- "priority" vs "prority" would
	// otherwise look like it worked.
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

// --- health ---------------------------------------------------------------

func (h *RESTHandler) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (h *RESTHandler) readyz(w http.ResponseWriter, r *http.Request) {
	redisErr := h.broker.Redis().Ping(r.Context()).Err()
	pgErr := h.broker.Store().Ping(r.Context())

	leading := map[string]bool{}
	if h.leaderFn != nil {
		leading = h.leaderFn()
	}

	status := http.StatusOK
	if redisErr != nil || pgErr != nil {
		status = http.StatusServiceUnavailable
	}

	writeJSON(w, status, map[string]any{
		"instance": h.instance,
		"redis":    errString(redisErr),
		"postgres": errString(pgErr),
		"loops":    leading,
	})
}

func errString(err error) string {
	if err != nil {
		return err.Error()
	}
	return "ok"
}

// --- jobs -----------------------------------------------------------------

type enqueueBody struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       *int            `json:"priority,omitempty"`
	MaxAttempts    *int            `json:"max_attempts,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
}

func (b enqueueBody) toRequest(traceID string) broker.EnqueueRequest {
	req := broker.EnqueueRequest{
		Queue:          b.Queue,
		Type:           b.Type,
		Payload:        b.Payload,
		Priority:       b.Priority,
		MaxAttempts:    b.MaxAttempts,
		IdempotencyKey: b.IdempotencyKey,
		TraceID:        traceID,
	}
	if b.RunAt != nil {
		req.RunAt = *b.RunAt
	}
	return req
}

type enqueueResponse struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	Deduplicated bool   `json:"deduplicated"`
	QueueDepth   int64  `json:"queue_depth"`
}

func (h *RESTHandler) enqueue(w http.ResponseWriter, r *http.Request) {
	var body enqueueBody
	if err := decodeBody(w, r, &body); err != nil {
		h.badRequest(w, err.Error())
		return
	}
	if body.Queue == "" || body.Type == "" {
		h.badRequest(w, "queue and type are required")
		return
	}

	res, err := h.broker.Enqueue(r.Context(), body.toRequest(logx.TraceFrom(r.Context())))
	if err != nil {
		h.writeError(w, r, err)
		return
	}

	// 200 rather than 201 for a deduplicated submission: nothing was created,
	// and a client retrying a request it thought failed should be able to tell
	// the difference.
	status := http.StatusCreated
	if res.Deduplicated {
		status = http.StatusOK
	}

	writeJSON(w, status, enqueueResponse{
		ID:           res.JobID,
		Status:       string(res.Status),
		Deduplicated: res.Deduplicated,
		QueueDepth:   res.Depth,
	})
}

// enqueueBatch submits many jobs, reporting per-item outcomes.
//
// This is what the image upload uses: one call creates all three derivative
// jobs. Doing it as three sequential requests would let a mid-flight Rails
// crash leave an image with only some of its sizes queued.
func (h *RESTHandler) enqueueBatch(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Jobs []enqueueBody `json:"jobs"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		h.badRequest(w, err.Error())
		return
	}
	if len(body.Jobs) == 0 {
		h.badRequest(w, "jobs must not be empty")
		return
	}
	if len(body.Jobs) > 1000 {
		h.badRequest(w, "at most 1000 jobs per batch")
		return
	}

	traceID := logx.TraceFrom(r.Context())

	type item struct {
		Index    int              `json:"index"`
		Response *enqueueResponse `json:"response,omitempty"`
		Error    string           `json:"error,omitempty"`
	}

	results := make([]item, 0, len(body.Jobs))
	var succeeded, failed int

	for i, jb := range body.Jobs {
		if jb.Queue == "" || jb.Type == "" {
			failed++
			results = append(results, item{Index: i, Error: "queue and type are required"})
			continue
		}

		res, err := h.broker.Enqueue(r.Context(), jb.toRequest(traceID))
		if err != nil {
			failed++
			results = append(results, item{Index: i, Error: err.Error()})
			continue
		}

		succeeded++
		results = append(results, item{Index: i, Response: &enqueueResponse{
			ID:           res.JobID,
			Status:       string(res.Status),
			Deduplicated: res.Deduplicated,
			QueueDepth:   res.Depth,
		}})
	}

	// 207 when the batch was mixed. A flat 201 would tell the caller everything
	// worked when some of it did not.
	status := http.StatusCreated
	if failed > 0 {
		status = http.StatusMultiStatus
	}

	writeJSON(w, status, map[string]any{
		"succeeded": succeeded,
		"failed":    failed,
		"results":   results,
	})
}

func (h *RESTHandler) getJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	job, err := h.broker.Store().GetJob(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}

	body := map[string]any{"job": jobToMap(job)}

	if r.URL.Query().Get("attempts") == "true" {
		attempts, err := h.broker.Store().ListAttempts(r.Context(), id)
		if err != nil {
			h.writeError(w, r, err)
			return
		}
		list := make([]map[string]any, 0, len(attempts))
		for _, a := range attempts {
			list = append(list, attemptToMap(a))
		}
		body["attempts"] = list
	}

	writeJSON(w, http.StatusOK, body)
}

func (h *RESTHandler) cancelJob(w http.ResponseWriter, r *http.Request) {
	cancelled, err := h.broker.Cancel(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cancelled": cancelled})
}

func (h *RESTHandler) retryJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	// Default to resetting the attempt counter. A human clicking "retry" on a
	// dead job wants it to actually run, not to fail once more against an
	// already-exhausted budget.
	reset := r.URL.Query().Get("reset") != "false"

	if err := h.broker.Retry(r.Context(), id, reset); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeJSON(w, http.StatusConflict, errorBody{
				Error: "job is not in a replayable state",
				Code:  "not_replayable",
			})
			return
		}
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": true})
}

// --- queues ---------------------------------------------------------------

func (h *RESTHandler) listQueues(w http.ResponseWriter, r *http.Request) {
	queues, err := h.broker.Store().ListQueues(r.Context())
	if err != nil {
		h.writeError(w, r, err)
		return
	}

	out := make([]map[string]any, 0, len(queues))
	for _, q := range queues {
		out = append(out, queueToMap(q))
	}
	writeJSON(w, http.StatusOK, map[string]any{"queues": out})
}

type queueBody struct {
	MaxConcurrency    *int    `json:"max_concurrency,omitempty"`
	RateLimitPerSec   *int    `json:"rate_limit_per_sec,omitempty"`
	RateLimitBurst    *int    `json:"rate_limit_burst,omitempty"`
	MaxAttempts       *int    `json:"max_attempts,omitempty"`
	VisibilityTimeout *int    `json:"visibility_timeout_sec,omitempty"`
	BackoffBaseMS     *int    `json:"backoff_base_ms,omitempty"`
	BackoffCapMS      *int    `json:"backoff_cap_ms,omitempty"`
	Paused            *bool   `json:"paused,omitempty"`
	Description       *string `json:"description,omitempty"`
}

func (h *RESTHandler) upsertQueue(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if err := broker.ValidateQueueName(name); err != nil {
		h.badRequest(w, err.Error())
		return
	}

	var body queueBody
	if err := decodeBody(w, r, &body); err != nil {
		h.badRequest(w, err.Error())
		return
	}

	// Every field is a pointer and unset fields are left alone, so the UI can
	// send just the one setting being changed. Round-tripping the whole config
	// would create a lost-update race between two operators editing the same
	// queue -- which is exactly when two people are most likely to be doing so.
	cfg := &store.QueueConfig{Name: name}
	if body.MaxConcurrency != nil {
		cfg.MaxConcurrency = *body.MaxConcurrency
	}
	if body.MaxAttempts != nil {
		cfg.MaxAttempts = *body.MaxAttempts
	}
	if body.VisibilityTimeout != nil {
		cfg.VisibilityTimeout = time.Duration(*body.VisibilityTimeout) * time.Second
	}
	if body.BackoffBaseMS != nil {
		cfg.BackoffBase = time.Duration(*body.BackoffBaseMS) * time.Millisecond
	}
	if body.BackoffCapMS != nil {
		cfg.BackoffCap = time.Duration(*body.BackoffCapMS) * time.Millisecond
	}
	if body.Paused != nil {
		cfg.Paused = *body.Paused
	}
	if body.Description != nil {
		cfg.Description = *body.Description
	}
	cfg.RateLimitPerSec = body.RateLimitPerSec
	cfg.RateLimitBurst = body.RateLimitBurst

	saved, created, err := h.broker.Store().UpsertQueue(r.Context(), cfg)
	if err != nil {
		h.writeError(w, r, err)
		return
	}

	// Drop the cached copy so the change lands on the next dequeue instead of
	// after the config TTL. This is what makes pausing feel immediate.
	h.broker.InvalidateQueue(name)
	if err := h.broker.RegisterQueue(r.Context(), name); err != nil {
		h.log.WarnContext(r.Context(), "queue saved but not registered for sweeps",
			"queue", name, "error", err)
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]any{"queue": queueToMap(saved), "created": created})
}

func (h *RESTHandler) pauseQueue(w http.ResponseWriter, r *http.Request) {
	h.setPaused(w, r, true)
}

func (h *RESTHandler) resumeQueue(w http.ResponseWriter, r *http.Request) {
	h.setPaused(w, r, false)
}

func (h *RESTHandler) setPaused(w http.ResponseWriter, r *http.Request, paused bool) {
	name := chi.URLParam(r, "name")

	if err := h.broker.Store().SetPaused(r.Context(), name, paused); err != nil {
		h.writeError(w, r, err)
		return
	}
	h.broker.InvalidateQueue(name)

	h.log.InfoContext(r.Context(), "queue dispatch toggled",
		"queue", name, "paused", paused)

	writeJSON(w, http.StatusOK, map[string]any{"queue": name, "paused": paused})
}

func (h *RESTHandler) depth(w http.ResponseWriter, r *http.Request) {
	queues := r.URL.Query()["queue"]
	if len(queues) == 0 {
		var err error
		if queues, err = h.broker.ActiveQueues(r.Context()); err != nil {
			h.writeError(w, r, err)
			return
		}
	}

	depths, err := h.broker.Depths(r.Context(), queues)
	if err != nil {
		h.writeError(w, r, err)
		return
	}

	out := make([]map[string]any, 0, len(depths))
	for _, d := range depths {
		out = append(out, map[string]any{
			"queue":     d.Queue,
			"ready":     d.Ready,
			"scheduled": d.Scheduled,
			"running":   d.Running,
			"total":     d.Total(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"depths": out})
}

// --- serialisation --------------------------------------------------------

func jobToMap(j *store.Job) map[string]any {
	out := map[string]any{
		"id":           j.ID,
		"queue":        j.Queue,
		"type":         j.Type,
		"payload":      json.RawMessage(j.Payload),
		"status":       string(j.Status),
		"priority":     j.Priority,
		"attempt":      j.Attempt,
		"max_attempts": j.MaxAttempts,
		"run_at":       j.RunAt,
		"enqueued_at":  j.EnqueuedAt,
	}
	if len(j.Result) > 0 {
		out["result"] = json.RawMessage(j.Result)
	}
	if j.IdempotencyKey != nil {
		out["idempotency_key"] = *j.IdempotencyKey
	}
	if j.LockedBy != nil {
		out["locked_by"] = *j.LockedBy
	}
	if j.LastError != nil {
		out["last_error"] = *j.LastError
	}
	if j.TraceID != nil {
		out["trace_id"] = *j.TraceID
	}
	if j.LeaseExpiresAt != nil {
		out["lease_expires_at"] = *j.LeaseExpiresAt
	}
	if j.StartedAt != nil {
		out["started_at"] = *j.StartedAt
	}
	if j.FinishedAt != nil {
		out["finished_at"] = *j.FinishedAt
	}
	return out
}

func attemptToMap(a *store.Attempt) map[string]any {
	out := map[string]any{
		"id":         a.ID,
		"attempt":    a.Attempt,
		"worker_id":  a.WorkerID,
		"outcome":    string(a.Outcome),
		"started_at": a.StartedAt,
	}
	if a.Error != nil {
		out["error"] = *a.Error
	}
	if a.FinishedAt != nil {
		out["finished_at"] = *a.FinishedAt
	}
	if a.DurationMS != nil {
		out["duration_ms"] = *a.DurationMS
	}
	return out
}

func queueToMap(q *store.QueueConfig) map[string]any {
	return map[string]any{
		"name":                   q.Name,
		"max_concurrency":        q.MaxConcurrency,
		"rate_limit_per_sec":     q.RateLimitPerSec,
		"rate_limit_burst":       q.RateLimitBurst,
		"max_attempts":           q.MaxAttempts,
		"visibility_timeout_sec": int(q.VisibilityTimeout.Seconds()),
		"backoff_base_ms":        int(q.BackoffBase.Milliseconds()),
		"backoff_cap_ms":         int(q.BackoffCap.Milliseconds()),
		"paused":                 q.Paused,
		"description":            q.Description,
	}
}
