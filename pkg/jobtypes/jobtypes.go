// Package jobtypes holds the domain vocabulary shared by the queue service,
// the workers, and the load generator. It deliberately has no dependency on
// Redis, Postgres, or gRPC so that both sides of the wire can agree on what a
// job *is* without agreeing on how it is stored or transported.
package jobtypes

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Status is the lifecycle state of a job. It mirrors the Postgres `job_status`
// enum exactly; the migration and this list must be changed together.
type Status string

const (
	// StatusPending means the job is in the ready set awaiting a worker.
	StatusPending Status = "pending"
	// StatusScheduled means the job is waiting for run_at to arrive. Both
	// user-requested delays and retry backoff land here.
	StatusScheduled Status = "scheduled"
	// StatusRunning means a worker holds a lease on the job.
	StatusRunning Status = "running"
	// StatusSucceeded is terminal.
	StatusSucceeded Status = "succeeded"
	// StatusFailed means the most recent attempt failed but retries remain.
	// It is a transient state: the job moves back to scheduled immediately.
	StatusFailed Status = "failed"
	// StatusDead is terminal: retries are exhausted or the error was
	// permanent. Dead jobs are what the dead-letter UI browses.
	StatusDead Status = "dead"
	// StatusCancelled is terminal and operator-initiated.
	StatusCancelled Status = "cancelled"
)

// Terminal reports whether a status will never change again without an
// explicit operator action such as a dead-letter replay.
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusDead, StatusCancelled:
		return true
	default:
		return false
	}
}

// Valid reports whether s is a status this system knows how to handle. Used to
// reject junk from the REST boundary before it reaches the database.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusScheduled, StatusRunning,
		StatusSucceeded, StatusFailed, StatusDead, StatusCancelled:
		return true
	default:
		return false
	}
}

// Outcome records how a single execution attempt ended. Unlike Status, which
// describes the job, Outcome describes one row in job_attempts.
type Outcome string

const (
	OutcomeSucceeded Outcome = "succeeded"
	// OutcomeFailed is a handler that returned an error.
	OutcomeFailed Outcome = "failed"
	// OutcomeTimeout is a handler that exceeded its per-job deadline.
	OutcomeTimeout Outcome = "timeout"
	// OutcomeLeaseExpired means no worker ever reported back and the reaper
	// reclaimed the job. Distinguishing this from OutcomeFailed is what makes
	// "how often do workers die mid-job?" an answerable question.
	OutcomeLeaseExpired Outcome = "lease_expired"
	// OutcomePanic is a handler that panicked; recovered at the pool boundary.
	OutcomePanic Outcome = "panic"
	// OutcomeCancelled means the job was cancelled or drained mid-flight.
	OutcomeCancelled Outcome = "cancelled"
)

// Priority ordering. Lower numbers are dispatched first. The range is kept
// small and fixed because the Redis score packs priority into the high digits
// of a float64 (see PriorityScore) and an unbounded range would eventually
// collide with the timestamp component.
const (
	PriorityHighest = 0
	PriorityDefault = 5
	PriorityLowest  = 9
)

// priorityMultiplier separates priority bands in the ready-set score.
//
// A score is priority*1e13 + enqueueMillis. Unix milliseconds are currently
// ~1.8e12 and will not reach 1e13 until the year 2286, so bands never overlap.
// The largest score we can produce is 9*1e13 + 1e13 = 1e14, comfortably inside
// float64's exact-integer range of 2^53 (~9.007e15). Redis sorted-set scores
// are float64, so staying under that bound is what guarantees two jobs
// enqueued a millisecond apart never silently compare equal.
const priorityMultiplier = 1e13

// PriorityScore builds the Redis ready-set score for a job: strict priority
// ordering across bands, FIFO by enqueue time within a band.
func PriorityScore(priority int, enqueuedAt time.Time) float64 {
	return float64(ClampPriority(priority))*priorityMultiplier +
		float64(enqueuedAt.UnixMilli())
}

// ClampPriority coerces an out-of-range priority into the valid band rather
// than rejecting it, so a client bug degrades scheduling instead of dropping
// work.
func ClampPriority(p int) int {
	if p < PriorityHighest {
		return PriorityHighest
	}
	if p > PriorityLowest {
		return PriorityLowest
	}
	return p
}

// Envelope is what a worker receives: everything needed to execute a job and
// nothing else. Bookkeeping columns such as last_error and finished_at stay in
// Postgres and never cross the wire.
type Envelope struct {
	ID          string          `json:"id"`
	Queue       string          `json:"queue"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Attempt     int             `json:"attempt"`
	MaxAttempts int             `json:"max_attempts"`
	Priority    int             `json:"priority"`
	EnqueuedAt  time.Time       `json:"enqueued_at"`
	// LeaseExpiresAt is the deadline by which the worker must ack, nack, or
	// extend. After it passes the reaper may hand this job to someone else.
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	// TraceID ties this execution back to the HTTP request that created it.
	TraceID string `json:"trace_id,omitempty"`
}

// AttemptsRemaining reports how many tries are left after the current one.
func (e *Envelope) AttemptsRemaining() int {
	n := e.MaxAttempts - e.Attempt
	if n < 0 {
		return 0
	}
	return n
}

// IsFinalAttempt reports whether a failure now sends the job to the dead-letter
// queue. Handlers can use this to log more loudly on the last try.
func (e *Envelope) IsFinalAttempt() bool { return e.AttemptsRemaining() <= 0 }

// --- Error classification -------------------------------------------------
//
// The retry ladder is only useful if it is applied to errors that might
// actually succeed later. Retrying a malformed-payload error five times over
// ten minutes wastes capacity and delays the dead-letter signal an operator
// needs. Handlers therefore classify their failures, and the default for an
// unclassified error is to retry — an unknown error is more likely transient
// than permanent, and a wrongly-retried job is cheaper than a wrongly-dropped
// one.

// PermanentError marks a failure that will never succeed on retry. Jobs failing
// with one skip the remaining attempts and go straight to dead-letter.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string {
	if e.Err == nil {
		return "permanent error"
	}
	return "permanent: " + e.Err.Error()
}
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the pool routes it directly to dead-letter.
func Permanent(err error) error { return &PermanentError{Err: err} }

// Permanentf is Permanent with formatting.
func Permanentf(format string, args ...any) error {
	return &PermanentError{Err: fmt.Errorf(format, args...)}
}

// RetryableError explicitly marks a failure as worth retrying. Since retry is
// already the default, this exists mainly for readability at call sites and to
// let a handler override a permanent error it is wrapping.
type RetryableError struct{ Err error }

func (e *RetryableError) Error() string {
	if e.Err == nil {
		return "retryable error"
	}
	return "retryable: " + e.Err.Error()
}
func (e *RetryableError) Unwrap() error { return e.Err }

// Retryable wraps err as explicitly retryable.
func Retryable(err error) error { return &RetryableError{Err: err} }

// IsPermanent reports whether err should skip the retry ladder.
//
// An explicit RetryableError anywhere in the chain wins over a PermanentError,
// so a handler can wrap a third-party permanent error it knows better about.
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}
	var retryable *RetryableError
	if errors.As(err, &retryable) {
		return false
	}
	var permanent *PermanentError
	return errors.As(err, &permanent)
}
