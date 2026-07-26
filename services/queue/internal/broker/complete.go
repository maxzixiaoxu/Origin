package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

// AckRequest reports a successful execution.
type AckRequest struct {
	JobID    string
	WorkerID string
	Result   []byte
	Duration time.Duration
}

// Ack marks a job succeeded.
//
// Redis is consulted first and its answer is final. Only the lease set knows
// who currently owns a job, and if this worker's lease was reaped while it was
// running -- a long GC pause, a network partition, a slow downstream call --
// then another worker has the job now. Its result must be discarded rather than
// written, or two executions would both claim to be the one that counted.
//
// Discarding a completed result feels wasteful, and it is. That waste is the
// price of at-least-once delivery, and the alternative -- letting a stale
// worker overwrite the state of a job someone else owns -- is a correctness
// bug rather than an efficiency one.
func (b *Broker) Ack(ctx context.Context, req AckRequest) error {
	job, err := b.store.GetJob(ctx, req.JobID)
	if err != nil {
		return err
	}

	code, err := b.runComplete(ctx, job.Queue, req.JobID, req.WorkerID,
		actionAck, 0, "")
	if err != nil {
		return err
	}
	if code != resultOK {
		b.recordAttempt(ctx, job, req.WorkerID, jobtypes.OutcomeSucceeded,
			"lease lost before ack", req.Duration)
		return fmt.Errorf("%w: job %s (code %d)", ErrLeaseLost, req.JobID, code)
	}

	if err := b.store.MarkSucceeded(ctx, req.JobID, req.Result); err != nil &&
		!errors.Is(err, store.ErrNotFound) {
		return err
	}

	b.recordAttempt(ctx, job, req.WorkerID, jobtypes.OutcomeSucceeded, "", req.Duration)
	return nil
}

// NackRequest reports a failed or abandoned execution.
type NackRequest struct {
	JobID    string
	WorkerID string
	Error    string
	// Permanent skips the retry ladder entirely. Set for errors the handler
	// knows cannot succeed later, such as a malformed payload -- retrying those
	// five times over ten minutes wastes capacity and delays the dead-letter
	// signal an operator needs to see.
	Permanent bool
	// RequeueImmediately skips backoff. Used during graceful drain, where the
	// job did not fail: the worker is shutting down and wants it picked up in
	// milliseconds rather than after a full visibility timeout.
	RequeueImmediately bool
	Outcome            jobtypes.Outcome
	Duration           time.Duration
}

// NackResult reports where the job went.
type NackResult struct {
	// Status is SCHEDULED when a retry was queued, PENDING for an immediate
	// requeue, DEAD when the job was dead-lettered.
	Status  jobtypes.Status
	RetryAt time.Time
}

// Nack reports failure and routes the job to a retry or to dead-letter.
func (b *Broker) Nack(ctx context.Context, req NackRequest) (*NackResult, error) {
	job, err := b.store.GetJob(ctx, req.JobID)
	if err != nil {
		return nil, err
	}

	cfg, err := b.queues.get(ctx, job.Queue)
	if err != nil {
		return nil, err
	}

	outcome := req.Outcome
	if outcome == "" {
		outcome = jobtypes.OutcomeFailed
	}

	now := b.now()

	// Graceful drain: not a failure, so the attempt counter is untouched and
	// the job goes straight back to ready. Consuming a retry for a deploy would
	// slowly dead-letter healthy jobs across enough rolling restarts.
	if req.RequeueImmediately {
		envelope := b.envelopeFor(job, job.Attempt)
		score := jobtypes.PriorityScore(job.Priority, job.EnqueuedAt)

		code, err := b.runCompleteWithEnvelope(ctx, job.Queue, req.JobID, req.WorkerID,
			actionRequeueNow, score, envelope)
		if err != nil {
			return nil, err
		}
		if code != resultOK {
			return nil, fmt.Errorf("%w: job %s", ErrLeaseLost, req.JobID)
		}

		if err := b.store.MarkRetrying(ctx, req.JobID, now, req.Error); err != nil &&
			!errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		b.recordAttempt(ctx, job, req.WorkerID, jobtypes.OutcomeCancelled,
			req.Error, req.Duration)

		return &NackResult{Status: jobtypes.StatusPending, RetryAt: now}, nil
	}

	// job.Attempt is the attempt that just ran. Retries remain only if it was
	// not already the last one.
	exhausted := job.Attempt >= job.MaxAttempts
	dead := req.Permanent || exhausted

	if dead {
		code, err := b.runComplete(ctx, job.Queue, req.JobID, req.WorkerID,
			actionDead, 0, "")
		if err != nil {
			return nil, err
		}
		if code != resultOK {
			return nil, fmt.Errorf("%w: job %s", ErrLeaseLost, req.JobID)
		}

		reason := req.Error
		if req.Permanent {
			reason = "permanent failure: " + reason
		} else {
			reason = fmt.Sprintf("exhausted %d attempts: %s", job.MaxAttempts, reason)
		}

		if err := b.store.MarkDead(ctx, req.JobID, reason); err != nil &&
			!errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		b.recordAttempt(ctx, job, req.WorkerID, outcome, req.Error, req.Duration)

		return &NackResult{Status: jobtypes.StatusDead}, nil
	}

	// Retry. The delay is drawn from the queue's own backoff policy, jittered.
	policy := backoffFor(cfg)
	retryAt := policy.NextRunAt(now, job.Attempt)

	// The envelope advertises the NEXT attempt number, and it is rewritten
	// inside the same script that requeues the job -- so no worker can ever
	// observe a job that is back in the scheduled set while still claiming its
	// previous attempt count.
	envelope := b.envelopeFor(job, job.Attempt+1)

	code, err := b.runCompleteWithEnvelope(ctx, job.Queue, req.JobID, req.WorkerID,
		actionRetry, float64(retryAt.UnixMilli()), envelope)
	if err != nil {
		return nil, err
	}
	if code != resultOK {
		return nil, fmt.Errorf("%w: job %s", ErrLeaseLost, req.JobID)
	}

	if err := b.store.MarkRetrying(ctx, req.JobID, retryAt, req.Error); err != nil &&
		!errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	b.recordAttempt(ctx, job, req.WorkerID, outcome, req.Error, req.Duration)

	return &NackResult{Status: jobtypes.StatusScheduled, RetryAt: retryAt}, nil
}

// envelopeFor projects a durable row into an execution envelope carrying the
// given attempt number.
func (b *Broker) envelopeFor(job *store.Job, attempt int) jobtypes.Envelope {
	e := job.Envelope()
	e.Attempt = attempt
	return e
}

// runComplete invokes complete.lua without rewriting the envelope.
func (b *Broker) runComplete(
	ctx context.Context,
	queue, jobID, workerID string,
	action int64,
	score float64,
	_ string,
) (int64, error) {
	return b.completeScript(ctx, queue, jobID, workerID, action, score, "")
}

// runCompleteWithEnvelope invokes complete.lua and refreshes the cached
// envelope in the same atomic step.
func (b *Broker) runCompleteWithEnvelope(
	ctx context.Context,
	queue, jobID, workerID string,
	action int64,
	score float64,
	envelope jobtypes.Envelope,
) (int64, error) {
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return 0, fmt.Errorf("encode envelope: %w", err)
	}
	return b.completeScript(ctx, queue, jobID, workerID, action, score, string(encoded))
}

func (b *Broker) completeScript(
	ctx context.Context,
	queue, jobID, workerID string,
	action int64,
	score float64,
	envelope string,
) (int64, error) {
	keys := KeysFor(queue)

	res, err := b.scripts.complete.Run(ctx, b.rdb,
		[]string{keys.Leases(), keys.Inflight(), keys.Ready(),
			keys.Scheduled(), keys.Job(jobID)},
		jobID, workerID, action, score, envelope, envelopeTTL.Milliseconds(),
	).Int64()
	if err != nil {
		return 0, fmt.Errorf("complete job %s (action %d): %w", jobID, action, err)
	}
	return res, nil
}

// recordAttempt appends to the audit trail.
//
// Failures here are logged and swallowed. The audit trail is valuable but it is
// not the job: refusing to acknowledge completed work because a history row
// could not be written would turn a reporting problem into a duplicate
// execution, since the job would be reaped and run again.
func (b *Broker) recordAttempt(
	ctx context.Context,
	job *store.Job,
	workerID string,
	outcome jobtypes.Outcome,
	errMsg string,
	duration time.Duration,
) {
	attempt := job.Attempt
	if attempt < 1 {
		attempt = 1
	}

	if err := b.store.RecordAttempt(ctx, &store.NewAttempt{
		JobID:    job.ID,
		Attempt:  attempt,
		WorkerID: workerID,
		Outcome:  outcome,
		Error:    errMsg,
		Duration: duration,
	}); err != nil {
		b.log.WarnContext(ctx, "could not record attempt history",
			"job_id", job.ID, "attempt", attempt, "error", err)
	}
}
