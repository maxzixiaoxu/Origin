// Package wire converts between the protobuf types and the domain types.
//
// It exists because both sides of the connection need the same conversions and
// neither can import the other's internals: the queue service's api package is
// internal to services/queue, and the worker's client is internal to
// services/worker. Duplicating the mapping in both would be worse than a shared
// package -- an enum added on one side and forgotten on the other produces jobs
// that silently round-trip as UNSPECIFIED.
//
// Only conversions genuinely needed by both live here. Mappings that touch the
// queue's persistence types stay in services/queue/internal/api, since the
// worker has no business knowing about database rows.
package wire

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	queuev1 "github.com/maxzixiaoxu/Origin/gen/queue/v1"
	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
)

// EnvelopeToProto converts an execution envelope for the wire.
func EnvelopeToProto(e jobtypes.Envelope) *queuev1.Envelope {
	out := &queuev1.Envelope{
		Id:          e.ID,
		Queue:       e.Queue,
		Type:        e.Type,
		Payload:     e.Payload,
		Attempt:     int32(e.Attempt),
		MaxAttempts: int32(e.MaxAttempts),
		Priority:    int32(e.Priority),
		TraceId:     e.TraceID,
	}
	if !e.EnqueuedAt.IsZero() {
		out.EnqueuedAt = timestamppb.New(e.EnqueuedAt)
	}
	if !e.LeaseExpiresAt.IsZero() {
		out.LeaseExpiresAt = timestamppb.New(e.LeaseExpiresAt)
	}
	return out
}

// EnvelopeFromProto rebuilds an execution envelope from the wire.
func EnvelopeFromProto(p *queuev1.Envelope) jobtypes.Envelope {
	if p == nil {
		return jobtypes.Envelope{}
	}

	e := jobtypes.Envelope{
		ID:          p.GetId(),
		Queue:       p.GetQueue(),
		Type:        p.GetType(),
		Payload:     p.GetPayload(),
		Attempt:     int(p.GetAttempt()),
		MaxAttempts: int(p.GetMaxAttempts()),
		Priority:    int(p.GetPriority()),
		TraceID:     p.GetTraceId(),
	}
	if ts := p.GetEnqueuedAt(); ts != nil {
		e.EnqueuedAt = ts.AsTime()
	}
	if ts := p.GetLeaseExpiresAt(); ts != nil {
		e.LeaseExpiresAt = ts.AsTime()
	}
	return e
}

// StatusToProto maps a job status onto the wire enum.
func StatusToProto(s jobtypes.Status) queuev1.JobStatus {
	switch s {
	case jobtypes.StatusPending:
		return queuev1.JobStatus_JOB_STATUS_PENDING
	case jobtypes.StatusScheduled:
		return queuev1.JobStatus_JOB_STATUS_SCHEDULED
	case jobtypes.StatusRunning:
		return queuev1.JobStatus_JOB_STATUS_RUNNING
	case jobtypes.StatusSucceeded:
		return queuev1.JobStatus_JOB_STATUS_SUCCEEDED
	case jobtypes.StatusFailed:
		return queuev1.JobStatus_JOB_STATUS_FAILED
	case jobtypes.StatusDead:
		return queuev1.JobStatus_JOB_STATUS_DEAD
	case jobtypes.StatusCancelled:
		return queuev1.JobStatus_JOB_STATUS_CANCELLED
	default:
		return queuev1.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

// StatusFromProto maps the wire enum back to a job status.
func StatusFromProto(s queuev1.JobStatus) jobtypes.Status {
	switch s {
	case queuev1.JobStatus_JOB_STATUS_PENDING:
		return jobtypes.StatusPending
	case queuev1.JobStatus_JOB_STATUS_SCHEDULED:
		return jobtypes.StatusScheduled
	case queuev1.JobStatus_JOB_STATUS_RUNNING:
		return jobtypes.StatusRunning
	case queuev1.JobStatus_JOB_STATUS_SUCCEEDED:
		return jobtypes.StatusSucceeded
	case queuev1.JobStatus_JOB_STATUS_FAILED:
		return jobtypes.StatusFailed
	case queuev1.JobStatus_JOB_STATUS_DEAD:
		return jobtypes.StatusDead
	case queuev1.JobStatus_JOB_STATUS_CANCELLED:
		return jobtypes.StatusCancelled
	default:
		return ""
	}
}

// OutcomeToProto maps an attempt outcome onto the wire enum.
func OutcomeToProto(o jobtypes.Outcome) queuev1.Outcome {
	switch o {
	case jobtypes.OutcomeSucceeded:
		return queuev1.Outcome_OUTCOME_SUCCEEDED
	case jobtypes.OutcomeFailed:
		return queuev1.Outcome_OUTCOME_FAILED
	case jobtypes.OutcomeTimeout:
		return queuev1.Outcome_OUTCOME_TIMEOUT
	case jobtypes.OutcomeLeaseExpired:
		return queuev1.Outcome_OUTCOME_LEASE_EXPIRED
	case jobtypes.OutcomePanic:
		return queuev1.Outcome_OUTCOME_PANIC
	case jobtypes.OutcomeCancelled:
		return queuev1.Outcome_OUTCOME_CANCELLED
	default:
		return queuev1.Outcome_OUTCOME_UNSPECIFIED
	}
}

// OutcomeFromProto maps the wire enum back to an attempt outcome.
//
// An unset outcome becomes "failed" rather than an empty string: the caller did
// not classify the failure, but recording an empty outcome would leave a row no
// dashboard filter matches, quietly hiding it from the very view meant to
// surface failures.
func OutcomeFromProto(o queuev1.Outcome) jobtypes.Outcome {
	switch o {
	case queuev1.Outcome_OUTCOME_SUCCEEDED:
		return jobtypes.OutcomeSucceeded
	case queuev1.Outcome_OUTCOME_FAILED:
		return jobtypes.OutcomeFailed
	case queuev1.Outcome_OUTCOME_TIMEOUT:
		return jobtypes.OutcomeTimeout
	case queuev1.Outcome_OUTCOME_LEASE_EXPIRED:
		return jobtypes.OutcomeLeaseExpired
	case queuev1.Outcome_OUTCOME_PANIC:
		return jobtypes.OutcomePanic
	case queuev1.Outcome_OUTCOME_CANCELLED:
		return jobtypes.OutcomeCancelled
	default:
		return jobtypes.OutcomeFailed
	}
}
