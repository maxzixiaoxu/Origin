package api

import (
	"google.golang.org/protobuf/types/known/timestamppb"

	queuev1 "github.com/maxzixiaoxu/Origin/gen/queue/v1"
	"github.com/maxzixiaoxu/Origin/pkg/wire"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/broker"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

// Conversion between the protobuf types and this service's persistence types.
//
// The envelope and enum mappings are shared with the worker and live in
// pkg/wire; only conversions that touch store types belong here, since the
// worker has no business knowing about database rows.

var (
	statusToProto    = wire.StatusToProto
	outcomeToProto   = wire.OutcomeToProto
	outcomeFromProto = wire.OutcomeFromProto
	envelopeToProto  = wire.EnvelopeToProto
)

// Remaining conversion between the wire types and this service's types.
//
// Kept in one file, and exhaustive on purpose. Protobuf enums default to zero,
// so a switch that silently falls through to UNSPECIFIED turns a typo into a
// job with no status rather than a compile error. Every mapping below has an
// explicit default that names what it saw.

func throttleToProto(t broker.ThrottleReason) queuev1.ThrottleReason {
	switch t {
	case broker.ThrottleEmpty:
		return queuev1.ThrottleReason_THROTTLE_REASON_EMPTY
	case broker.ThrottleRateLimit:
		return queuev1.ThrottleReason_THROTTLE_REASON_RATE_LIMIT
	case broker.ThrottleConcurrency:
		return queuev1.ThrottleReason_THROTTLE_REASON_CONCURRENCY
	case broker.ThrottlePaused:
		return queuev1.ThrottleReason_THROTTLE_REASON_PAUSED
	default:
		return queuev1.ThrottleReason_THROTTLE_REASON_UNSPECIFIED
	}
}

// ThrottleFromProto maps a wire throttle reason back to the domain type. Used
// by the worker to decide how long to back off.
func ThrottleFromProto(t queuev1.ThrottleReason) broker.ThrottleReason {
	switch t {
	case queuev1.ThrottleReason_THROTTLE_REASON_EMPTY:
		return broker.ThrottleEmpty
	case queuev1.ThrottleReason_THROTTLE_REASON_RATE_LIMIT:
		return broker.ThrottleRateLimit
	case queuev1.ThrottleReason_THROTTLE_REASON_CONCURRENCY:
		return broker.ThrottleConcurrency
	case queuev1.ThrottleReason_THROTTLE_REASON_PAUSED:
		return broker.ThrottlePaused
	default:
		return broker.ThrottleEmpty
	}
}

func jobToProto(j *store.Job) *queuev1.Job {
	if j == nil {
		return nil
	}

	out := &queuev1.Job{
		Id:          j.ID,
		Queue:       j.Queue,
		Type:        j.Type,
		Payload:     j.Payload,
		Status:      statusToProto(j.Status),
		Priority:    int32(j.Priority),
		Attempt:     int32(j.Attempt),
		MaxAttempts: int32(j.MaxAttempts),
		Result:      j.Result,
		RunAt:       timestamppb.New(j.RunAt),
		EnqueuedAt:  timestamppb.New(j.EnqueuedAt),
	}

	if j.IdempotencyKey != nil {
		out.IdempotencyKey = *j.IdempotencyKey
	}
	if j.LockedBy != nil {
		out.LockedBy = *j.LockedBy
	}
	if j.LastError != nil {
		out.LastError = *j.LastError
	}
	if j.TraceID != nil {
		out.TraceId = *j.TraceID
	}
	if j.LeaseExpiresAt != nil {
		out.LeaseExpiresAt = timestamppb.New(*j.LeaseExpiresAt)
	}
	if j.StartedAt != nil {
		out.StartedAt = timestamppb.New(*j.StartedAt)
	}
	if j.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(*j.FinishedAt)
	}
	return out
}

func attemptToProto(a *store.Attempt) *queuev1.Attempt {
	if a == nil {
		return nil
	}

	out := &queuev1.Attempt{
		Id:        a.ID,
		JobId:     a.JobID,
		Attempt:   int32(a.Attempt),
		WorkerId:  a.WorkerID,
		Outcome:   outcomeToProto(a.Outcome),
		StartedAt: timestamppb.New(a.StartedAt),
	}
	if a.Error != nil {
		out.Error = *a.Error
	}
	if a.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(*a.FinishedAt)
	}
	if a.DurationMS != nil {
		out.DurationMs = int32(*a.DurationMS)
	}
	return out
}

func queueConfigToProto(q *store.QueueConfig) *queuev1.QueueConfig {
	if q == nil {
		return nil
	}

	out := &queuev1.QueueConfig{
		Name:                 q.Name,
		MaxConcurrency:       int32(q.MaxConcurrency),
		MaxAttempts:          int32(q.MaxAttempts),
		VisibilityTimeoutSec: int32(q.VisibilityTimeout.Seconds()),
		BackoffBaseMs:        int32(q.BackoffBase.Milliseconds()),
		BackoffCapMs:         int32(q.BackoffCap.Milliseconds()),
		Paused:               q.Paused,
		Description:          q.Description,
	}

	// Optional on the wire so "unlimited" is distinguishable from "zero", which
	// would otherwise mean a queue that dispatches nothing.
	if q.RateLimitPerSec != nil {
		v := int32(*q.RateLimitPerSec)
		out.RateLimitPerSec = &v
	}
	if q.RateLimitBurst != nil {
		v := int32(*q.RateLimitBurst)
		out.RateLimitBurst = &v
	}
	return out
}

func depthToProto(d broker.Depth) *queuev1.Depth {
	return &queuev1.Depth{
		Queue:     d.Queue,
		Ready:     d.Ready,
		Scheduled: d.Scheduled,
		Running:   d.Running,
	}
}
