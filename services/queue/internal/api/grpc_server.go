// Package api exposes the broker over gRPC (for workers) and REST (for Rails).
package api

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	queuev1 "github.com/maxzixiaoxu/Origin/gen/queue/v1"
	"github.com/maxzixiaoxu/Origin/pkg/logx"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/broker"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

// GRPCServer implements queue.v1.QueueService.
//
// It is a thin translation layer on purpose: no queue logic lives here, only
// wire conversion and error mapping. Anything that decides how the queue
// behaves belongs in the broker, where it can be tested without a network.
type GRPCServer struct {
	queuev1.UnimplementedQueueServiceServer

	broker   *broker.Broker
	log      *slog.Logger
	instance string
	// leaderFn reports which background loops this replica leads, for Health.
	leaderFn func() map[string]bool
}

// GRPCOptions configures the server.
type GRPCOptions struct {
	Broker   *broker.Broker
	Log      *slog.Logger
	Instance string
	LeaderFn func() map[string]bool
}

// NewGRPCServer builds the gRPC service.
func NewGRPCServer(opts GRPCOptions) (*GRPCServer, error) {
	if opts.Broker == nil {
		return nil, errors.New("api: Broker is required")
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &GRPCServer{
		broker:   opts.Broker,
		log:      log,
		instance: opts.Instance,
		leaderFn: opts.LeaderFn,
	}, nil
}

// traceContext lifts the trace id out of gRPC metadata into the context, so
// every log line emitted while handling this call carries it automatically and
// correlates with the Rails request that started it.
func traceContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	if vals := md.Get(logx.MetadataKey); len(vals) > 0 && vals[0] != "" {
		return logx.WithTrace(ctx, vals[0])
	}
	return ctx
}

// toStatusError maps domain errors onto gRPC codes.
//
// The mapping matters more than it looks: a worker treats codes differently.
// NotFound and FailedPrecondition are terminal for that call and must not be
// retried, while Unavailable is worth another attempt. Returning Internal for
// everything would make clients retry unretryable operations forever.
func toStatusError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, broker.ErrLeaseLost):
		// Not an error condition in the system's sense -- it is the expected
		// answer when a worker was too slow. FailedPrecondition tells the
		// caller to stop, as opposed to Aborted which invites a retry.
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// --- enqueue --------------------------------------------------------------

// Enqueue submits one job.
func (s *GRPCServer) Enqueue(
	ctx context.Context,
	req *queuev1.EnqueueRequest,
) (*queuev1.EnqueueResponse, error) {
	ctx = traceContext(ctx)

	res, err := s.broker.Enqueue(ctx, enqueueFromProto(ctx, req))
	if err != nil {
		return nil, toStatusError(err)
	}

	return &queuev1.EnqueueResponse{
		Id:           res.JobID,
		Status:       statusToProto(res.Status),
		Deduplicated: res.Deduplicated,
		QueueDepth:   res.Depth,
	}, nil
}

func enqueueFromProto(ctx context.Context, req *queuev1.EnqueueRequest) broker.EnqueueRequest {
	out := broker.EnqueueRequest{
		Queue:   req.GetQueue(),
		Type:    req.GetType(),
		Payload: req.GetPayload(),
		TraceID: req.GetTraceId(),
	}

	// An explicit trace id on the request wins; otherwise inherit the one from
	// call metadata so a job created inside a traced request stays correlated
	// even when the caller did not think to set the field.
	if out.TraceID == "" {
		out.TraceID = logx.TraceFrom(ctx)
	}
	if req.Priority != nil {
		p := int(req.GetPriority())
		out.Priority = &p
	}
	if req.MaxAttempts != nil {
		m := int(req.GetMaxAttempts())
		out.MaxAttempts = &m
	}
	if req.IdempotencyKey != nil {
		out.IdempotencyKey = req.GetIdempotencyKey()
	}
	if ts := req.GetRunAt(); ts != nil {
		out.RunAt = ts.AsTime()
	}
	return out
}

// EnqueueBatch submits many jobs, reporting per-item success.
//
// One malformed job must not reject the other 999. A loader importing a batch
// needs to know exactly which entries failed, not that "the batch" failed.
func (s *GRPCServer) EnqueueBatch(
	ctx context.Context,
	req *queuev1.EnqueueBatchRequest,
) (*queuev1.EnqueueBatchResponse, error) {
	ctx = traceContext(ctx)

	out := &queuev1.EnqueueBatchResponse{
		Results: make([]*queuev1.EnqueueBatchResult, 0, len(req.GetJobs())),
	}

	for i, item := range req.GetJobs() {
		res, err := s.broker.Enqueue(ctx, enqueueFromProto(ctx, item))
		if err != nil {
			out.Failed++
			out.Results = append(out.Results, &queuev1.EnqueueBatchResult{
				Index: int32(i),
				Error: err.Error(),
			})
			continue
		}

		out.Succeeded++
		out.Results = append(out.Results, &queuev1.EnqueueBatchResult{
			Index: int32(i),
			Response: &queuev1.EnqueueResponse{
				Id:           res.JobID,
				Status:       statusToProto(res.Status),
				Deduplicated: res.Deduplicated,
				QueueDepth:   res.Depth,
			},
		})
	}

	return out, nil
}

// --- worker hot path ------------------------------------------------------

// Dequeue leases jobs for a worker.
func (s *GRPCServer) Dequeue(
	ctx context.Context,
	req *queuev1.DequeueRequest,
) (*queuev1.DequeueResponse, error) {
	ctx = traceContext(ctx)

	res, err := s.broker.Dequeue(ctx, broker.DequeueRequest{
		Queues:   req.GetQueues(),
		MaxJobs:  int(req.GetMaxJobs()),
		WorkerID: req.GetWorkerId(),
	})
	if err != nil {
		return nil, toStatusError(err)
	}

	out := &queuev1.DequeueResponse{
		Jobs:           make([]*queuev1.Envelope, 0, len(res.Jobs)),
		ThrottleReason: throttleToProto(res.ThrottleReason),
		RetryAfterMs:   res.RetryAfter.Milliseconds(),
	}
	for _, e := range res.Jobs {
		out.Jobs = append(out.Jobs, envelopeToProto(e))
	}
	return out, nil
}

// Ack marks a job succeeded.
//
// A lost lease is reported as accepted=false rather than as an error, because
// it is a normal outcome the worker must handle rather than an operational
// failure. Only a genuine fault produces a non-OK status.
func (s *GRPCServer) Ack(
	ctx context.Context,
	req *queuev1.AckRequest,
) (*queuev1.AckResponse, error) {
	ctx = traceContext(ctx)

	err := s.broker.Ack(ctx, broker.AckRequest{
		JobID:    req.GetJobId(),
		WorkerID: req.GetWorkerId(),
		Result:   req.GetResult(),
		Duration: time.Duration(req.GetDurationMs()) * time.Millisecond,
	})
	if errors.Is(err, broker.ErrLeaseLost) {
		return &queuev1.AckResponse{
			Accepted: false,
			Reason:   "lease was reclaimed before the ack arrived",
		}, nil
	}
	if err != nil {
		return nil, toStatusError(err)
	}
	return &queuev1.AckResponse{Accepted: true}, nil
}

// Nack reports failure and schedules a retry or dead-letters the job.
func (s *GRPCServer) Nack(
	ctx context.Context,
	req *queuev1.NackRequest,
) (*queuev1.NackResponse, error) {
	ctx = traceContext(ctx)

	res, err := s.broker.Nack(ctx, broker.NackRequest{
		JobID:              req.GetJobId(),
		WorkerID:           req.GetWorkerId(),
		Error:              req.GetError(),
		Permanent:          req.GetPermanent(),
		RequeueImmediately: req.GetRequeueImmediately(),
		Outcome:            outcomeFromProto(req.GetOutcome()),
		Duration:           time.Duration(req.GetDurationMs()) * time.Millisecond,
	})
	if errors.Is(err, broker.ErrLeaseLost) {
		return &queuev1.NackResponse{
			Accepted: false,
			Reason:   "lease was reclaimed before the nack arrived",
		}, nil
	}
	if err != nil {
		return nil, toStatusError(err)
	}

	out := &queuev1.NackResponse{
		Accepted: true,
		Status:   statusToProto(res.Status),
	}
	if !res.RetryAt.IsZero() {
		out.RetryAt = timestamppb.New(res.RetryAt)
	}
	return out, nil
}

// ExtendLeases renews every lease a worker holds, in one call.
func (s *GRPCServer) ExtendLeases(
	ctx context.Context,
	req *queuev1.ExtendLeasesRequest,
) (*queuev1.ExtendLeasesResponse, error) {
	ctx = traceContext(ctx)

	results, err := s.broker.ExtendLeases(ctx, req.GetWorkerId(), req.GetJobIds())
	if err != nil {
		return nil, toStatusError(err)
	}

	out := &queuev1.ExtendLeasesResponse{
		Results: make([]*queuev1.LeaseResult, 0, len(results)),
	}
	for _, r := range results {
		lr := &queuev1.LeaseResult{JobId: r.JobID, Extended: r.Extended}
		if r.Extended {
			lr.ExpiresAt = timestamppb.New(r.ExpiresAt)
		}
		out.Results = append(out.Results, lr)
	}
	return out, nil
}

// --- admin ----------------------------------------------------------------

// GetJob loads a job and optionally its attempt history.
func (s *GRPCServer) GetJob(
	ctx context.Context,
	req *queuev1.GetJobRequest,
) (*queuev1.GetJobResponse, error) {
	ctx = traceContext(ctx)

	job, err := s.broker.Store().GetJob(ctx, req.GetJobId())
	if err != nil {
		return nil, toStatusError(err)
	}

	out := &queuev1.GetJobResponse{Job: jobToProto(job)}

	if req.GetIncludeAttempts() {
		attempts, err := s.broker.Store().ListAttempts(ctx, req.GetJobId())
		if err != nil {
			return nil, toStatusError(err)
		}
		out.Attempts = make([]*queuev1.Attempt, 0, len(attempts))
		for _, a := range attempts {
			out.Attempts = append(out.Attempts, attemptToProto(a))
		}
	}
	return out, nil
}

// CancelJob removes a job from circulation.
func (s *GRPCServer) CancelJob(
	ctx context.Context,
	req *queuev1.CancelJobRequest,
) (*queuev1.CancelJobResponse, error) {
	ctx = traceContext(ctx)

	cancelled, err := s.broker.Cancel(ctx, req.GetJobId())
	if err != nil {
		return nil, toStatusError(err)
	}
	if !cancelled {
		return &queuev1.CancelJobResponse{
			Cancelled: false,
			Reason:    "job had already reached a terminal state",
		}, nil
	}
	return &queuev1.CancelJobResponse{Cancelled: true}, nil
}

// RetryJob returns a dead or failed job to the ready set.
func (s *GRPCServer) RetryJob(
	ctx context.Context,
	req *queuev1.RetryJobRequest,
) (*queuev1.RetryJobResponse, error) {
	ctx = traceContext(ctx)

	err := s.broker.Retry(ctx, req.GetJobId(), req.GetResetAttempts())
	if errors.Is(err, store.ErrNotFound) {
		return &queuev1.RetryJobResponse{
			Retried: false,
			Reason:  "job is not in a replayable state",
		}, nil
	}
	if err != nil {
		return nil, toStatusError(err)
	}
	return &queuev1.RetryJobResponse{Retried: true}, nil
}

// ListQueues returns every queue's configuration.
func (s *GRPCServer) ListQueues(
	ctx context.Context,
	_ *queuev1.ListQueuesRequest,
) (*queuev1.ListQueuesResponse, error) {
	ctx = traceContext(ctx)

	queues, err := s.broker.Store().ListQueues(ctx)
	if err != nil {
		return nil, toStatusError(err)
	}

	out := &queuev1.ListQueuesResponse{
		Queues: make([]*queuev1.QueueConfig, 0, len(queues)),
	}
	for _, q := range queues {
		out.Queues = append(out.Queues, queueConfigToProto(q))
	}
	return out, nil
}

// UpsertQueue creates or reconfigures a queue.
func (s *GRPCServer) UpsertQueue(
	ctx context.Context,
	req *queuev1.UpsertQueueRequest,
) (*queuev1.UpsertQueueResponse, error) {
	ctx = traceContext(ctx)

	cfg := req.GetConfig()
	if cfg == nil {
		return nil, status.Error(codes.InvalidArgument, "config is required")
	}
	if err := broker.ValidateQueueName(cfg.GetName()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	in := &store.QueueConfig{
		Name:              cfg.GetName(),
		MaxConcurrency:    int(cfg.GetMaxConcurrency()),
		MaxAttempts:       int(cfg.GetMaxAttempts()),
		VisibilityTimeout: time.Duration(cfg.GetVisibilityTimeoutSec()) * time.Second,
		BackoffBase:       time.Duration(cfg.GetBackoffBaseMs()) * time.Millisecond,
		BackoffCap:        time.Duration(cfg.GetBackoffCapMs()) * time.Millisecond,
		Paused:            cfg.GetPaused(),
		Description:       cfg.GetDescription(),
	}
	if cfg.RateLimitPerSec != nil {
		v := int(cfg.GetRateLimitPerSec())
		in.RateLimitPerSec = &v
	}
	if cfg.RateLimitBurst != nil {
		v := int(cfg.GetRateLimitBurst())
		in.RateLimitBurst = &v
	}

	saved, created, err := s.broker.Store().UpsertQueue(ctx, in)
	if err != nil {
		return nil, toStatusError(err)
	}

	// Drop the cached copy so the change takes effect on the next dequeue
	// rather than after the cache TTL. This is what makes "pause this queue"
	// during an incident feel immediate.
	s.broker.InvalidateQueue(saved.Name)

	if err := s.broker.RegisterQueue(ctx, saved.Name); err != nil {
		s.log.WarnContext(ctx, "queue saved but not registered for sweeps",
			"queue", saved.Name, "error", err)
	}

	return &queuev1.UpsertQueueResponse{
		Config:  queueConfigToProto(saved),
		Created: created,
	}, nil
}

// QueueDepth reports live counts straight from Redis.
func (s *GRPCServer) QueueDepth(
	ctx context.Context,
	req *queuev1.QueueDepthRequest,
) (*queuev1.QueueDepthResponse, error) {
	ctx = traceContext(ctx)

	queues := req.GetQueues()
	if len(queues) == 0 {
		var err error
		if queues, err = s.broker.ActiveQueues(ctx); err != nil {
			return nil, toStatusError(err)
		}
	}

	depths, err := s.broker.Depths(ctx, queues)
	if err != nil {
		return nil, toStatusError(err)
	}

	out := &queuev1.QueueDepthResponse{
		Depths: make([]*queuev1.Depth, 0, len(depths)),
	}
	for _, d := range depths {
		out.Depths = append(out.Depths, depthToProto(d))
	}
	return out, nil
}

// Health reports dependency reachability and leadership.
func (s *GRPCServer) Health(
	ctx context.Context,
	_ *queuev1.HealthRequest,
) (*queuev1.HealthResponse, error) {
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	redisOK := s.broker.Redis().Ping(checkCtx).Err() == nil
	pgOK := s.broker.Store().Ping(checkCtx) == nil

	isLeader := false
	if s.leaderFn != nil {
		for _, leading := range s.leaderFn() {
			if leading {
				isLeader = true
				break
			}
		}
	}

	return &queuev1.HealthResponse{
		Healthy:    redisOK && pgOK,
		RedisOk:    redisOK,
		PostgresOk: pgOK,
		IsLeader:   isLeader,
		InstanceId: s.instance,
	}, nil
}
