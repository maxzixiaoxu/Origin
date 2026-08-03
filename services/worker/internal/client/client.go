// Package client is the worker's gRPC connection to the queue service.
package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	queuev1 "github.com/maxzixiaoxu/Origin/gen/queue/v1"
	"github.com/maxzixiaoxu/Origin/pkg/jobtypes"
	"github.com/maxzixiaoxu/Origin/pkg/logx"
	"github.com/maxzixiaoxu/Origin/pkg/wire"
	"github.com/maxzixiaoxu/Origin/services/worker/internal/pool"
)

// Client wraps the generated gRPC stub in the interface the pool expects.
type Client struct {
	conn *grpc.ClientConn
	rpc  queuev1.QueueServiceClient
}

// Dial connects to the queue service.
//
// Connection is lazy and self-healing by design. gRPC reconnects with backoff
// on its own, so a worker started before the queue service -- which is the
// normal case under docker compose -- waits and succeeds rather than crash
// looping. Requiring the dependency to be up first would make startup order
// significant, which is exactly the fragility container orchestration exists to
// remove.
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),

		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  200 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				// Capped well below a visibility timeout so a worker that loses
				// its connection can reconnect and resume heartbeating before
				// its leases expire.
				MaxDelay: 5 * time.Second,
			},
			MinConnectTimeout: 5 * time.Second,
		}),

		// Without keepalive, a silently dropped connection -- a NAT timeout, a
		// load balancer reaping an idle stream -- is only discovered on the
		// next call. For the heartbeater that delay can span a visibility
		// timeout, so the worker would lose every lease it holds while
		// believing itself healthy.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                20 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial queue service at %s: %w", addr, err)
	}

	return &Client{conn: conn, rpc: queuev1.NewQueueServiceClient(conn)}, nil
}

// Close releases the connection.
func (c *Client) Close() error { return c.conn.Close() }

// Raw exposes the generated client for callers needing the full surface.
func (c *Client) Raw() queuev1.QueueServiceClient { return c.rpc }

// withTrace propagates the trace id into gRPC metadata so the queue service and
// the worker share one identifier for a job's whole life.
func withTrace(ctx context.Context) context.Context {
	if id := logx.TraceFrom(ctx); id != "" {
		return metadata.AppendToOutgoingContext(ctx, logx.MetadataKey, id)
	}
	return ctx
}

// Dequeue leases jobs.
func (c *Client) Dequeue(
	ctx context.Context,
	queues []string,
	maxJobs int,
	workerID string,
) (*pool.DequeueResult, error) {
	res, err := c.rpc.Dequeue(withTrace(ctx), &queuev1.DequeueRequest{
		Queues:   queues,
		MaxJobs:  int32(maxJobs),
		WorkerId: workerID,
	})
	if err != nil {
		return nil, err
	}

	out := &pool.DequeueResult{
		Jobs:       make([]jobtypes.Envelope, 0, len(res.GetJobs())),
		RetryAfter: time.Duration(res.GetRetryAfterMs()) * time.Millisecond,
	}
	for _, e := range res.GetJobs() {
		out.Jobs = append(out.Jobs, wire.EnvelopeFromProto(e))
	}

	switch res.GetThrottleReason() {
	case queuev1.ThrottleReason_THROTTLE_REASON_RATE_LIMIT,
		queuev1.ThrottleReason_THROTTLE_REASON_CONCURRENCY:
		out.Throttled = true
	case queuev1.ThrottleReason_THROTTLE_REASON_PAUSED:
		out.Paused = true
	}

	return out, nil
}

// Ack marks a job succeeded.
//
// A refused ack -- accepted=false -- is not an error. It means the lease was
// reclaimed while the job ran, which is a normal outcome under at-least-once
// delivery and one the pool already handles by dropping the result.
func (c *Client) Ack(
	ctx context.Context,
	jobID, workerID string,
	result []byte,
	duration time.Duration,
) error {
	_, err := c.rpc.Ack(withTrace(ctx), &queuev1.AckRequest{
		JobId:      jobID,
		WorkerId:   workerID,
		Result:     result,
		DurationMs: duration.Milliseconds(),
	})
	return err
}

// Nack reports failure or hands a job back.
func (c *Client) Nack(ctx context.Context, req pool.NackRequest) error {
	_, err := c.rpc.Nack(withTrace(ctx), &queuev1.NackRequest{
		JobId:              req.JobID,
		WorkerId:           req.WorkerID,
		Error:              req.Error,
		Permanent:          req.Permanent,
		RequeueImmediately: req.RequeueImmediately,
		Outcome:            wire.OutcomeToProto(req.Outcome),
		DurationMs:         req.Duration.Milliseconds(),
	})
	return err
}

// ExtendLeases renews every lease the worker holds, in one call.
func (c *Client) ExtendLeases(
	ctx context.Context,
	workerID string,
	jobIDs []string,
) ([]pool.LeaseResult, error) {
	res, err := c.rpc.ExtendLeases(withTrace(ctx), &queuev1.ExtendLeasesRequest{
		WorkerId: workerID,
		JobIds:   jobIDs,
	})
	if err != nil {
		return nil, err
	}

	out := make([]pool.LeaseResult, 0, len(res.GetResults()))
	for _, r := range res.GetResults() {
		out = append(out, pool.LeaseResult{
			JobID:    r.GetJobId(),
			Extended: r.GetExtended(),
		})
	}
	return out, nil
}

// Verify the client satisfies the pool's expectations at compile time rather
// than at the first dequeue.
var _ pool.Client = (*Client)(nil)
