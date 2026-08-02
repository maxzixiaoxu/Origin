// Command queued is the queue service.
//
// It owns the broker (Redis hot path plus the durable Postgres record) and runs
// the background loops that keep the two in agreement: promotion, reaping,
// reconciliation, and statistics rollup.
//
// It serves two surfaces: gRPC for workers, which call constantly on the hot
// path, and a JSON REST API for the Rails admin, which does not.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	queuev1 "github.com/maxzixiaoxu/Origin/gen/queue/v1"
	"github.com/maxzixiaoxu/Origin/pkg/config"
	"github.com/maxzixiaoxu/Origin/pkg/logx"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/api"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/broker"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/leader"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/maintenance"
	"github.com/maxzixiaoxu/Origin/services/queue/internal/store"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("queued exited with an error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadQueueService()
	if err != nil {
		// Configuration problems are reported before the logger is built, so
		// they go to stderr plainly rather than as a structured record nobody
		// has configured a sink for yet.
		return fmt.Errorf("invalid configuration: %w", err)
	}

	log := logx.New(logx.Options{
		Level:   cfg.Log.Level,
		Service: "queued",
		Text:    cfg.Log.Text,
	})
	logx.SetDefault(log)

	log.Info("starting queue service",
		"instance", cfg.InstanceID,
		"grpc", cfg.GRPCAddr,
		"http", cfg.HTTPAddr,
		"metrics", cfg.MetricsAddr)

	// SIGTERM is what a container orchestrator sends first. Catching it is what
	// makes shutdown graceful rather than a hard kill mid-operation.
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pg, err := store.New(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pg.Close()
	log.Info("connected to postgres")

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.Redis.Addr,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		MinIdleConns: cfg.Redis.MinIdleConns,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	})
	defer func() { _ = rdb.Close() }()

	pingCtx, cancelPing := context.WithTimeout(ctx, cfg.Redis.DialTimeout)
	defer cancelPing()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("ping redis at %s: %w", cfg.Redis.Addr, err)
	}
	log.Info("connected to redis")

	b, err := broker.New(ctx, broker.Options{
		Redis:           rdb,
		Store:           pg,
		Log:             log,
		MaxDequeueBatch: cfg.DequeueMaxBatch,
		QueueConfigTTL:  time.Second,
	})
	if err != nil {
		return err
	}

	// Reconcile before serving anything.
	//
	// A restart is the most likely moment for Redis and Postgres to disagree --
	// the previous process may have died mid-operation, or Redis may have been
	// replaced entirely. Rebuilding first means workers never see a partially
	// populated queue, and it is why a `redis-cli FLUSHALL` costs throughput
	// rather than work.
	if cfg.ReconcileOnBoot {
		start := time.Now()
		res, err := b.Reconcile(ctx, cfg.ReconcileBatch)
		if err != nil {
			return fmt.Errorf("reconcile on boot: %w", err)
		}
		log.Info("boot reconciliation complete",
			"scanned", res.Scanned,
			"restored", res.Restored,
			"stranded", res.Stranded,
			"took", time.Since(start))
	}

	group, runners, err := buildMaintenance(cfg, b, rdb, log)
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := group.Run(gctx); err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("maintenance loops: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		return serveREST(gctx, cfg, b, runners, log)
	})

	g.Go(func() error {
		return serveGRPC(gctx, cfg, b, runners, log)
	})

	log.Info("queue service ready")

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	log.Info("queue service stopped cleanly")
	return nil
}

// maintenanceRunner pairs a loop with its elector so health output can report
// which loops this replica currently leads.
type maintenanceRunner struct {
	name    string
	elector *leader.Elector
}

// buildMaintenance constructs the four background loops.
//
// Each campaigns for its own lock rather than sharing one. There is no reason a
// single replica should own all four, and separate locks let leadership spread
// across the fleet -- so a busy reaper does not also carry the rollup's
// aggregation queries.
func buildMaintenance(
	cfg config.QueueService,
	b *broker.Broker,
	rdb *redis.Client,
	log *slog.Logger,
) (*maintenance.Group, []maintenanceRunner, error) {
	type spec struct {
		name     string
		task     maintenance.Task
		interval time.Duration
	}

	specs := []spec{
		{"promoter", maintenance.NewPromoter(b, cfg.PromoteBatch, log), cfg.PromoteInterval},
		{"reaper", maintenance.NewReaper(b, cfg.ReapBatch, log), cfg.ReapInterval},
		{"rollup", maintenance.NewRollup(b, 10*time.Minute, log), cfg.RollupInterval},
		// Reconciliation is a repair sweep, not a hot path. Running it far less
		// often than the others keeps its full-table scan off the database
		// during normal operation, while still bounding how long a stranded job
		// can sit unnoticed.
		{"reconciler", maintenance.NewReconciler(b, cfg.ReconcileBatch, log), 30 * time.Second},
	}

	var (
		runners  []*maintenance.Runner
		reported []maintenanceRunner
	)

	for _, s := range specs {
		elector, err := leader.New(leader.Options{
			Redis:      rdb,
			Key:        broker.Lock(s.name),
			FenceKey:   broker.FenceCounter(),
			InstanceID: cfg.InstanceID,
			TTL:        cfg.LeaderTTL,
			Log:        log,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("build elector for %s: %w", s.name, err)
		}

		runner, err := maintenance.NewRunner(maintenance.RunnerOptions{
			Task:     s.task,
			Elector:  elector,
			Interval: s.interval,
			Log:      log,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("build runner for %s: %w", s.name, err)
		}

		runners = append(runners, runner)
		reported = append(reported, maintenanceRunner{name: s.name, elector: elector})
	}

	return maintenance.NewGroup(log, runners...), reported, nil
}

// serveREST exposes the admin JSON API that Rails calls, plus health endpoints.
func serveREST(
	ctx context.Context,
	cfg config.QueueService,
	b *broker.Broker,
	runners []maintenanceRunner,
	log *slog.Logger,
) error {
	handler, err := api.NewRESTHandler(api.RESTOptions{
		Broker:   b,
		Log:      log,
		Instance: cfg.InstanceID,
		LeaderFn: leaderSnapshot(runners),
	})
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		// A fresh context: the parent is already cancelled, and Shutdown needs a
		// live one to wait out in-flight requests.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Warn("REST server did not shut down cleanly", "error", err)
		}
	}()

	log.Info("REST API listening", "addr", cfg.HTTPAddr)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("REST server: %w", err)
	}
	return nil
}

// leaderSnapshot reports which loops this replica currently leads.
func leaderSnapshot(runners []maintenanceRunner) func() map[string]bool {
	return func() map[string]bool {
		out := make(map[string]bool, len(runners))
		for _, r := range runners {
			out[r.name] = r.elector.IsLeader()
		}
		return out
	}
}

// serveGRPC exposes the queue to workers.
func serveGRPC(
	ctx context.Context,
	cfg config.QueueService,
	b *broker.Broker,
	runners []maintenanceRunner,
	log *slog.Logger,
) error {
	svc, err := api.NewGRPCServer(api.GRPCOptions{
		Broker:   b,
		Log:      log,
		Instance: cfg.InstanceID,
		LeaderFn: leaderSnapshot(runners),
	})
	if err != nil {
		return err
	}

	srv := grpc.NewServer(
		// Dequeue returns a batch of envelopes, each carrying a payload. The
		// 4MB default is comfortable for the JSON payloads this queue is
		// designed for -- image jobs reference object keys rather than
		// embedding bytes -- but a batch of 64 large payloads could approach
		// it, so the ceiling is raised deliberately rather than discovered
		// through a truncated dequeue in production.
		grpc.MaxRecvMsgSize(16<<20),
		grpc.MaxSendMsgSize(16<<20),

		// Workers keepalive-ping while idle between dequeues. Without matching
		// enforcement the server would terminate those connections as abusive,
		// and every worker would reconnect on a loop.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    20 * time.Second,
			Timeout: 10 * time.Second,
		}),
	)

	queuev1.RegisterQueueServiceServer(srv, svc)

	// Reflection lets grpcurl explore the API with no .proto file on hand,
	// which is what makes the service debuggable from a shell during a demo.
	reflection.Register(srv)

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}

	go func() {
		<-ctx.Done()
		// GracefulStop lets in-flight RPCs finish. A worker mid-dequeue would
		// otherwise get a transport error for a call that already leased jobs
		// -- leaving those leases held by a worker that never received them,
		// idle until the reaper cleans up.
		stopped := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
		case <-time.After(cfg.ShutdownTimeout):
			log.Warn("grpc graceful stop timed out; forcing close",
				"timeout", cfg.ShutdownTimeout)
			srv.Stop()
		}
	}()

	log.Info("grpc listening", "addr", cfg.GRPCAddr)

	if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("grpc server: %w", err)
	}
	return nil
}
