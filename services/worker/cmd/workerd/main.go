// Command workerd executes jobs.
//
// It connects to the queue service over gRPC, runs a bounded pool of handler
// goroutines, and drains gracefully on SIGTERM so a rolling deploy costs
// milliseconds of latency rather than a full visibility timeout per in-flight
// job.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maxzixiaoxu/Origin/pkg/config"
	"github.com/maxzixiaoxu/Origin/pkg/logx"
	"github.com/maxzixiaoxu/Origin/pkg/objectstore"
	"github.com/maxzixiaoxu/Origin/services/worker/internal/client"
	"github.com/maxzixiaoxu/Origin/services/worker/internal/handlers"
	"github.com/maxzixiaoxu/Origin/services/worker/internal/pool"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("workerd exited with an error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadWorker()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	log := logx.New(logx.Options{
		Level:   cfg.Log.Level,
		Service: "workerd",
		Text:    cfg.Log.Text,
	})
	logx.SetDefault(log)

	log.Info("starting worker",
		"worker_id", cfg.ID,
		"queues", cfg.Queues,
		"concurrency", cfg.Concurrency,
		"queue_addr", cfg.QueueAddr)

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	qc, err := client.Dial(cfg.QueueAddr)
	if err != nil {
		return err
	}
	defer func() { _ = qc.Close() }()

	registry := handlers.NewRegistry()
	handlers.RegisterBench(registry)

	// Object storage is optional so a worker can run the benchmark handlers
	// with no MinIO present. Making it mandatory would mean the load generator
	// could not run without infrastructure it never touches.
	store, err := connectObjectStore(ctx, cfg.ID, log)
	if err != nil {
		return err
	}
	if store != nil {
		registry.Register("image.derive", handlers.NewImageDeriver(store, cfg.ID, log))
	} else {
		log.Warn("object storage is not configured; image.derive is unavailable " +
			"and such jobs will dead-letter as unhandled")
	}

	log.Info("registered handlers", "types", registry.Types())

	p, err := pool.New(pool.Options{
		Client:            qc,
		Registry:          registry,
		Log:               log,
		WorkerID:          cfg.ID,
		Queues:            cfg.Queues,
		Concurrency:       cfg.Concurrency,
		FetchBatch:        cfg.FetchMaxBatch,
		EmptyPollInterval: cfg.EmptyPollInterval,
		JobTimeout:        cfg.JobTimeout,
		HeartbeatInterval: cfg.HeartbeatInterval,
		DrainTimeout:      cfg.DrainTimeout,
	})
	if err != nil {
		return err
	}

	healthDone := make(chan struct{})
	go func() {
		defer close(healthDone)
		serveHealth(ctx, cfg, p, store, log)
	}()

	// Run blocks until ctx is cancelled and the drain completes.
	if err := p.Run(ctx); err != nil {
		return err
	}

	<-healthDone
	log.Info("worker stopped cleanly")
	return nil
}

// connectObjectStore dials MinIO or S3, returning nil when unconfigured.
func connectObjectStore(
	ctx context.Context,
	workerID string,
	log *slog.Logger,
) (*objectstore.Store, error) {
	endpoint := config.Str("S3_ENDPOINT", "")
	bucket := config.Str("S3_BUCKET", "")

	if endpoint == "" && bucket == "" {
		return nil, nil
	}
	if bucket == "" {
		return nil, errors.New("S3_ENDPOINT is set but S3_BUCKET is not")
	}

	dialCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	store, err := objectstore.New(dialCtx, objectstore.Config{
		Endpoint:  endpoint,
		Region:    config.Str("S3_REGION", "us-east-1"),
		Bucket:    bucket,
		AccessKey: config.Str("S3_ACCESS_KEY", ""),
		SecretKey: config.Str("S3_SECRET_KEY", ""),
		// MinIO has no wildcard DNS, so bucket-in-hostname addressing cannot
		// resolve. Harmless against real S3.
		PathStyle:      config.Bool("S3_PATH_STYLE", true),
		PublicEndpoint: config.Str("S3_PUBLIC_ENDPOINT", ""),
	})
	if err != nil {
		return nil, fmt.Errorf("connect to object storage: %w", err)
	}

	log.Info("connected to object storage",
		"endpoint", endpoint, "bucket", bucket, "worker_id", workerID)
	return store, nil
}

// serveHealth exposes liveness and pool statistics.
//
// Liveness deliberately does not check the queue service or object storage.
// Returning unhealthy while a dependency is briefly down would make an
// orchestrator kill workers that are about to reconnect on their own, and every
// job they were running would then have to be reaped -- turning a dependency
// blip into real duplicated work.
func serveHealth(
	ctx context.Context,
	cfg config.Worker,
	p *pool.Pool,
	store *objectstore.Store,
	log *slog.Logger,
) {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		s := p.Stats()

		storageOK := "not configured"
		if store != nil {
			checkCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := store.Ping(checkCtx); err != nil {
				storageOK = err.Error()
			} else {
				storageOK = "ok"
			}
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w,
			`{"worker_id":%q,"concurrency":%d,"in_flight":%d,`+
				`"completed":%d,"failed":%d,"lease_lost":%d,"panics":%d,`+
				`"storage":%q}`+"\n",
			cfg.ID, cfg.Concurrency, s.InFlight,
			s.Completed, s.Failed, s.LeaseLost, s.Panics, storageOK)
	})

	srv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(), cfg.DrainTimeout+5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("worker health endpoints listening", "addr", cfg.MetricsAddr)

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("worker health server failed", "error", err)
	}
}
