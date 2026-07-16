// Package config loads service configuration from the environment.
//
// Everything is read from env vars with a documented default, and every
// service validates its config at startup and refuses to run if it is
// nonsensical. Failing loudly at boot beats a worker that silently comes up
// with a concurrency of zero and processes nothing while looking healthy.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// --- env helpers ----------------------------------------------------------

// Str returns the env var or a default.
func Str(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// Int returns the env var parsed as an int, or def if unset or unparseable.
func Int(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// Bool accepts the usual truthy spellings; anything else yields def.
func Bool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

// Dur parses a Go duration string such as "30s" or "5m".
func Dur(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// List parses a comma-separated env var, trimming blanks. Empty entries are
// dropped so "a,,b" and "a, b" both yield [a b].
func List(key string, def []string) []string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

// --- shared sections ------------------------------------------------------

// Postgres holds connection settings for the durable store.
type Postgres struct {
	URL string
	// MaxConns bounds the pool. Sized against Postgres' own max_connections:
	// several service replicas each opening a large pool is a common way to
	// exhaust the server's connection slots and take down everything at once.
	MaxConns          int32
	MinConns          int32
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	ConnectTimeout    time.Duration
	StatementTimeout  time.Duration
	HealthCheckPeriod time.Duration
}

func loadPostgres() Postgres {
	return Postgres{
		URL: Str("POSTGRES_URL",
			"postgres://jobq:jobq@localhost:5432/jobq?sslmode=disable"),
		MaxConns:          int32(Int("POSTGRES_MAX_CONNS", 20)),
		MinConns:          int32(Int("POSTGRES_MIN_CONNS", 2)),
		MaxConnLifetime:   Dur("POSTGRES_MAX_CONN_LIFETIME", time.Hour),
		MaxConnIdleTime:   Dur("POSTGRES_MAX_CONN_IDLE", 30*time.Minute),
		ConnectTimeout:    Dur("POSTGRES_CONNECT_TIMEOUT", 10*time.Second),
		StatementTimeout:  Dur("POSTGRES_STATEMENT_TIMEOUT", 15*time.Second),
		HealthCheckPeriod: Dur("POSTGRES_HEALTHCHECK_PERIOD", time.Minute),
	}
}

func (p Postgres) validate() error {
	if p.URL == "" {
		return errors.New("POSTGRES_URL is required")
	}
	if p.MaxConns < 1 {
		return fmt.Errorf("POSTGRES_MAX_CONNS must be >= 1, got %d", p.MaxConns)
	}
	if p.MinConns > p.MaxConns {
		return fmt.Errorf("POSTGRES_MIN_CONNS (%d) exceeds POSTGRES_MAX_CONNS (%d)",
			p.MinConns, p.MaxConns)
	}
	return nil
}

// Redis holds connection settings for the hot path.
type Redis struct {
	Addr     string
	Password string
	DB       int
	PoolSize int
	// MinIdleConns keeps warm connections so a burst of dequeues does not pay
	// TCP setup on the critical path.
	MinIdleConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func loadRedis() Redis {
	return Redis{
		Addr:         Str("REDIS_ADDR", "localhost:6379"),
		Password:     Str("REDIS_PASSWORD", ""),
		DB:           Int("REDIS_DB", 0),
		PoolSize:     Int("REDIS_POOL_SIZE", 50),
		MinIdleConns: Int("REDIS_MIN_IDLE_CONNS", 5),
		DialTimeout:  Dur("REDIS_DIAL_TIMEOUT", 5*time.Second),
		ReadTimeout:  Dur("REDIS_READ_TIMEOUT", 3*time.Second),
		WriteTimeout: Dur("REDIS_WRITE_TIMEOUT", 3*time.Second),
	}
}

func (r Redis) validate() error {
	if r.Addr == "" {
		return errors.New("REDIS_ADDR is required")
	}
	if r.PoolSize < 1 {
		return fmt.Errorf("REDIS_POOL_SIZE must be >= 1, got %d", r.PoolSize)
	}
	return nil
}

// Log holds logging settings.
type Log struct {
	Level string
	Text  bool
}

func loadLog() Log {
	return Log{
		Level: Str("LOG_LEVEL", "info"),
		Text:  Bool("LOG_TEXT", false),
	}
}

// --- queue service --------------------------------------------------------

// QueueService is the configuration for the queued binary.
type QueueService struct {
	Postgres Postgres
	Redis    Redis
	Log      Log

	// GRPCAddr serves workers; HTTPAddr serves Rails; MetricsAddr serves
	// Prometheus. They are separate listeners so the metrics and worker planes
	// can be firewalled independently of the admin plane.
	GRPCAddr    string
	HTTPAddr    string
	MetricsAddr string

	// InstanceID identifies this replica in leader-election locks and logs.
	InstanceID string

	// ShutdownTimeout bounds how long in-flight RPCs may finish on SIGTERM.
	ShutdownTimeout time.Duration

	// PromoteInterval controls how often due scheduled jobs move to ready.
	// This is the floor on retry-delay accuracy: a job scheduled for T becomes
	// runnable somewhere in [T, T+PromoteInterval].
	PromoteInterval time.Duration
	PromoteBatch    int

	// ReapInterval controls how often expired leases are reclaimed. It bounds
	// how long a crashed worker's job sits idle before another worker can take
	// it, so it is the dominant term in crash-recovery time.
	ReapInterval time.Duration
	ReapBatch    int

	// RollupInterval controls how often per-minute dashboard stats are
	// aggregated.
	RollupInterval time.Duration

	// LeaderTTL is how long a leader lock survives without renewal. Renewal
	// happens at TTL/3, so two missed renewals lose leadership.
	LeaderTTL time.Duration

	// ReconcileOnBoot rebuilds Redis state from Postgres at startup.
	ReconcileOnBoot bool
	ReconcileBatch  int

	// DequeueMaxBatch caps how many jobs one Dequeue call may return, bounding
	// how much work a single worker can lock up at once.
	DequeueMaxBatch int
}

// LoadQueueService reads queue-service config from the environment.
func LoadQueueService() (QueueService, error) {
	c := QueueService{
		Postgres:        loadPostgres(),
		Redis:           loadRedis(),
		Log:             loadLog(),
		GRPCAddr:        Str("GRPC_ADDR", ":9090"),
		HTTPAddr:        Str("HTTP_ADDR", ":8080"),
		MetricsAddr:     Str("METRICS_ADDR", ":9100"),
		InstanceID:      Str("INSTANCE_ID", defaultInstanceID("queued")),
		ShutdownTimeout: Dur("SHUTDOWN_TIMEOUT", 20*time.Second),
		PromoteInterval: Dur("PROMOTE_INTERVAL", time.Second),
		PromoteBatch:    Int("PROMOTE_BATCH", 500),
		ReapInterval:    Dur("REAP_INTERVAL", 2*time.Second),
		ReapBatch:       Int("REAP_BATCH", 500),
		RollupInterval:  Dur("ROLLUP_INTERVAL", 15*time.Second),
		LeaderTTL:       Dur("LEADER_TTL", 15*time.Second),
		ReconcileOnBoot: Bool("RECONCILE_ON_BOOT", true),
		ReconcileBatch:  Int("RECONCILE_BATCH", 1000),
		DequeueMaxBatch: Int("DEQUEUE_MAX_BATCH", 64),
	}
	return c, c.Validate()
}

// Validate rejects configurations that would produce a running-but-useless
// service.
func (c QueueService) Validate() error {
	if err := c.Postgres.validate(); err != nil {
		return err
	}
	if err := c.Redis.validate(); err != nil {
		return err
	}
	if c.GRPCAddr == "" || c.HTTPAddr == "" {
		return errors.New("GRPC_ADDR and HTTP_ADDR are required")
	}
	if c.PromoteInterval <= 0 || c.ReapInterval <= 0 {
		return errors.New("PROMOTE_INTERVAL and REAP_INTERVAL must be positive")
	}
	if c.PromoteBatch < 1 || c.ReapBatch < 1 {
		return errors.New("PROMOTE_BATCH and REAP_BATCH must be >= 1")
	}
	if c.DequeueMaxBatch < 1 {
		return errors.New("DEQUEUE_MAX_BATCH must be >= 1")
	}
	// A leader lock that expires faster than three renewal periods would flap
	// under ordinary scheduling jitter, causing constant leadership churn.
	if c.LeaderTTL < 3*time.Second {
		return fmt.Errorf("LEADER_TTL must be >= 3s to survive renewal jitter, got %s",
			c.LeaderTTL)
	}
	return nil
}

// --- worker ---------------------------------------------------------------

// Worker is the configuration for the workerd binary.
type Worker struct {
	Log Log

	// QueueAddr is the queue service's gRPC endpoint.
	QueueAddr string
	// MetricsAddr serves Prometheus for this worker.
	MetricsAddr string

	// ID identifies this worker in leases, logs, and the job_attempts table.
	// It must be unique per process or lease-ownership checks become
	// meaningless, so it defaults to hostname+random rather than a fixed name.
	ID string

	// Queues this worker pulls from, in priority order.
	Queues []string

	// Concurrency is the number of jobs executed simultaneously, and therefore
	// the size of both the goroutine pool and the fetch-slot budget.
	Concurrency int

	// FetchMaxBatch caps jobs pulled per Dequeue call. Larger batches amortise
	// round trips; smaller ones spread work more evenly across workers.
	FetchMaxBatch int

	// EmptyPollInterval is how long to wait after an empty dequeue before
	// asking again. Prevents a hot loop against Redis on an idle queue.
	EmptyPollInterval time.Duration

	// JobTimeout bounds a single execution. Must be comfortably under the
	// queue's visibility timeout times the heartbeat renewal count, or a
	// long job would lose its lease mid-flight.
	JobTimeout time.Duration

	// HeartbeatInterval is how often in-flight leases are renewed in one
	// batched call.
	HeartbeatInterval time.Duration

	// DrainTimeout bounds graceful shutdown. Jobs still running when it
	// elapses are explicitly nacked for immediate reassignment.
	DrainTimeout time.Duration
}

// LoadWorker reads worker config from the environment.
func LoadWorker() (Worker, error) {
	c := Worker{
		Log:               loadLog(),
		QueueAddr:         Str("QUEUE_ADDR", "localhost:9090"),
		MetricsAddr:       Str("METRICS_ADDR", ":9101"),
		ID:                Str("WORKER_ID", defaultInstanceID("worker")),
		Queues:            List("WORKER_QUEUES", []string{"default"}),
		Concurrency:       Int("WORKER_CONCURRENCY", 16),
		FetchMaxBatch:     Int("WORKER_FETCH_BATCH", 16),
		EmptyPollInterval: Dur("WORKER_EMPTY_POLL_INTERVAL", 250*time.Millisecond),
		JobTimeout:        Dur("WORKER_JOB_TIMEOUT", 5*time.Minute),
		HeartbeatInterval: Dur("WORKER_HEARTBEAT_INTERVAL", 5*time.Second),
		DrainTimeout:      Dur("WORKER_DRAIN_TIMEOUT", 30*time.Second),
	}
	return c, c.Validate()
}

// Validate rejects worker configs that cannot make progress or that would
// systematically lose leases.
func (c Worker) Validate() error {
	if c.QueueAddr == "" {
		return errors.New("QUEUE_ADDR is required")
	}
	if c.ID == "" {
		return errors.New("WORKER_ID is required")
	}
	if len(c.Queues) == 0 {
		return errors.New("WORKER_QUEUES must name at least one queue")
	}
	if c.Concurrency < 1 {
		return fmt.Errorf("WORKER_CONCURRENCY must be >= 1, got %d", c.Concurrency)
	}
	if c.FetchMaxBatch < 1 {
		return fmt.Errorf("WORKER_FETCH_BATCH must be >= 1, got %d", c.FetchMaxBatch)
	}
	// Fetching more than the pool can hold would mean locking jobs under a
	// ticking lease while no goroutine is free to run them.
	if c.FetchMaxBatch > c.Concurrency {
		return fmt.Errorf("WORKER_FETCH_BATCH (%d) must not exceed WORKER_CONCURRENCY (%d): "+
			"jobs would be leased with no slot to run them",
			c.FetchMaxBatch, c.Concurrency)
	}
	if c.HeartbeatInterval <= 0 {
		return errors.New("WORKER_HEARTBEAT_INTERVAL must be positive")
	}
	if c.JobTimeout <= 0 {
		return errors.New("WORKER_JOB_TIMEOUT must be positive")
	}
	if c.DrainTimeout <= 0 {
		return errors.New("WORKER_DRAIN_TIMEOUT must be positive")
	}
	return nil
}

// defaultInstanceID builds a stable-per-process, unique-across-processes
// identifier. Hostname alone is not enough because docker compose scaling can
// place replicas on one host, and two workers sharing an ID would each believe
// they own the other's leases.
func defaultInstanceID(prefix string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%s-%d", prefix, host, os.Getpid())
}
