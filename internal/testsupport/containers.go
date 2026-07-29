// Package testsupport boots real Redis and Postgres containers for integration
// tests.
//
// These tests run against the real thing rather than mocks, because almost
// everything worth testing in this system is a property of Redis itself:
// whether a Lua script is atomic against concurrent clients, whether ZPOPMIN
// removes a member exactly once, whether a partial unique index actually
// rejects a duplicate under a race. A mock would encode the author's belief
// about those behaviours and then confirm it, which is precisely the class of
// bug that reaches production.
//
// Containers are started once per test package and shared. Each test calls
// Reset to get a clean slate, which is far cheaper than a container per test.
package testsupport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Images are pinned to match docker-compose.yml. Tests passing against a
// different Redis or Postgres than production runs would be a slow-burning
// source of false confidence.
const (
	postgresImage = "postgres:16-alpine"
	redisImage    = "redis:7-alpine"

	pgUser = "jobq"
	pgPass = "jobq"
	pgName = "jobq"
)

// Env is a running pair of containers plus connected clients.
type Env struct {
	RedisAddr   string
	PostgresURL string

	Redis *redis.Client
	Pool  *pgxpool.Pool

	terminate func()
}

var (
	sharedOnce sync.Once
	sharedEnv  *Env
	sharedErr  error
)

// Start returns the package-shared environment, booting it on first use.
//
// Skips the test rather than failing it when Docker is unavailable, so
// `go test ./...` still works on a machine without a Docker daemon. Use
// `go test -short` to skip these deliberately.
func Start(t *testing.T) *Env {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping container-backed integration test in -short mode")
	}

	sharedOnce.Do(func() {
		sharedEnv, sharedErr = startEnv()
	})

	if sharedErr != nil {
		if isDockerUnavailable(sharedErr) {
			t.Skipf("docker is not available, skipping integration test: %v", sharedErr)
		}
		t.Fatalf("could not start test environment: %v", sharedErr)
	}

	return sharedEnv
}

// Shutdown terminates the shared containers. Call it from TestMain after
// m.Run() so containers do not outlive the test binary.
func Shutdown() {
	if sharedEnv != nil && sharedEnv.terminate != nil {
		sharedEnv.terminate()
	}
}

func isDockerUnavailable(err error) bool {
	msg := err.Error()
	for _, s := range []string{
		"Cannot connect to the Docker daemon",
		"docker daemon",
		"permission denied while trying to connect",
		"rootless Docker not found",
		"failed to find a viable Docker host",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

func startEnv() (*Env, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	pgC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        postgresImage,
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     pgUser,
				"POSTGRES_PASSWORD": pgPass,
				"POSTGRES_DB":       pgName,
			},
			// Postgres briefly accepts connections during init and then
			// restarts. Requiring the readiness line twice is the documented
			// way to avoid connecting to that transient first instance and
			// getting a confusing "database is starting up" failure.
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90 * time.Second),
		},
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("start postgres container: %w", err)
	}
	cleanups = append(cleanups, func() {
		_ = pgC.Terminate(context.Background())
	})

	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		Started: true,
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        redisImage,
			ExposedPorts: []string{"6379/tcp"},
			// noeviction mirrors production. Under an eviction policy Redis
			// would silently drop queue keys at the memory ceiling, which is
			// indistinguishable from job loss.
			Cmd: []string{"redis-server", "--maxmemory-policy", "noeviction"},
			WaitingFor: wait.ForLog("Ready to accept connections").
				WithStartupTimeout(60 * time.Second),
		},
	})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("start redis container: %w", err)
	}
	cleanups = append(cleanups, func() {
		_ = redisC.Terminate(context.Background())
	})

	pgHost, err := pgC.Host(ctx)
	if err != nil {
		cleanup()
		return nil, err
	}
	pgPort, err := pgC.MappedPort(ctx, "5432/tcp")
	if err != nil {
		cleanup()
		return nil, err
	}
	redisHost, err := redisC.Host(ctx)
	if err != nil {
		cleanup()
		return nil, err
	}
	redisPort, err := redisC.MappedPort(ctx, "6379/tcp")
	if err != nil {
		cleanup()
		return nil, err
	}

	pgURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		pgUser, pgPass, pgHost, pgPort.Port(), pgName)
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("connect to test postgres: %w", err)
	}
	cleanups = append(cleanups, pool.Close)

	if err := waitForPostgres(ctx, pool); err != nil {
		cleanup()
		return nil, err
	}

	if err := applyMigrations(ctx, pool); err != nil {
		cleanup()
		return nil, fmt.Errorf("apply migrations: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	cleanups = append(cleanups, func() { _ = rdb.Close() })

	if err := rdb.Ping(ctx).Err(); err != nil {
		cleanup()
		return nil, fmt.Errorf("ping test redis: %w", err)
	}

	return &Env{
		RedisAddr:   redisAddr,
		PostgresURL: pgURL,
		Redis:       rdb,
		Pool:        pool,
		terminate:   cleanup,
	}, nil
}

func waitForPostgres(ctx context.Context, pool *pgxpool.Pool) error {
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error

	for time.Now().Before(deadline) {
		if err := pool.Ping(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("postgres did not become ready: %w", lastErr)
}

// applyMigrations runs the same .sql files docker compose applies, rather than
// a hand-maintained test schema. A test schema that drifts from the real one
// tests a database that does not exist.
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	dir, err := migrationsDir()
	if err != nil {
		return err
	}

	entries, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return fmt.Errorf("no .up.sql migrations found in %s", dir)
	}
	// Glob returns sorted results, and the files are sequence-numbered, so this
	// applies them in the same order golang-migrate would.
	for _, path := range entries {
		sql, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("exec %s: %w", filepath.Base(path), err)
		}
	}
	return nil
}

// migrationsDir locates the repository's migrations directory relative to this
// source file, so tests work regardless of the working directory `go test`
// chose.
func migrationsDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("could not determine caller location")
	}
	// internal/testsupport/containers.go -> repo root
	root := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	dir := filepath.Join(root, "migrations")

	if _, err := os.Stat(dir); err != nil {
		return "", fmt.Errorf("migrations directory not found at %s: %w", dir, err)
	}
	return dir, nil
}

// Reset restores a clean slate between tests.
//
// TRUNCATE with RESTART IDENTITY and CASCADE rather than DROP and re-migrate:
// it is an order of magnitude faster and keeps the schema, indexes, and
// constraints identical to what migrations produced.
func (e *Env) Reset(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	if err := e.Redis.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis: %v", err)
	}

	if _, err := e.Pool.Exec(ctx,
		`TRUNCATE job_attempts, jobs, queue_stats_minute, queues
         RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}

	// Restore the bootstrap queue the migration seeds, since TRUNCATE removed it.
	if _, err := e.Pool.Exec(ctx,
		`INSERT INTO queues (name, description) VALUES ('default', 'Default queue')`,
	); err != nil {
		t.Fatalf("reseed default queue: %v", err)
	}
}
