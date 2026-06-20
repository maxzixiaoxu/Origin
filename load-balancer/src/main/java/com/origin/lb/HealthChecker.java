package com.origin.lb;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Actively polls every backend's health endpoint and flips its healthy flag.
 *
 * "Active" (the LB reaches out on a timer) rather than "passive" (infer health
 * from failed real requests). Active checks detect a recovered backend and add
 * it back to rotation without needing a user request to hit it first.
 *
 * A backend is healthy iff its health path returns HTTP 200 with a body that
 * contains "UP" (Spring Actuator returns {"status":"UP"}).
 */
public final class HealthChecker {

    private final BackendPool pool;
    private final HttpClient client;
    private final String healthPath;
    private final Duration timeout;
    private final long intervalMs;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "health-checker");
                t.setDaemon(true);
                return t;
            });

    public HealthChecker(BackendPool pool, HttpClient client, Config config) {
        this.pool = pool;
        this.client = client;
        this.healthPath = config.healthPath();
        this.timeout = Duration.ofMillis(config.healthTimeoutMs());
        this.intervalMs = config.healthIntervalMs();
    }

    /** One blocking pass over all backends. Called once before the server starts. */
    public void checkAllOnce() {
        for (Backend backend : pool.all()) {
            checkOne(backend);
        }
    }

    /** Begin periodic checks on a background daemon thread. */
    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAll, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    private void checkAll() {
        // Guard the whole pass: an uncaught exception would silently cancel
        // all future runs of a scheduleAtFixedRate task.
        try {
            for (Backend backend : pool.all()) {
                checkOne(backend);
            }
        } catch (Exception e) {
            System.err.println("[health] unexpected error during health sweep: " + e);
        }
    }

    private void checkOne(Backend backend) {
        boolean healthy;
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(backend.baseUri() + healthPath))
                    .timeout(timeout)
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            healthy = response.statusCode() == 200 && response.body().contains("UP");
        } catch (Exception e) {
            // Connection refused, timeout, DNS failure -> treat as down.
            healthy = false;
        }

        boolean changed = backend.setHealthy(healthy);
        if (changed) {
            System.out.printf("[health] %s is now %s%n", backend.name(), healthy ? "UP" : "DOWN");
        }
    }
}
