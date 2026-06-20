package com.origin.lb;

import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One backend instance the load balancer can route to.
 *
 * The mutable fields are atomic because they are read and written from many
 * request-handling virtual threads simultaneously (plus the health-check
 * thread), with no lock around them:
 *
 *   healthy            - flipped by {@link HealthChecker}, read on every route
 *   activeConnections  - in-flight requests right now; the signal that drives
 *                        the least-connections strategy. Incremented before a
 *                        request is forwarded, decremented when it completes.
 *   totalRequests      - lifetime counter, purely for /lb/status reporting.
 */
public final class Backend {

    private final String name;
    private final URI baseUri;

    // Start optimistic=false: we run one health check pass before serving any
    // traffic, so a backend only becomes routable once we've confirmed it's up.
    private final AtomicBoolean healthy = new AtomicBoolean(false);
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final AtomicLong totalRequests = new AtomicLong(0);

    public Backend(String name, URI baseUri) {
        this.name = name;
        this.baseUri = baseUri;
    }

    public String name() {
        return name;
    }

    public URI baseUri() {
        return baseUri;
    }

    public boolean isHealthy() {
        return healthy.get();
    }

    /**
     * Sets health and returns true if the state actually changed, so callers
     * can log transitions ("backend-2 went DOWN") instead of every poll.
     */
    public boolean setHealthy(boolean value) {
        return healthy.getAndSet(value) != value;
    }

    public int activeConnections() {
        return activeConnections.get();
    }

    /** Call when a request starts being forwarded to this backend. */
    public void acquire() {
        activeConnections.incrementAndGet();
        totalRequests.incrementAndGet();
    }

    /** Call when that request finishes (success or failure). Always pair with acquire(). */
    public void release() {
        activeConnections.decrementAndGet();
    }

    public long totalRequests() {
        return totalRequests.get();
    }

    @Override
    public String toString() {
        return name + "(" + baseUri + ")";
    }
}
