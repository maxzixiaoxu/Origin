package com.origin.lb;

import java.util.List;

/**
 * The fixed set of backends this LB knows about.
 *
 * The list itself never changes here (Week 3's auto-scaler will make it
 * dynamic). What changes is each backend's health flag. {@link #healthy()}
 * is the single chokepoint that keeps unhealthy backends out of routing:
 * strategies only ever pick from what this returns.
 */
public final class BackendPool {

    private final List<Backend> backends;

    public BackendPool(List<Backend> backends) {
        if (backends.isEmpty()) {
            throw new IllegalArgumentException("Need at least one backend");
        }
        this.backends = List.copyOf(backends);
    }

    /** All backends, healthy or not (for health checks and status reporting). */
    public List<Backend> all() {
        return backends;
    }

    /** Only the backends currently passing health checks — the routable set. */
    public List<Backend> healthy() {
        return backends.stream().filter(Backend::isHealthy).toList();
    }
}
