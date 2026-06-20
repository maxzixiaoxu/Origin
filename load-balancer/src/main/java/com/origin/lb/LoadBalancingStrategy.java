package com.origin.lb;

import java.util.List;

/**
 * Chooses which backend should serve the next request.
 *
 * Implementations receive only healthy backends (already filtered by the
 * pool) and the list is guaranteed non-empty. This is the one decision that
 * differs between "round-robin" and "least-connections" — everything else in
 * the proxy path is identical, which is exactly why it's pulled out here.
 */
public interface LoadBalancingStrategy {

    Backend select(List<Backend> healthyBackends);

    /** Human-readable name for logs and /lb/status. */
    String name();

    /** Factory: map the LB_STRATEGY config value to an implementation. */
    static LoadBalancingStrategy fromName(String name) {
        return switch (name.trim().toLowerCase()) {
            case "least-connections", "least_connections", "lc" -> new LeastConnectionsStrategy();
            case "round-robin", "round_robin", "rr" -> new RoundRobinStrategy();
            default -> throw new IllegalArgumentException(
                    "Unknown strategy '" + name + "' (use round-robin or least-connections)");
        };
    }
}
