package com.origin.lb;

import java.util.List;

/**
 * Route to the backend with the fewest requests currently in flight.
 *
 * Unlike round-robin, this reacts to real load: a backend stuck on a slow
 * request keeps a high active count and stops receiving new work until it
 * drains. That's what makes latency more even when request cost varies (our
 * prime endpoint's cost scales with ?limit).
 */
public final class LeastConnectionsStrategy implements LoadBalancingStrategy {

    @Override
    public Backend select(List<Backend> healthyBackends) {
        Backend best = null;
        for (Backend candidate : healthyBackends) {
            if (best == null || isBetter(candidate, best)) {
                best = candidate;
            }
        }
        return best;
    }

    /**
     * Primary key: fewer active connections. Tie-break: fewer lifetime
     * requests, so when several backends are equally idle (the common case
     * under light load) we still spread traffic instead of always hammering
     * the first one in the list.
     */
    private boolean isBetter(Backend candidate, Backend current) {
        int byActive = Integer.compare(candidate.activeConnections(), current.activeConnections());
        if (byActive != 0) {
            return byActive < 0;
        }
        return candidate.totalRequests() < current.totalRequests();
    }

    @Override
    public String name() {
        return "least-connections";
    }
}
