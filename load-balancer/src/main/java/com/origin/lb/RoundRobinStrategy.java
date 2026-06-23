package com.origin.lb;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hand each successive request to the next backend in order, wrapping around.
 * Simple and perfectly even when every request costs the same.
 */
public final class RoundRobinStrategy implements LoadBalancingStrategy {

    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public Backend select(List<Backend> healthyBackends) {
        // getAndIncrement() then floorMod so the index stays valid even after
        // the int counter overflows past Integer.MAX_VALUE into negatives.
        int index = Math.floorMod(counter.getAndIncrement(), healthyBackends.size());
        return healthyBackends.get(index);
    }

    @Override
    public String name() {
        return "round-robin";
    }
}
