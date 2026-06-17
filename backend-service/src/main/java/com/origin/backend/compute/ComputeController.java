package com.origin.backend.compute;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/compute")
public class ComputeController {

    private static final Logger log = LoggerFactory.getLogger(ComputeController.class);

    /** Guard rail so a single request can't wedge the box for minutes. */
    private static final int MAX_LIMIT = 5_000_000;

    private final PrimeService primeService;
    private final InstanceInfo instance;
    private final Timer computeTimer;

    public ComputeController(PrimeService primeService,
                             InstanceInfo instance,
                             MeterRegistry registry) {
        this.primeService = primeService;
        this.instance = instance;
        // A named timer -> exposed at /actuator/prometheus as
        // compute_primes_seconds_{count,sum,max}. This is what the Grafana
        // latency panels will read.
        this.computeTimer = Timer.builder("compute.primes")
                .description("Time spent counting primes")
                .tag("instance", instance.id())
                .register(registry);
    }

    @GetMapping("/primes")
    public ComputeResult primes(@RequestParam(defaultValue = "100000") int limit) {
        int effectiveLimit = Math.min(Math.max(limit, 0), MAX_LIMIT);

        long start = System.nanoTime();
        long primeCount = computeTimer.record(() -> primeService.countPrimesUpTo(effectiveLimit));
        long tookMs = (System.nanoTime() - start) / 1_000_000;

        log.info("computed primes up to {} -> {} primes in {}ms", effectiveLimit, primeCount, tookMs);

        return new ComputeResult(instance.id(), effectiveLimit, primeCount, tookMs);
    }

    /** JSON response body. A record gives us an immutable, auto-serialized DTO. */
    public record ComputeResult(String instance, int limit, long primeCount, long tookMs) {
    }
}
