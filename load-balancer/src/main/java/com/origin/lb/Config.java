package com.origin.lb;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

/**
 * All tunables, read once from environment variables at startup.
 *
 * Env vars (not files/flags) so the same jar is configured identically whether
 * it runs on your laptop or as a container in docker-compose — 12-factor style.
 * Every setting has a default that "just works" against three local backends.
 */
public record Config(
        int port,
        List<URI> backendUris,
        String strategy,
        String healthPath,
        long healthIntervalMs,
        long healthTimeoutMs,
        long requestTimeoutMs
) {

    public static Config fromEnv() {
        return new Config(
                intEnv("LB_PORT", 8080),
                parseBackends(env("LB_BACKENDS",
                        "http://localhost:8081,http://localhost:8082,http://localhost:8083")),
                env("LB_STRATEGY", "round-robin"),
                env("LB_HEALTH_PATH", "/actuator/health"),
                longEnv("LB_HEALTH_INTERVAL_MS", 5000),
                longEnv("LB_HEALTH_TIMEOUT_MS", 2000),
                longEnv("LB_REQUEST_TIMEOUT_MS", 10000)
        );
    }

    private static List<URI> parseBackends(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(URI::create)
                .toList();
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private static int intEnv(String key, int fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : Integer.parseInt(value.trim());
    }

    private static long longEnv(String key, long fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : Long.parseLong(value.trim());
    }
}
