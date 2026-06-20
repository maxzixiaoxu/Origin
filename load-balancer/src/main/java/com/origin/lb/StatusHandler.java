package com.origin.lb;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

/**
 * The LB's own admin surface, served under /lb/ so it never collides with
 * proxied traffic (HttpServer routes by longest-matching context path, so
 * "/lb/..." goes here and everything else goes to the ProxyHandler at "/").
 *
 *   GET /lb/health  -> 200, tiny json; the container liveness probe
 *   GET /lb/status  -> 200, per-backend health + live/total request counts
 *
 * JSON is assembled by hand because the LB has no dependencies.
 */
public final class StatusHandler implements HttpHandler {

    private final BackendPool pool;
    private final LoadBalancingStrategy strategy;

    public StatusHandler(BackendPool pool, LoadBalancingStrategy strategy) {
        this.pool = pool;
        this.strategy = strategy;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            String path = exchange.getRequestURI().getPath();
            String json = path.endsWith("/health") ? healthJson() : statusJson();
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        } finally {
            exchange.close();
        }
    }

    private String healthJson() {
        // Liveness = the LB process is up. We still report how many backends
        // are healthy so the probe response is informative.
        return "{\"status\":\"UP\",\"healthyBackends\":" + pool.healthy().size()
                + ",\"totalBackends\":" + pool.all().size() + "}";
    }

    private String statusJson() {
        StringJoiner backends = new StringJoiner(",", "[", "]");
        for (Backend b : pool.all()) {
            backends.add("{"
                    + "\"name\":\"" + b.name() + "\","
                    + "\"uri\":\"" + b.baseUri() + "\","
                    + "\"healthy\":" + b.isHealthy() + ","
                    + "\"activeConnections\":" + b.activeConnections() + ","
                    + "\"totalRequests\":" + b.totalRequests()
                    + "}");
        }
        return "{\"strategy\":\"" + strategy.name() + "\",\"backends\":" + backends + "}";
    }
}
