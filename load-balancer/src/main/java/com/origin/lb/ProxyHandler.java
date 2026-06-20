package com.origin.lb;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * The reverse-proxy request path. One instance handles every request; each
 * request runs on its own virtual thread (see LoadBalancer's server executor),
 * so the blocking {@code client.send()} call below is cheap — it parks the
 * virtual thread without tying up an OS thread.
 *
 * Flow per request:
 *   1. resolve a request id (for end-to-end tracing)
 *   2. buffer the request body (so a failed attempt can be safely replayed)
 *   3. pick a healthy backend via the strategy, forward, stream response back
 *   4. if the backend errors, drop it from this request's candidate list and
 *      try the next one; only 502 once every healthy backend has failed
 */
public final class ProxyHandler implements HttpHandler {

    /**
     * Headers we must NOT copy onto the outgoing request. Two groups:
     *  - hop-by-hop headers (connection-scoped, meaningless to forward)
     *  - headers HttpClient manages itself and rejects if you set them
     * Plus the two we set explicitly below (x-request-id, x-forwarded-for).
     */
    private static final Set<String> REQUEST_SKIP = Set.of(
            "connection", "keep-alive", "proxy-connection", "proxy-authenticate",
            "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade",
            "host", "content-length", "expect", "x-request-id", "x-forwarded-for");

    /** Response headers we regenerate ourselves; don't copy from the backend. */
    private static final Set<String> RESPONSE_SKIP = Set.of(
            "connection", "keep-alive", "te", "trailer", "transfer-encoding",
            "upgrade", "content-length");

    public static final String REQUEST_ID_HEADER = "X-Request-Id";

    private final BackendPool pool;
    private final LoadBalancingStrategy strategy;
    private final HttpClient client;
    private final Duration requestTimeout;

    public ProxyHandler(BackendPool pool, LoadBalancingStrategy strategy,
                        HttpClient client, Config config) {
        this.pool = pool;
        this.strategy = strategy;
        this.client = client;
        this.requestTimeout = Duration.ofMillis(config.requestTimeoutMs());
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        long start = System.nanoTime();
        String requestId = resolveRequestId(exchange);
        try {
            // A fresh mutable copy: we remove backends from THIS request's
            // candidate list as they fail, without touching the shared pool.
            List<Backend> candidates = new ArrayList<>(pool.healthy());
            if (candidates.isEmpty()) {
                sendError(exchange, 503, "No healthy backends available");
                return;
            }

            byte[] requestBody = exchange.getRequestBody().readAllBytes();
            String method = exchange.getRequestMethod();
            String pathAndQuery = pathAndQuery(exchange.getRequestURI());

            while (!candidates.isEmpty()) {
                Backend backend = strategy.select(candidates);
                backend.acquire();
                try {
                    HttpResponse<byte[]> response =
                            forward(backend, method, pathAndQuery, requestBody, exchange, requestId);
                    long ms = (System.nanoTime() - start) / 1_000_000;
                    System.out.printf("[proxy] %s %s %s -> %s status=%d %dms%n",
                            requestId, method, pathAndQuery, backend.name(), response.statusCode(), ms);
                    writeResponse(exchange, backend, response);
                    return;
                } catch (IOException | InterruptedException e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    System.out.printf("[proxy] %s forward to %s failed (%s) -> trying another backend%n",
                            requestId, backend.name(), e);
                    candidates.remove(backend);
                } finally {
                    backend.release();
                }
            }

            // Every healthy backend failed for this request.
            sendError(exchange, 502, "All backends failed to handle the request");
        } catch (Exception e) {
            System.err.printf("[proxy] %s unexpected error: %s%n", requestId, e);
            trySendError(exchange, 500, "Load balancer error");
        } finally {
            exchange.close();
        }
    }

    private HttpResponse<byte[]> forward(Backend backend, String method, String pathAndQuery,
                                         byte[] requestBody, HttpExchange exchange, String requestId)
            throws IOException, InterruptedException {

        URI target = URI.create(backend.baseUri() + pathAndQuery);
        HttpRequest.Builder builder = HttpRequest.newBuilder(target).timeout(requestTimeout);

        // Copy client headers, minus the ones we must not forward.
        for (Map.Entry<String, List<String>> header : exchange.getRequestHeaders().entrySet()) {
            if (REQUEST_SKIP.contains(header.getKey().toLowerCase())) {
                continue;
            }
            for (String value : header.getValue()) {
                builder.header(header.getKey(), value);
            }
        }
        // Standard proxy headers: trace id follows the request, XFF records origin.
        builder.header(REQUEST_ID_HEADER, requestId);
        builder.header("X-Forwarded-For", clientIp(exchange));

        HttpRequest.BodyPublisher body = requestBody.length > 0
                ? HttpRequest.BodyPublishers.ofByteArray(requestBody)
                : HttpRequest.BodyPublishers.noBody();
        builder.method(method, body);

        return client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private void writeResponse(HttpExchange exchange, Backend backend, HttpResponse<byte[]> response) {
        Headers out = exchange.getResponseHeaders();
        response.headers().map().forEach((name, values) -> {
            if (!RESPONSE_SKIP.contains(name.toLowerCase())) {
                out.put(name, new ArrayList<>(values));
            }
        });
        // Let the caller see which backend answered and how it was chosen.
        out.set("X-Backend", backend.name());
        out.set("X-LB-Strategy", strategy.name());

        try {
            byte[] payload = response.body();
            if (payload.length == 0) {
                exchange.sendResponseHeaders(response.statusCode(), -1);
            } else {
                exchange.sendResponseHeaders(response.statusCode(), payload.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(payload);
                }
            }
        } catch (IOException e) {
            // Client hung up before we finished writing — nothing to recover.
            System.err.println("[proxy] failed writing response to client: " + e);
        }
    }

    private String resolveRequestId(HttpExchange exchange) {
        String existing = exchange.getRequestHeaders().getFirst(REQUEST_ID_HEADER);
        if (existing != null && !existing.isBlank()) {
            return existing;
        }
        return UUID.randomUUID().toString().substring(0, 8);
    }

    private String pathAndQuery(URI uri) {
        String path = uri.getRawPath();
        String query = uri.getRawQuery();
        return query == null ? path : path + "?" + query;
    }

    private String clientIp(HttpExchange exchange) {
        InetSocketAddress remote = exchange.getRemoteAddress();
        return remote == null ? "unknown" : remote.getAddress().getHostAddress();
    }

    private void sendError(HttpExchange exchange, int status, String message) throws IOException {
        byte[] body = (message + "\n").getBytes();
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private void trySendError(HttpExchange exchange, int status, String message) {
        try {
            sendError(exchange, status, message);
        } catch (IOException ignored) {
            // Response already committed or client gone; nothing we can do.
        }
    }
}
