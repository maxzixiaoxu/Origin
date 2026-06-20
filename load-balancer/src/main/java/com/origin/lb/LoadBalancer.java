package com.origin.lb;

import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Entry point. Wires the pieces together and starts serving:
 *
 *   Config -> BackendPool -> HealthChecker (one pass, then periodic)
 *          -> HttpServer (virtual thread per request)
 *              "/"     -> ProxyHandler   (all real traffic)
 *              "/lb/"  -> StatusHandler  (admin: /lb/status, /lb/health)
 */
public final class LoadBalancer {

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();

        BackendPool pool = new BackendPool(buildBackends(config));
        LoadBalancingStrategy strategy = LoadBalancingStrategy.fromName(config.strategy());

        // One HttpClient shared by the proxy and the health checker. HTTP/1.1
        // keeps behaviour simple; virtual-thread executor matches the server.
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofMillis(2000))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();

        HealthChecker healthChecker = new HealthChecker(pool, client, config);

        printBanner(config, pool, strategy);

        // Learn backend states BEFORE accepting traffic, so we never route to a
        // backend we haven't confirmed is up.
        System.out.println("[lb] running initial health check...");
        healthChecker.checkAllOnce();
        healthChecker.start();

        HttpServer server = HttpServer.create(new InetSocketAddress(config.port()), 0);
        // The whole point of Java 21 here: one virtual thread per request means
        // the blocking backend call in ProxyHandler costs almost nothing.
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/", new ProxyHandler(pool, strategy, client, config));
        server.createContext("/lb/", new StatusHandler(pool, strategy));
        server.start();

        System.out.printf("[lb] listening on http://0.0.0.0:%d  (strategy=%s)%n",
                config.port(), strategy.name());

        // Clean shutdown on Ctrl-C / docker stop (SIGTERM).
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[lb] shutting down...");
            healthChecker.stop();
            server.stop(0);
        }));
    }

    private static List<Backend> buildBackends(Config config) {
        List<Backend> backends = new ArrayList<>();
        for (URI uri : config.backendUris()) {
            // authority = host:port, which is unique and readable even when
            // several backends share a host (e.g. localhost:8081/8082/8083).
            String name = uri.getAuthority() != null ? uri.getAuthority() : uri.toString();
            backends.add(new Backend(name, uri));
        }
        return backends;
    }

    private static void printBanner(Config config, BackendPool pool, LoadBalancingStrategy strategy) {
        System.out.println("=== Origin Load Balancer ===");
        System.out.println("  strategy : " + strategy.name());
        System.out.println("  port     : " + config.port());
        System.out.println("  health   : " + config.healthPath()
                + " every " + config.healthIntervalMs() + "ms");
        System.out.println("  backends :");
        for (Backend b : pool.all()) {
            System.out.println("    - " + b.name() + " (" + b.baseUri() + ")");
        }
    }
}
