# Origin — Auto-Scaling Load Balancer Demo

Demonstration of how a horizontally-scaled web service actually works:
a cluster of identical backend instances sitting behind a **custom Java load balancer**,
with active health checking, pluggable routing strategies, and transparent failover,
built on the JDK.

> *Java microservice cluster with a custom load balancer and
> auto-scaling controller; load-tested with a self-built traffic generator, demonstrating
> significant latency reduction and automatic horizontal scaling under simulated traffic spikes.*

## Architecture 

```
                        ┌───────────────────────────────────────┐
                        │        load-balancer  (port 8088)      │
   client  ───────────► │                                        │
   curl / browser       │  com.sun.net.httpserver.HttpServer     │
                        │    "/"    → ProxyHandler                │
                        │    "/lb/" → StatusHandler (admin)       │
                        │                                        │
                        │  strategy: round-robin | least-conn    │
                        │  HealthChecker polls every 5s          │
                        └───────┬───────────┬───────────┬────────┘
                                │           │           │   java.net.http.HttpClient
                                │           │           │   (one virtual thread / request)
                                ▼           ▼           ▼
                        ┌───────────┐ ┌───────────┐ ┌───────────┐
                        │ backend-1 │ │ backend-2 │ │ backend-3 │   Spring Boot
                        │  :8080    │ │  :8080    │ │  :8080    │   /api/compute/primes
                        │           │ │           │ │           │   /actuator/health
                        └───────────┘ └───────────┘ └───────────┘   /actuator/prometheus
                          (private Compose network — no host ports)
```

Every request that enters the LB is tagged with an `X-Request-Id` that is forwarded to the
backend and echoed in logs on both sides, so one request is traceable end-to-end. The
response carries `X-Backend` (which instance served it) and `X-LB-Strategy` (how it was chosen).

---

## Repository layout

```
Origin/
├── docker-compose.yml          
├── backend-service/           
│   ├── Dockerfile              #   multi-stage build, non-root, HEALTHCHECK
│   ├── pom.xml
│   └── src/main/java/com/origin/backend/
│       ├── BackendServiceApplication.java
│       └── compute/
│           ├── ComputeController.java   # GET /api/compute/primes?limit=N
│           ├── PrimeService.java        # deliberately CPU-heavy prime count
│           ├── InstanceInfo.java        # "who am I" (INSTANCE_ID)
│           └── RequestIdFilter.java     # X-Request-Id → MDC → logs
│
└── load-balancer/             
    ├── Dockerfile
    ├── pom.xml                 
    └── src/main/java/com/origin/lb/
        ├── LoadBalancer.java             # main(): wires everything, starts server
        ├── Config.java                   # all tunables from env vars (12-factor)
        ├── Backend.java                  # one backend + atomic health/conn counters
        ├── BackendPool.java              # the set of backends; healthy() = routable
        ├── LoadBalancingStrategy.java    # interface + factory
        ├── RoundRobinStrategy.java
        ├── LeastConnectionsStrategy.java
        ├── HealthChecker.java            # active polling, evict + re-add
        ├── ProxyHandler.java             # forward + retry-failover + header hygiene
        └── StatusHandler.java            # /lb/status, /lb/health
```

---

## Quick start

Requires **Docker** (with Compose). No local Java or Maven needed — the images build
themselves via the bundled Maven wrapper.

```bash
# Build all four images and start the stack (3 backends, then the LB once they're healthy)
docker compose up --build -d

# Send traffic through the load balancer
curl "localhost:8088/api/compute/primes?limit=100000"

# See how traffic is distributed and which backends are healthy
curl localhost:8088/lb/status | jq

# Watch routing decisions live
docker compose logs -f load-balancer

# Tear it down
docker compose down
```

> **Port note:** the LB is published on host port **8088** (`8088:8080` in `docker-compose.yml`).
> Host `8080` is occupied by an unrelated service on the dev box. The backends are **not**
> published to the host at all — they're only reachable over the private Compose network by
> their service names (`backend-1`/`-2`/`-3`), which is exactly how a real cluster is wired.

Sample response (note the `instance` field rotating as you repeat the call):

```json
{ "instance": "backend-1", "limit": 100000, "primeCount": 9592, "tookMs": 34 }
```

---

## Backend service

A single Spring Boot app, built once and run as three identical containers. Each instance is
distinguishable, observable, and does real CPU work so scaling has a visible effect.

**`GET /api/compute/primes?limit=N`** — counts primes up to `N` (default `100000`, capped at
`5_000_000`). Returns `{instance, limit, primeCount, tookMs}`.

**Observability:**

- **`/actuator/health`** — used both by Docker's `HEALTHCHECK` and by the LB's health checker.
- **`/actuator/prometheus`** — the scrape endpoint Week 4's Grafana will read.
- **Per-instance identity** (`InstanceInfo`): resolves `INSTANCE_ID` → `HOSTNAME` → `"local"`.
  Set per container in Compose, so every log line and every metric is attributable to a
  specific backend — the only way to *prove* traffic is being distributed.
- **Per-request tracing** (`RequestIdFilter`): reads `X-Request-Id` (or mints one), puts it in
  the SLF4J **MDC** so it prints on every log line, and echoes it back as a response header.
  The MDC is cleared in a `finally` — pooled request threads are reused, and a leftover id
  would leak into the next unrelated request.
- **A named timer** (`compute.primes`, tagged by instance) so latency is queryable per backend.

**Containerization** (`backend-service/Dockerfile`):

- **Multi-stage build** — a JDK stage compiles the jar; a slim JRE stage runs it (smaller
  image, smaller attack surface).
- **Layer caching** — dependencies are resolved (`dependency:go-offline`) *before* source is
  copied, so editing code doesn't re-download the world.
- **Non-root user** and a **`HEALTHCHECK`** that greps `/actuator/health` for `"status":"UP"`.

---

## Load balancer

A reverse proxy written directly against the JDK: `HttpServer` accepts client requests,
`HttpClient` forwards them to a backend. 
