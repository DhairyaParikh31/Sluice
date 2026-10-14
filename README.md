# Sluice

A production-grade HTTP reverse proxy built from scratch in Go — implementing round-robin, weighted, and least-connections load balancing; per-client token bucket rate limiting without any external library; connection pooling; and a live Prometheus + Grafana metrics dashboard.

This is not a tutorial follow-along. Every component — the token bucket algorithm, load balancing strategies, proxy middleware chain, and metrics pipeline — is implemented from first principles.

```
$ go run ./cmd/server \
    --port 8080 \
    --upstreams http://localhost:9001,http://localhost:9002 \
    --strategy least-conn \
    --rate 100 \
    --capacity 200

Sluice reverse proxy
  Listening:  :8080
  Strategy:   least-conn
  Upstreams:  2
    → http://localhost:9001 (weight=1)
    → http://localhost:9002 (weight=1)
  Rate limit: 100 req/s (burst: 200)
  Metrics:    http://localhost:8080/metrics
```

---

## Why I built this

After two years of optimizing backend performance in production systems, I wanted to understand how reverse proxies like Nginx and Caddy make load balancing and rate limiting decisions at the systems level — not just how to configure them, but how they work internally.

Sluice was built to answer three specific questions:

1. **How does a token bucket rate limiter work under concurrent load?** What are the locking tradeoffs between per-client granularity and throughput?
2. **When does least-connections outperform round-robin?** Under what traffic patterns does dynamic routing produce measurably better latency?
3. **What does connection pooling actually do to latency?** What is the per-request cost of opening a new TCP connection vs reusing one?

---

## Architecture

```
Incoming HTTP Request
        │
        ▼
┌───────────────────┐
│   Rate Limiter    │  Token bucket per client IP
│  (tokenbucket.go) │  Allow() → O(1), fine-grained mutex
└────────┬──────────┘
         │ token available → continue
         │ bucket empty    → HTTP 429
         ▼
┌───────────────────┐
│   Load Balancer   │  Pick upstream by strategy
│  (balancer.go)    │  Round-Robin: atomic counter, lock-free
│                   │  Weighted:    expanded sequence, O(1)
│                   │  Least-Conn:  atomic int64 per upstream
└────────┬──────────┘
         │ upstream selected
         ▼
┌───────────────────┐
│   Reverse Proxy   │  Forward via httputil.ReverseProxy
│   (proxy.go)      │  Connection pooling via http.Transport
│                   │  X-Forwarded-For propagation
│                   │  Error handler → HTTP 502
└────────┬──────────┘
         │ response received
         ▼
┌───────────────────┐
│   Metrics         │  Record duration, status code, errors
│   (metrics.go)    │  Prometheus counters + histograms
└────────┬──────────┘
         │
         ▼
   Return response to client
```

---

## Benchmark Results

**Hardware:** AMD Ryzen 5 4600H · Windows 11 · ASUS TUF A15 · 16GB DDR4  
**Methodology:** `hey -c 50 -z 10s` · 50 concurrent clients · 10 second duration

### Throughput — normal operation (rate limit: 1000 req/s)

| Metric | Value |
|--------|-------|
| Requests/sec | **15,321** |
| Average latency | 3.3ms |
| p50 latency | 0.9ms |
| p95 latency | 20.8ms |
| p99 latency | 40.8ms |
| Fastest response | 0.1ms |
| Slowest response | 128.5ms |

### Throughput — rate limiter stress test (rate limit: 10 req/s, burst: 20)

| Metric | Value |
|--------|-------|
| Requests/sec | **51,965** |
| Average latency | 1.0ms |
| p50 latency | 0.8ms |
| p95 latency | 2.2ms |
| p99 latency | 4.0ms |
| HTTP 200 (allowed) | 63 responses |
| HTTP 429 (rate limited) | 259,837 responses |

**Key insight:** The rate limiter correctly rejects 99.97% of requests when the bucket is exhausted — while maintaining sub-millisecond average latency. The token bucket runs in O(1) with a fine-grained per-client mutex — clients never block each other.

### Load balancing strategy comparison

All three strategies tested under identical load (50 concurrent clients, 10s):

| Strategy | Throughput | p50 | p99 | Best for |
|----------|-----------|-----|-----|----------|
| Round-Robin | ~15,000 req/s | 0.9ms | 40ms | Uniform request cost |
| Weighted | ~15,000 req/s | 0.9ms | 40ms | Mixed backend capacity |
| Least-Conn | ~15,300 req/s | 0.8ms | 38ms | Variable request duration |

**Key insight:** Least-connections shows marginally better tail latency when one upstream slows down — it naturally routes away from the slow backend as its connection count grows. Round-robin keeps sending requests regardless of upstream health.

---

## Core Components

### Token Bucket Rate Limiter

One bucket per client IP, created lazily on first request. Refill is continuous and time-based — not batch-based — giving smooth rate enforcement without the 2× burst problem of fixed-window rate limiting.

**Mathematics:**
```
tokens_now = min(capacity, tokens_last + rate × elapsed_seconds)
allowed    = tokens_now >= 1.0
if allowed: tokens_now -= 1.0
```

**Concurrency design:**
- Each bucket has its own `sync.Mutex` — clients never block each other
- The bucket map uses `sync.RWMutex` with double-checked locking on creation
- `Allow()` is O(1) regardless of total client count

**Why token bucket over alternatives:**
- Fixed window: allows 2× burst at window boundary (known flaw)
- Sliding window: accurate but O(n) memory per client
- Token bucket: controlled bursting up to capacity, O(1) per check, minimal memory

### Load Balancer

Three strategies implement the `Balancer` interface — the proxy calls `Next()` without knowing which strategy is active:

**Round-Robin** — uses `atomic.Uint64` counter. Completely lock-free. O(1).

**Weighted** — expands weights into a flat sequence at construction time. `[A(w=3), B(w=1)]` → `[A, A, A, B]`. O(1) per request after O(total_weight) construction.

**Least-Connections** — uses `atomic.Int64` per upstream for connection counting. O(n) scan over upstreams (n is typically 2-20, negligible). Caller must call `Done(upstream)` after request completes.

### Reverse Proxy

Built on `net/http/httputil.ReverseProxy` with a configured `http.Transport` for connection pooling:

```go
transport = &http.Transport{
    MaxIdleConns:        200,
    MaxIdleConnsPerHost: 50,
    IdleConnTimeout:     90 * time.Second,
    DialContext: (&net.Dialer{
        Timeout:   5 * time.Second,
        KeepAlive: 30 * time.Second,
    }).DialContext,
}
```

Connection pooling reuses TCP connections to upstreams — eliminating the per-request TCP handshake cost that dominates latency at high throughput.

### Prometheus Metrics

Five metrics exposed at `/metrics`:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `sluice_requests_total` | Counter | upstream, status_code | Total proxied requests |
| `sluice_request_duration_seconds` | Histogram | upstream | End-to-end latency |
| `sluice_active_connections` | Gauge | upstream | In-flight connections |
| `sluice_rate_limited_total` | Counter | client | Rejected requests |
| `sluice_upstream_errors_total` | Counter | upstream | Connection errors |

---

## Getting Started

### Prerequisites
- Go 1.22+
- Docker + Docker Compose (for Grafana dashboard)
- Windows, Linux, or macOS

### Install

```bash
git clone https://github.com/DhairyaParikh31/sluice
cd sluice
go mod tidy
go test ./...
```

### Build

```bash
go build -o bin/sluice ./cmd/server
```

### Run

```bash
# Start two fake backends (Python built-in HTTP server)
python -m http.server 9001  # terminal 1
python -m http.server 9002  # terminal 2

# Start Sluice
./bin/sluice \
  --port 8080 \
  --upstreams http://localhost:9001,http://localhost:9002 \
  --strategy least-conn \
  --rate 100 \
  --capacity 200
```

### Benchmark with hey

```bash
# Install hey
go install github.com/rakyll/hey@latest

# Run load test
hey -c 50 -z 10s http://localhost:8080/
```

### Prometheus + Grafana Dashboard

```bash
# Start monitoring stack
docker-compose up -d

# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000 (admin/admin)
```

Add Prometheus as a Grafana datasource (`http://host.docker.internal:9090`), then create panels for:
- `rate(sluice_requests_total[30s])` — requests per second
- `histogram_quantile(0.95, rate(sluice_request_duration_seconds_bucket[30s]))` — p95 latency
- `sluice_active_connections` — live connection gauge
- `rate(sluice_rate_limited_total[30s])` — rate limit rejections

---

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `8080` | Port to listen on |
| `--upstreams` | required | Comma-separated upstream URLs |
| `--strategy` | `round-robin` | `round-robin`, `weighted`, `least-conn` |
| `--rate` | `100` | Token refill rate per second per client |
| `--capacity` | `200` | Token bucket capacity (burst limit) per client |
| `--weights` | `""` | Comma-separated weights for `weighted` strategy |

---

## Project Structure

```
sluice/
├── cmd/server/main.go              # Entry point — flags, wiring, HTTP server
├── internal/
│   ├── ratelimiter/
│   │   ├── tokenbucket.go          # Token bucket — per-client, no external libs
│   │   └── tokenbucket_test.go
│   ├── balancer/
│   │   ├── balancer.go             # Round-robin, weighted, least-connections
│   │   └── balancer_test.go
│   ├── proxy/
│   │   ├── proxy.go                # HTTP reverse proxy with middleware chain
│   │   └── proxy_test.go
│   └── metrics/
│       └── metrics.go              # Prometheus metrics — 5 collectors
├── docker-compose.yml              # Prometheus + Grafana stack
├── prometheus.yml                  # Scrape config for Sluice /metrics
└── README.md
```

---

## GT Course Mappings

| Course | Concepts Demonstrated |
|--------|----------------------|
| **CS 6250** Computer Networks | HTTP proxying, TCP connection pooling, X-Forwarded-For, request routing |
| **CS 6210** Advanced OS | Goroutine scheduling, mutex design, atomic operations, connection lifecycle |
| **CS 7260** Internetworking Architectures | Load balancing algorithms, traffic shaping, rate limiting, latency analysis |

---

## Limitations

- No TLS termination (HTTPS upstream support only via pass-through)
- No health checks — failed upstreams remain in rotation until request fails
- No WAL/persistence — rate limit buckets are in-memory only, reset on restart
- Single process — no clustering or distributed rate limiting

---

## Related Projects

- [VaultKV](https://github.com/DhairyaParikh31/vaultkv) — disk-persistent key-value storage engine with WAL and LSM-tree
- [CruxDB](https://github.com/DhairyaParikh31/cruxdb) — in-memory SQL database with hand-written parser and B-tree indexing

---

## License

MIT
