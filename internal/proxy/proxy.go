// Package proxy implements the core HTTP reverse proxy for Sluice.
//
// Request flow through the proxy:
//
//  1. Extract client IP for rate limiting
//  2. Check rate limiter — return 429 if exceeded
//  3. Pick upstream via load balancer
//  4. Forward request using httputil.ReverseProxy
//  5. Record metrics (duration, status code, errors)
//
// Connection pooling is configured on the http.Transport to reuse
// TCP connections to upstreams — reducing per-request latency
// significantly compared to opening a new connection per request.
package proxy

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/DhairyaParikh31/sluice/internal/balancer"
	"github.com/DhairyaParikh31/sluice/internal/metrics"
	"github.com/DhairyaParikh31/sluice/internal/ratelimiter"
)

// Config holds all configuration for the Sluice proxy.
type Config struct {
	// Balancer selects which upstream handles each request.
	Balancer balancer.Balancer

	// Limiter controls per-client request rate.
	// If nil, rate limiting is disabled.
	Limiter *ratelimiter.Limiter

	// Metrics records Prometheus metrics.
	// If nil, metrics recording is disabled.
	Metrics *metrics.Metrics

	// Transport is the HTTP transport used to connect to upstreams.
	// If nil, a default transport with connection pooling is used.
	Transport http.RoundTripper
}

// Proxy is the core HTTP handler for Sluice.
// It implements http.Handler — register it with http.ListenAndServe.
type Proxy struct {
	cfg       Config
	transport http.RoundTripper
}

// New creates a Proxy from the given Config.
// If Config.Transport is nil, a connection-pooled transport is created.
func New(cfg Config) *Proxy {
	transport := cfg.Transport
	if transport == nil {
		transport = &http.Transport{
			// Connection pooling — reuse TCP connections to upstreams.
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     90 * time.Second,

			// Dial timeout — fail fast if upstream is unreachable.
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,

			// Response header timeout — upstream must start responding.
			ResponseHeaderTimeout: 30 * time.Second,

			// TLS handshake timeout.
			TLSHandshakeTimeout: 10 * time.Second,
		}
	}

	return &Proxy{cfg: cfg, transport: transport}
}

// ServeHTTP handles an incoming HTTP request.
// Implements http.Handler.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// ── Step 1: Extract client IP ──────────────────────────────────────
	clientIP := extractClientIP(r)

	// ── Step 2: Rate limiting ──────────────────────────────────────────
	if p.cfg.Limiter != nil && !p.cfg.Limiter.Allow(clientIP) {
		if p.cfg.Metrics != nil {
			p.cfg.Metrics.RecordRateLimited(clientIP)
		}
		http.Error(w, "429 Too Many Requests — rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// ── Step 3: Pick upstream ──────────────────────────────────────────
	upstream, err := p.cfg.Balancer.Next()
	if err != nil {
		http.Error(w, "503 Service Unavailable — no upstreams", http.StatusServiceUnavailable)
		return
	}

	upstreamHost := upstream.URL.Host

	// Track active connections for least-conn balancer.
	if lc, ok := p.cfg.Balancer.(*balancer.LeastConnBalancer); ok {
		defer lc.Done(upstream)
	}

	if p.cfg.Metrics != nil {
		p.cfg.Metrics.SetActiveConns(upstreamHost, upstream.ActiveConns)
	}

	// ── Step 4: Forward request ────────────────────────────────────────
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

	reverseProxy := &httputil.ReverseProxy{
		Director:  director(upstream.URL),
		Transport: p.transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if p.cfg.Metrics != nil {
				p.cfg.Metrics.RecordUpstreamError(upstreamHost)
			}
			http.Error(w, fmt.Sprintf("502 Bad Gateway — upstream error: %v", err),
				http.StatusBadGateway)
		},
	}

	reverseProxy.ServeHTTP(rw, r)

	// ── Step 5: Record metrics ─────────────────────────────────────────
	if p.cfg.Metrics != nil {
		p.cfg.Metrics.RecordRequest(upstreamHost, rw.statusCode, time.Since(start))
	}
}

// director returns a function that modifies the request before forwarding.
// It sets the correct scheme, host, and path for the upstream.
func director(target *url.URL) func(*http.Request) {
	return func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host

		// Preserve the original path — don't rewrite it.
		if target.Path != "" && target.Path != "/" {
			req.URL.Path = target.Path + req.URL.Path
		}

		// Set X-Forwarded-For so upstreams know the original client IP.
		if clientIP, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
			if prior, ok := req.Header["X-Forwarded-For"]; ok {
				clientIP = strings.Join(prior, ", ") + ", " + clientIP
			}
			req.Header.Set("X-Forwarded-For", clientIP)
		}

		// Set X-Forwarded-Host.
		req.Header.Set("X-Forwarded-Host", req.Host)

		// Remove hop-by-hop headers that should not be forwarded.
		req.Header.Del("Te")
		req.Header.Del("Trailers")
	}
}

// extractClientIP extracts the client IP from the request.
// Checks X-Forwarded-For first (for clients behind a load balancer),
// then falls back to RemoteAddr.
func extractClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// X-Forwarded-For may be a comma-separated list — take the first.
		parts := strings.Split(xff, ",")
		ip := strings.TrimSpace(parts[0])
		if ip != "" {
			return ip
		}
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// responseWriter wraps http.ResponseWriter to capture the status code.
// httputil.ReverseProxy calls WriteHeader internally — we intercept it
// to record the status code for metrics.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}