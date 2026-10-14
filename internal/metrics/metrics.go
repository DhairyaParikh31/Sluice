// Package metrics defines and records Prometheus metrics for Sluice.
//
// Metrics exposed at /metrics endpoint:
//
//	sluice_requests_total          - total requests by upstream and status
//	sluice_request_duration_seconds - request latency histogram by upstream
//	sluice_active_connections      - current active connections by upstream
//	sluice_rate_limited_total      - total rate-limited requests by client
//	sluice_upstream_errors_total   - total upstream errors by upstream
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds all Prometheus metric definitions for Sluice.
type Metrics struct {
	requestsTotal      *prometheus.CounterVec
	requestDuration    *prometheus.HistogramVec
	activeConnections  *prometheus.GaugeVec
	rateLimitedTotal   *prometheus.CounterVec
	upstreamErrors     *prometheus.CounterVec
}

// New creates and registers all Sluice metrics with the given registerer.
// Use prometheus.DefaultRegisterer for production, a new registry for tests.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sluice_requests_total",
				Help: "Total number of requests proxied, by upstream and HTTP status code.",
			},
			[]string{"upstream", "status_code"},
		),

		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "sluice_request_duration_seconds",
				Help: "Request latency in seconds by upstream.",
				// Buckets cover 1ms to 10s — typical proxy latency range.
				Buckets: []float64{
					0.001, 0.005, 0.010, 0.025, 0.050,
					0.100, 0.250, 0.500, 1.000, 2.500, 5.000, 10.000,
				},
			},
			[]string{"upstream"},
		),

		activeConnections: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "sluice_active_connections",
				Help: "Current number of active proxy connections by upstream.",
			},
			[]string{"upstream"},
		),

		rateLimitedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sluice_rate_limited_total",
				Help: "Total number of requests rejected by rate limiter.",
			},
			[]string{"client"},
		),

		upstreamErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "sluice_upstream_errors_total",
				Help: "Total number of upstream errors (connection refused, timeout, etc.).",
			},
			[]string{"upstream"},
		),
	}

	reg.MustRegister(
		m.requestsTotal,
		m.requestDuration,
		m.activeConnections,
		m.rateLimitedTotal,
		m.upstreamErrors,
	)

	return m
}

// RecordRequest records a completed proxied request.
//
//	upstream   - the backend URL host (e.g. "localhost:8081")
//	statusCode - HTTP response status code
//	duration   - total request duration including upstream processing
func (m *Metrics) RecordRequest(upstream string, statusCode int, duration time.Duration) {
	status := strconv.Itoa(statusCode)
	m.requestsTotal.WithLabelValues(upstream, status).Inc()
	m.requestDuration.WithLabelValues(upstream).Observe(duration.Seconds())
}

// RecordRateLimited increments the rate-limited counter for a client.
func (m *Metrics) RecordRateLimited(clientIP string) {
	m.rateLimitedTotal.WithLabelValues(clientIP).Inc()
}

// RecordUpstreamError increments the error counter for an upstream.
func (m *Metrics) RecordUpstreamError(upstream string) {
	m.upstreamErrors.WithLabelValues(upstream).Inc()
}

// ConnOpen increments the active connection gauge for an upstream.
// Call when a request is dispatched to an upstream.
func (m *Metrics) ConnOpen(upstream string) {
	m.activeConnections.WithLabelValues(upstream).Inc()
}

// ConnClose decrements the active connection gauge for an upstream.
// Call when a request to an upstream completes (success or error).
func (m *Metrics) ConnClose(upstream string) {
	m.activeConnections.WithLabelValues(upstream).Dec()
}

// Handler returns the HTTP handler for the /metrics endpoint.
// Mount this at /metrics on a separate admin port or the main server.
func Handler() http.Handler {
	return promhttp.Handler()
}

// HandlerFor returns a metrics handler for a specific registry.
// Used in tests to avoid polluting the default registry.
func HandlerFor(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}