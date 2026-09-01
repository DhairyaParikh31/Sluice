// Package balancer implements three load balancing strategies for Sluice.
//
// All three implement the Balancer interface — the proxy calls Next()
// to get the next upstream URL without knowing which strategy is active.
//
// Strategy comparison:
//
//	Round-Robin:       simple, fair, O(1) — best for uniform request cost
//	Weighted:          proportional traffic, O(1) — best for mixed capacity
//	Least-Connections: routes to least busy upstream — best for variable
//	                   request processing time (e.g. mixed fast/slow APIs)
//
// All three are safe for concurrent use.
package balancer

import (
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
)

// ErrNoUpstreams is returned when Next() is called with no upstreams registered.
var ErrNoUpstreams = errors.New("balancer: no upstreams available")

// Upstream represents a single backend server.
type Upstream struct {
	URL         *url.URL
	Weight      int    // used by WeightedBalancer only
	ActiveConns int64  // used by LeastConnBalancer — atomically updated
}

// NewUpstream parses rawURL and returns an Upstream.
func NewUpstream(rawURL string, weight int) (*Upstream, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if weight <= 0 {
		weight = 1
	}
	return &Upstream{URL: u, Weight: weight}, nil
}

// Balancer is the interface all load balancing strategies implement.
type Balancer interface {
	// Next returns the next upstream to forward a request to.
	Next() (*Upstream, error)

	// Upstreams returns all registered upstreams.
	Upstreams() []*Upstream
}

// ── Round-Robin ────────────────────────────────────────────────────────

// RoundRobin distributes requests evenly across all upstreams in order.
// Uses an atomic counter for lock-free operation.
//
// Given upstreams [A, B, C]:
// Request 1 → A, Request 2 → B, Request 3 → C, Request 4 → A, ...
type RoundRobin struct {
	upstreams []*Upstream
	counter   atomic.Uint64
}

// NewRoundRobin creates a round-robin balancer for the given upstreams.
func NewRoundRobin(upstreams []*Upstream) (*RoundRobin, error) {
	if len(upstreams) == 0 {
		return nil, ErrNoUpstreams
	}
	return &RoundRobin{upstreams: upstreams}, nil
}

// Next returns the next upstream using atomic modulo arithmetic.
// Lock-free — uses atomic.Uint64 for the counter.
func (r *RoundRobin) Next() (*Upstream, error) {
	if len(r.upstreams) == 0 {
		return nil, ErrNoUpstreams
	}
	idx := r.counter.Add(1) - 1
	return r.upstreams[idx%uint64(len(r.upstreams))], nil
}

// Upstreams returns all registered upstreams.
func (r *RoundRobin) Upstreams() []*Upstream {
	return r.upstreams
}

// ── Weighted Round-Robin ───────────────────────────────────────────────

// WeightedBalancer distributes requests proportionally to upstream weights.
//
// Example: upstreams A(weight=3), B(weight=1) → A gets 75%, B gets 25%.
//
// Implementation: expand weights into a flat sequence and use round-robin
// over the sequence. [A, A, A, B] for weights [3, 1].
// O(1) per request after O(total_weight) construction.
type WeightedBalancer struct {
	sequence []*Upstream // expanded weight sequence
	counter  atomic.Uint64
}

// NewWeightedBalancer creates a weighted balancer.
// Each upstream's weight determines its share of traffic.
func NewWeightedBalancer(upstreams []*Upstream) (*WeightedBalancer, error) {
	if len(upstreams) == 0 {
		return nil, ErrNoUpstreams
	}

	// Expand into flat sequence proportional to weights.
	var sequence []*Upstream
	for _, u := range upstreams {
		w := u.Weight
		if w <= 0 {
			w = 1
		}
		for i := 0; i < w; i++ {
			sequence = append(sequence, u)
		}
	}

	if len(sequence) == 0 {
		return nil, ErrNoUpstreams
	}

	return &WeightedBalancer{sequence: sequence}, nil
}

// Next returns the next upstream based on weight-proportional distribution.
func (w *WeightedBalancer) Next() (*Upstream, error) {
	if len(w.sequence) == 0 {
		return nil, ErrNoUpstreams
	}
	idx := w.counter.Add(1) - 1
	return w.sequence[idx%uint64(len(w.sequence))], nil
}

// Upstreams returns the unique set of upstreams (deduplicated from sequence).
func (w *WeightedBalancer) Upstreams() []*Upstream {
	seen := make(map[*Upstream]bool)
	var result []*Upstream
	for _, u := range w.sequence {
		if !seen[u] {
			seen[u] = true
			result = append(result, u)
		}
	}
	return result
}

// ── Least Connections ──────────────────────────────────────────────────

// LeastConnBalancer routes each request to the upstream with the fewest
// active connections at that moment.
//
// Best strategy when requests have variable processing time — slow
// upstreams naturally receive fewer new requests as their connection
// count grows, while fast upstreams continue receiving traffic.
//
// Active connection tracking:
//   - Call Done() on the upstream when the request completes
//   - Failure to call Done() will cause the upstream to appear
//     permanently busy and receive no new traffic
type LeastConnBalancer struct {
	mu        sync.RWMutex
	upstreams []*Upstream
}

// NewLeastConnBalancer creates a least-connections balancer.
func NewLeastConnBalancer(upstreams []*Upstream) (*LeastConnBalancer, error) {
	if len(upstreams) == 0 {
		return nil, ErrNoUpstreams
	}
	return &LeastConnBalancer{upstreams: upstreams}, nil
}

// Next returns the upstream with the fewest active connections.
// Scans all upstreams — O(n) where n = number of upstreams.
// For typical proxy deployments (2-20 upstreams) this is negligible.
func (l *LeastConnBalancer) Next() (*Upstream, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.upstreams) == 0 {
		return nil, ErrNoUpstreams
	}

	best := l.upstreams[0]
	bestConns := atomic.LoadInt64(&best.ActiveConns)

	for _, u := range l.upstreams[1:] {
		conns := atomic.LoadInt64(&u.ActiveConns)
		if conns < bestConns {
			best = u
			bestConns = conns
		}
	}

	// Increment active connection count before returning.
	// Caller must call Done(upstream) when request completes.
	atomic.AddInt64(&best.ActiveConns, 1)
	return best, nil
}

// Done decrements the active connection count for an upstream.
// Must be called when a proxied request completes (success or error).
// Typically called in a defer after Next().
func (l *LeastConnBalancer) Done(u *Upstream) {
	atomic.AddInt64(&u.ActiveConns, -1)
}

// Upstreams returns all registered upstreams.
func (l *LeastConnBalancer) Upstreams() []*Upstream {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.upstreams
}

// ── Factory ────────────────────────────────────────────────────────────

// Strategy identifies a load balancing algorithm.
type Strategy string

const (
	StrategyRoundRobin   Strategy = "round-robin"
	StrategyWeighted     Strategy = "weighted"
	StrategyLeastConn    Strategy = "least-conn"
)

// New creates a Balancer for the given strategy and upstreams.
func New(strategy Strategy, upstreams []*Upstream) (Balancer, error) {
	switch strategy {
	case StrategyRoundRobin:
		return NewRoundRobin(upstreams)
	case StrategyWeighted:
		return NewWeightedBalancer(upstreams)
	case StrategyLeastConn:
		return NewLeastConnBalancer(upstreams)
	default:
		return NewRoundRobin(upstreams)
	}
}