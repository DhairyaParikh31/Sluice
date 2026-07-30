// Package ratelimiter implements a per-client token bucket rate limiter.
//
// The token bucket algorithm controls the rate at which requests are
// allowed through. Each client gets a bucket that:
//   - Holds up to Capacity tokens
//   - Refills at Rate tokens per second (continuously, not in batches)
//   - Consumes 1 token per request
//   - Returns HTTP 429 when the bucket is empty
//
// Why token bucket over fixed window or sliding window?
//
//   - Fixed window: allows 2x burst at window boundaries (known flaw)
//   - Sliding window: accurate but memory-intensive at scale
//   - Token bucket: allows controlled bursting up to Capacity,
//     smooth average rate enforcement, O(1) per request, minimal memory
//
// Token Bucket Mathematics:
//
//	tokens_now = min(capacity, tokens_last + rate x elapsed_seconds)
//	allowed    = tokens_now >= 1.0
//	if allowed: tokens_now -= 1.0
package ratelimiter

import (
	"sync"
	"time"
)

// Bucket is a single token bucket for one client.
type Bucket struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64
	lastTime time.Time
}

func newBucket(capacity, rate float64) *Bucket {
	return &Bucket{
		tokens:   capacity,
		capacity: capacity,
		rate:     rate,
		lastTime: time.Now(),
	}
}

// Allow attempts to consume one token from the bucket.
// Returns true if allowed, false if rate-limited (bucket empty).
func (b *Bucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.lastTime).Seconds()
	b.lastTime = now

	// Refill tokens proportional to elapsed time, capped at capacity.
	b.tokens = minF(b.capacity, b.tokens+b.rate*elapsed)

	if b.tokens < 1.0 {
		return false
	}

	b.tokens -= 1.0
	return true
}

// Tokens returns the current token count.
func (b *Bucket) Tokens() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens
}

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// Limiter manages per-client token buckets identified by string key.
type Limiter struct {
	mu       sync.RWMutex
	buckets  map[string]*Bucket
	capacity float64
	rate     float64
}

// NewLimiter creates a Limiter where each client bucket has the given
// capacity (burst limit) and refill rate (sustained requests/sec).
//
// Example: NewLimiter(10, 5) allows bursts of 10, sustaining 5 req/sec.
func NewLimiter(capacity, rate float64) *Limiter {
	return &Limiter{
		buckets:  make(map[string]*Bucket),
		capacity: capacity,
		rate:     rate,
	}
}

// Allow returns true if the client identified by key is allowed,
// false if they should receive a 429 Too Many Requests response.
func (l *Limiter) Allow(key string) bool {
	// Fast path: bucket exists — read lock only.
	l.mu.RLock()
	bucket, exists := l.buckets[key]
	l.mu.RUnlock()

	if exists {
		return bucket.Allow()
	}

	// Slow path: create new bucket — write lock.
	l.mu.Lock()
	// Double-check after acquiring write lock.
	bucket, exists = l.buckets[key]
	if !exists {
		bucket = newBucket(l.capacity, l.rate)
		l.buckets[key] = bucket
	}
	l.mu.Unlock()

	return bucket.Allow()
}

// BucketCount returns the number of tracked client buckets.
func (l *Limiter) BucketCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.buckets)
}

// Reset removes all buckets — used in tests.
func (l *Limiter) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buckets = make(map[string]*Bucket)
}