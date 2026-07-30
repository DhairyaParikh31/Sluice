package ratelimiter

import (
	"sync"
	"testing"
	"time"
)

// TestNewBucketStartsFull verifies a new bucket starts at full capacity.
func TestNewBucketStartsFull(t *testing.T) {
	b := newBucket(10, 1)
	if b.Tokens() != 10 {
		t.Errorf("expected 10 tokens, got %.2f", b.Tokens())
	}
}

// TestAllowConsumesToken verifies each Allow() call consumes one token.
func TestAllowConsumesToken(t *testing.T) {
	b := newBucket(5, 0) // rate=0 so no refill during test

	for i := 0; i < 5; i++ {
		if !b.Allow() {
			t.Fatalf("expected Allow()=true on call %d", i+1)
		}
	}

	// Bucket should now be empty.
	if b.Allow() {
		t.Error("expected Allow()=false when bucket empty")
	}
}

// TestBucketRefillsOverTime verifies tokens are restored over time.
func TestBucketRefillsOverTime(t *testing.T) {
	// Rate = 100 tokens/sec, capacity = 10, start empty.
	b := newBucket(10, 100)
	// Drain the bucket.
	for b.Allow() {
	}

	// Wait 100ms — should refill 10 tokens (100 tokens/sec * 0.1s).
	time.Sleep(110 * time.Millisecond)

	if !b.Allow() {
		t.Error("expected bucket to refill after 100ms at 100 tokens/sec")
	}
}

// TestBucketCapCapped verifies tokens never exceed capacity.
func TestBucketCapCapped(t *testing.T) {
	b := newBucket(5, 1000) // very fast refill
	time.Sleep(50 * time.Millisecond)

	tokens := b.Tokens()
	if tokens > 5 {
		t.Errorf("tokens %.2f exceeded capacity 5", tokens)
	}
}

// TestLimiterAllowsNewClient verifies first request from new client passes.
func TestLimiterAllowsNewClient(t *testing.T) {
	l := NewLimiter(5, 1)
	if !l.Allow("192.168.1.1") {
		t.Error("expected first request from new client to be allowed")
	}
}

// TestLimiterTracksClients verifies different clients have separate buckets.
func TestLimiterTracksClients(t *testing.T) {
	l := NewLimiter(2, 0) // capacity 2, no refill

	// Drain client A.
	l.Allow("client-a")
	l.Allow("client-a")

	// Client A should be rate limited.
	if l.Allow("client-a") {
		t.Error("client-a should be rate limited after 2 requests")
	}

	// Client B should still be allowed — separate bucket.
	if !l.Allow("client-b") {
		t.Error("client-b should be allowed — independent bucket")
	}
}

// TestLimiterRateLimitsAtThreshold verifies 429 kicks in at capacity+1.
func TestLimiterRateLimitsAtThreshold(t *testing.T) {
	capacity := 10.0
	l := NewLimiter(capacity, 0) // no refill

	allowed := 0
	denied := 0

	for i := 0; i < 20; i++ {
		if l.Allow("client") {
			allowed++
		} else {
			denied++
		}
	}

	if allowed != int(capacity) {
		t.Errorf("expected %d allowed, got %d", int(capacity), allowed)
	}
	if denied != 10 {
		t.Errorf("expected 10 denied, got %d", denied)
	}
}

// TestLimiterBucketCount verifies bucket creation tracking.
func TestLimiterBucketCount(t *testing.T) {
	l := NewLimiter(10, 1)

	l.Allow("a")
	l.Allow("b")
	l.Allow("c")
	l.Allow("a") // duplicate — should not create new bucket

	if l.BucketCount() != 3 {
		t.Errorf("expected 3 buckets, got %d", l.BucketCount())
	}
}

// TestLimiterConcurrentSafety verifies no race conditions under
// concurrent access from multiple goroutines.
func TestLimiterConcurrentSafety(t *testing.T) {
	l := NewLimiter(1000, 1000)
	var wg sync.WaitGroup
	clients := []string{"a", "b", "c", "d", "e"}

	for _, client := range clients {
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				l.Allow(key)
			}(client)
		}
	}

	wg.Wait()

	if l.BucketCount() != len(clients) {
		t.Errorf("expected %d buckets, got %d", len(clients), l.BucketCount())
	}
}

// TestLimiterReset verifies Reset clears all buckets.
func TestLimiterReset(t *testing.T) {
	l := NewLimiter(10, 1)
	l.Allow("a")
	l.Allow("b")
	l.Reset()

	if l.BucketCount() != 0 {
		t.Errorf("expected 0 buckets after Reset, got %d", l.BucketCount())
	}
}

// BenchmarkAllow benchmarks the hot path: Allow() for existing client.
func BenchmarkAllow(b *testing.B) {
	l := NewLimiter(1e9, 1e9) // effectively unlimited for benchmarking
	l.Allow("client")         // pre-create bucket
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Allow("client")
	}
}

// BenchmarkAllowConcurrent benchmarks concurrent Allow() calls.
func BenchmarkAllowConcurrent(b *testing.B) {
	l := NewLimiter(1e9, 1e9)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow("client")
		}
	})
}

// BenchmarkAllowManyClients benchmarks Allow() with many unique clients.
func BenchmarkAllowManyClients(b *testing.B) {
	l := NewLimiter(1e9, 1e9)
	clients := make([]string, 10000)
	for i := range clients {
		clients[i] = "client-" + string(rune(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Allow(clients[i%len(clients)])
	}
}