package balancer

import (
	"sync"
	"testing"
)

// helper: create upstreams from raw URLs with equal weight.
func makeUpstreams(t *testing.T, urls ...string) []*Upstream {
	t.Helper()
	ups := make([]*Upstream, len(urls))
	for i, u := range urls {
		up, err := NewUpstream(u, 1)
		if err != nil {
			t.Fatalf("NewUpstream(%q): %v", u, err)
		}
		ups[i] = up
	}
	return ups
}

// ── Round-Robin tests ──────────────────────────────────────────────────

func TestRoundRobinDistribution(t *testing.T) {
	ups := makeUpstreams(t,
		"http://a.local",
		"http://b.local",
		"http://c.local",
	)
	rr, err := NewRoundRobin(ups)
	if err != nil {
		t.Fatalf("NewRoundRobin: %v", err)
	}

	counts := make(map[string]int)
	for i := 0; i < 300; i++ {
		u, err := rr.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		counts[u.URL.Host]++
	}

	// Each upstream should get exactly 100 requests.
	for _, u := range ups {
		got := counts[u.URL.Host]
		if got != 100 {
			t.Errorf("upstream %s: expected 100 requests got %d", u.URL.Host, got)
		}
	}
}

func TestRoundRobinCyclesCorrectly(t *testing.T) {
	ups := makeUpstreams(t, "http://a.local", "http://b.local")
	rr, _ := NewRoundRobin(ups)

	first, _ := rr.Next()
	second, _ := rr.Next()
	third, _ := rr.Next()

	if first.URL.Host != "a.local" {
		t.Errorf("first: expected a.local got %s", first.URL.Host)
	}
	if second.URL.Host != "b.local" {
		t.Errorf("second: expected b.local got %s", second.URL.Host)
	}
	if third.URL.Host != "a.local" {
		t.Errorf("third (cycle): expected a.local got %s", third.URL.Host)
	}
}

func TestRoundRobinNoUpstreams(t *testing.T) {
	_, err := NewRoundRobin(nil)
	if err != ErrNoUpstreams {
		t.Errorf("expected ErrNoUpstreams, got %v", err)
	}
}

func TestRoundRobinConcurrentSafety(t *testing.T) {
	ups := makeUpstreams(t, "http://a.local", "http://b.local", "http://c.local")
	rr, _ := NewRoundRobin(ups)

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rr.Next()
		}()
	}
	wg.Wait()
}

// ── Weighted tests ─────────────────────────────────────────────────────

func TestWeightedDistribution(t *testing.T) {
	a, _ := NewUpstream("http://a.local", 3)
	b, _ := NewUpstream("http://b.local", 1)

	wb, err := NewWeightedBalancer([]*Upstream{a, b})
	if err != nil {
		t.Fatalf("NewWeightedBalancer: %v", err)
	}

	counts := make(map[string]int)
	total := 400
	for i := 0; i < total; i++ {
		u, _ := wb.Next()
		counts[u.URL.Host]++
	}

	// a should get ~75%, b should get ~25%.
	aCount := counts["a.local"]
	bCount := counts["b.local"]

	if aCount != 300 {
		t.Errorf("a.local: expected 300 (75%%), got %d", aCount)
	}
	if bCount != 100 {
		t.Errorf("b.local: expected 100 (25%%), got %d", bCount)
	}
}

func TestWeightedNoUpstreams(t *testing.T) {
	_, err := NewWeightedBalancer(nil)
	if err != ErrNoUpstreams {
		t.Errorf("expected ErrNoUpstreams, got %v", err)
	}
}

func TestWeightedSingleUpstream(t *testing.T) {
	a, _ := NewUpstream("http://only.local", 5)
	wb, _ := NewWeightedBalancer([]*Upstream{a})

	for i := 0; i < 10; i++ {
		u, err := wb.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if u.URL.Host != "only.local" {
			t.Errorf("expected only.local got %s", u.URL.Host)
		}
	}
}

// ── Least-Connections tests ────────────────────────────────────────────

func TestLeastConnPicksLeastBusy(t *testing.T) {
	ups := makeUpstreams(t, "http://a.local", "http://b.local", "http://c.local")
	lc, err := NewLeastConnBalancer(ups)
	if err != nil {
		t.Fatalf("NewLeastConnBalancer: %v", err)
	}

	// Manually give a.local 5 active connections.
	ups[0].ActiveConns = 5
	// b.local gets 2.
	ups[1].ActiveConns = 2
	// c.local gets 0 — should be picked.

	// Reset counter first since Next() will increment it.
	ups[0].ActiveConns = 5
	ups[1].ActiveConns = 2
	ups[2].ActiveConns = 0

	// Override: manually set without going through Next().
	// Use Next() now — it should pick c.local (0 conns).
	// But Next() increments, so we need to account for that.
	// Reset all to known values.
	ups[0].ActiveConns = 10
	ups[1].ActiveConns = 5
	ups[2].ActiveConns = 1

	u, err := lc.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if u.URL.Host != "c.local" {
		t.Errorf("expected c.local (least busy), got %s", u.URL.Host)
	}
}

func TestLeastConnDoneDecrementsCount(t *testing.T) {
	ups := makeUpstreams(t, "http://a.local")
	lc, _ := NewLeastConnBalancer(ups)

	u, _ := lc.Next()
	if ups[0].ActiveConns != 1 {
		t.Errorf("expected 1 active conn after Next, got %d", ups[0].ActiveConns)
	}

	lc.Done(u)
	if ups[0].ActiveConns != 0 {
		t.Errorf("expected 0 active conns after Done, got %d", ups[0].ActiveConns)
	}
}

func TestLeastConnNoUpstreams(t *testing.T) {
	_, err := NewLeastConnBalancer(nil)
	if err != ErrNoUpstreams {
		t.Errorf("expected ErrNoUpstreams, got %v", err)
	}
}

func TestLeastConnConcurrentSafety(t *testing.T) {
	ups := makeUpstreams(t, "http://a.local", "http://b.local")
	lc, _ := NewLeastConnBalancer(ups)

	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			u, err := lc.Next()
			if err == nil {
				lc.Done(u)
			}
		}()
	}
	wg.Wait()

	// After all goroutines complete, active conns should be back to 0.
	for _, u := range ups {
		if u.ActiveConns != 0 {
			t.Errorf("%s: expected 0 active conns after all Done(), got %d",
				u.URL.Host, u.ActiveConns)
		}
	}
}

// ── Factory test ───────────────────────────────────────────────────────

func TestFactoryCreatesCorrectStrategy(t *testing.T) {
	ups := makeUpstreams(t, "http://a.local", "http://b.local")

	tests := []struct {
		strategy Strategy
	}{
		{StrategyRoundRobin},
		{StrategyWeighted},
		{StrategyLeastConn},
	}

	for _, tc := range tests {
		b, err := New(tc.strategy, ups)
		if err != nil {
			t.Errorf("New(%s): %v", tc.strategy, err)
			continue
		}
		if _, err := b.Next(); err != nil {
			t.Errorf("Next() after New(%s): %v", tc.strategy, err)
		}
	}
}

// ── Benchmarks ─────────────────────────────────────────────────────────

func BenchmarkRoundRobinNext(b *testing.B) {
	ups := makeUpstreams(b.(interface{ Helper() }), "http://a.local", "http://b.local", "http://c.local")
	rr, _ := NewRoundRobin(ups)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rr.Next()
	}
}

func BenchmarkLeastConnNext(b *testing.B) {
	ups := makeUpstreams(b.(interface{ Helper() }), "http://a.local", "http://b.local", "http://c.local")
	lc, _ := NewLeastConnBalancer(ups)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u, _ := lc.Next()
		lc.Done(u)
	}
}