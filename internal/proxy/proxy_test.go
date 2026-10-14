package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DhairyaParikh31/sluice/internal/balancer"
	"github.com/DhairyaParikh31/sluice/internal/ratelimiter"
)

// makeUpstream creates a test upstream pointing to the given server.
func makeUpstream(t *testing.T, server *httptest.Server) *balancer.Upstream {
	t.Helper()
	u, err := balancer.NewUpstream(server.URL, 1)
	if err != nil {
		t.Fatalf("NewUpstream: %v", err)
	}
	return u
}

// TestProxyForwardsRequest verifies that requests are forwarded
// to the upstream and the response is returned to the client.
func TestProxyForwardsRequest(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello from backend"))
	}))
	defer backend.Close()

	ups := makeUpstream(t, backend)
	bal, _ := balancer.NewRoundRobin([]*balancer.Upstream{ups})

	p := New(Config{Balancer: bal})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rw := httptest.NewRecorder()

	p.ServeHTTP(rw, req)

	if rw.Code != http.StatusOK {
		t.Errorf("expected 200 got %d", rw.Code)
	}
	body, _ := io.ReadAll(rw.Body)
	if string(body) != "hello from backend" {
		t.Errorf("unexpected body: %q", body)
	}
}

// TestProxyRateLimiting verifies that rate-limited clients receive 429.
func TestProxyRateLimiting(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	ups := makeUpstream(t, backend)
	bal, _ := balancer.NewRoundRobin([]*balancer.Upstream{ups})

	// Capacity 2, no refill — only 2 requests allowed.
	limiter := ratelimiter.NewLimiter(2, 0)

	p := New(Config{Balancer: bal, Limiter: limiter})

	allowed := 0
	denied := 0

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "10.0.0.1:9999"
		rw := httptest.NewRecorder()
		p.ServeHTTP(rw, req)

		if rw.Code == http.StatusTooManyRequests {
			denied++
		} else {
			allowed++
		}
	}

	if allowed != 2 {
		t.Errorf("expected 2 allowed, got %d", allowed)
	}
	if denied != 3 {
		t.Errorf("expected 3 denied, got %d", denied)
	}
}

// TestProxyNoUpstreams verifies that 503 is returned when no upstreams exist.
func TestProxyNoUpstreams(t *testing.T) {
	// Create a balancer then manually break it by using an empty one.
	ups, _ := balancer.NewUpstream("http://localhost:1", 1)
	bal, _ := balancer.NewRoundRobin([]*balancer.Upstream{ups})

	p := New(Config{Balancer: bal})

	// The upstream doesn't exist — proxy should return 502.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "127.0.0.1:1234"
	rw := httptest.NewRecorder()

	p.ServeHTTP(rw, req)

	if rw.Code != http.StatusBadGateway {
		t.Errorf("expected 502 for unreachable upstream, got %d", rw.Code)
	}
}

// TestProxyXForwardedFor verifies X-Forwarded-For is set correctly.
func TestProxyXForwardedFor(t *testing.T) {
	var receivedXFF string

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedXFF = r.Header.Get("X-Forwarded-For")
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	ups := makeUpstream(t, backend)
	bal, _ := balancer.NewRoundRobin([]*balancer.Upstream{ups})

	p := New(Config{Balancer: bal})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "1.2.3.4:5678"
	rw := httptest.NewRecorder()

	p.ServeHTTP(rw, req)

	if receivedXFF == "" {
		t.Error("expected X-Forwarded-For to be set")
	}
	if receivedXFF != "1.2.3.4" {
		t.Errorf("expected X-Forwarded-For=1.2.3.4, got %q", receivedXFF)
	}
}

// TestProxyDifferentClients verifies different client IPs
// are rate-limited independently.
func TestProxyDifferentClients(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	ups := makeUpstream(t, backend)
	bal, _ := balancer.NewRoundRobin([]*balancer.Upstream{ups})
	limiter := ratelimiter.NewLimiter(1, 0) // 1 request per client

	p := New(Config{Balancer: bal, Limiter: limiter})

	// Client A — first request allowed, second denied.
	req1 := httptest.NewRequest(http.MethodGet, "/", nil)
	req1.RemoteAddr = "10.0.0.1:1111"
	rw1 := httptest.NewRecorder()
	p.ServeHTTP(rw1, req1)
	if rw1.Code != http.StatusOK {
		t.Errorf("client A first request: expected 200 got %d", rw1.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.RemoteAddr = "10.0.0.1:1111"
	rw2 := httptest.NewRecorder()
	p.ServeHTTP(rw2, req2)
	if rw2.Code != http.StatusTooManyRequests {
		t.Errorf("client A second request: expected 429 got %d", rw2.Code)
	}

	// Client B — first request should be allowed (separate bucket).
	req3 := httptest.NewRequest(http.MethodGet, "/", nil)
	req3.RemoteAddr = "10.0.0.2:2222"
	rw3 := httptest.NewRecorder()
	p.ServeHTTP(rw3, req3)
	if rw3.Code != http.StatusOK {
		t.Errorf("client B first request: expected 200 got %d", rw3.Code)
	}
}

// TestExtractClientIP verifies IP extraction from various request formats.
func TestExtractClientIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		xff        string
		wantIP     string
	}{
		{
			name:       "plain remote addr",
			remoteAddr: "192.168.1.1:1234",
			wantIP:     "192.168.1.1",
		},
		{
			name:       "X-Forwarded-For single",
			remoteAddr: "10.0.0.1:9999",
			xff:        "203.0.113.5",
			wantIP:     "203.0.113.5",
		},
		{
			name:       "X-Forwarded-For list",
			remoteAddr: "10.0.0.1:9999",
			xff:        "203.0.113.5, 10.0.0.1",
			wantIP:     "203.0.113.5",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tc.remoteAddr
			if tc.xff != "" {
				req.Header.Set("X-Forwarded-For", tc.xff)
			}
			got := extractClientIP(req)
			if got != tc.wantIP {
				t.Errorf("extractClientIP: want %q got %q", tc.wantIP, got)
			}
		})
	}
}

// BenchmarkProxy benchmarks end-to-end proxy throughput.
func BenchmarkProxy(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	ups, _ := balancer.NewUpstream(backend.URL, 1)
	bal, _ := balancer.NewRoundRobin([]*balancer.Upstream{ups})
	p := New(Config{Balancer: bal})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = "127.0.0.1:1234"
			rw := httptest.NewRecorder()
			p.ServeHTTP(rw, req)
		}
	})
}