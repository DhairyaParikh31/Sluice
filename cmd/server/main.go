// Command server starts the Sluice HTTP reverse proxy.
//
// Usage:
//
//	sluice --port 8080 \
//	       --upstreams http://localhost:9001,http://localhost:9002 \
//	       --strategy round-robin \
//	       --rate 100 \
//	       --capacity 200
//
// Flags:
//
//	--port       Port to listen on (default: 8080)
//	--upstreams  Comma-separated list of upstream URLs
//	--strategy   Load balancing: round-robin | weighted | least-conn (default: round-robin)
//	--rate       Token refill rate per second per client (default: 100)
//	--capacity   Token bucket capacity per client (default: 200)
//	--weights    Comma-separated weights matching --upstreams (weighted strategy only)
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/DhairyaParikh31/sluice/internal/balancer"
	"github.com/DhairyaParikh31/sluice/internal/metrics"
	"github.com/DhairyaParikh31/sluice/internal/proxy"
	"github.com/DhairyaParikh31/sluice/internal/ratelimiter"
)

func main() {
	port := flag.String("port", "8080", "Port to listen on")
	upstreamsFlag := flag.String("upstreams", "", "Comma-separated upstream URLs (required)")
	strategy := flag.String("strategy", "round-robin", "Load balancing strategy: round-robin | weighted | least-conn")
	rate := flag.Float64("rate", 100, "Token bucket refill rate per second per client")
	capacity := flag.Float64("capacity", 200, "Token bucket capacity per client")
	weightsFlag := flag.String("weights", "", "Comma-separated weights for weighted strategy")
	flag.Parse()

	if *upstreamsFlag == "" {
		fmt.Fprintln(os.Stderr, "error: --upstreams is required")
		flag.Usage()
		os.Exit(1)
	}

	// Parse upstream URLs.
	rawURLs := strings.Split(*upstreamsFlag, ",")
	weightStrs := strings.Split(*weightsFlag, ",")

	upstreams := make([]*balancer.Upstream, 0, len(rawURLs))
	for i, rawURL := range rawURLs {
		rawURL = strings.TrimSpace(rawURL)
		weight := 1
		if i < len(weightStrs) && weightStrs[i] != "" {
			if w, err := strconv.Atoi(strings.TrimSpace(weightStrs[i])); err == nil {
				weight = w
			}
		}
		u, err := balancer.NewUpstream(rawURL, weight)
		if err != nil {
			log.Fatalf("invalid upstream %q: %v", rawURL, err)
		}
		upstreams = append(upstreams, u)
	}

	// Create load balancer.
	bal, err := balancer.New(balancer.Strategy(*strategy), upstreams)
	if err != nil {
		log.Fatalf("create balancer: %v", err)
	}

	// Create rate limiter.
	limiter := ratelimiter.NewLimiter(*capacity, *rate)

	// Create metrics.
	m := metrics.New()

	// Create proxy.
	p := proxy.New(proxy.Config{
		Balancer: bal,
		Limiter:  limiter,
		Metrics:  m,
	})

	// Register handlers.
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/", p)

	addr := ":" + *port
	server := &http.Server{Addr: addr, Handler: mux}

	// Print startup info.
	fmt.Printf("Sluice reverse proxy\n")
	fmt.Printf("  Listening:  %s\n", addr)
	fmt.Printf("  Strategy:   %s\n", *strategy)
	fmt.Printf("  Upstreams:  %d\n", len(upstreams))
	for _, u := range upstreams {
		fmt.Printf("    → %s (weight=%d)\n", u.URL.String(), u.Weight)
	}
	fmt.Printf("  Rate limit: %.0f req/s (burst: %.0f)\n", *rate, *capacity)
	fmt.Printf("  Metrics:    http://localhost%s/metrics\n", addr)

	// Graceful shutdown.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		server.Close()
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server: %v", err)
	}

	fmt.Println("Sluice stopped.")
}