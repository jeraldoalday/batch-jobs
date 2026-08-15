package api

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// RateLimit configures per-client-IP request throttling using a token
// bucket. The zero value disables rate limiting.
type RateLimit struct {
	// RPS is the steady-state number of requests per second a single
	// client IP may make.
	RPS float64

	// Burst is the bucket capacity: the number of requests a client may
	// make back-to-back before RPS-paced throttling kicks in. Must be >= 1
	// for the limiter to allow any requests.
	Burst int
}

// enabled reports whether the configuration describes an active limit.
func (r RateLimit) enabled() bool {
	return r.RPS > 0 && r.Burst > 0
}

// bucket is one client IP's token bucket. tokens is fractional so RPS
// values below 1 (e.g. 0.5 req/s) still refill correctly between requests.
type bucket struct {
	tokens   float64
	lastFill time.Time
}

// limiter is a per-key token bucket rate limiter, safe for concurrent use.
// Buckets are created lazily on first use and never evicted; that's fine
// for the modest, long-lived client populations this API expects, but
// would need an eviction policy for a public-facing deployment with many
// distinct IPs.
type limiter struct {
	cfg RateLimit

	mu      sync.Mutex
	buckets map[string]*bucket
}

func newLimiter(cfg RateLimit) *limiter {
	return &limiter{cfg: cfg, buckets: make(map[string]*bucket)}
}

// allow reports whether a request from key may proceed, consuming a token
// if so.
func (l *limiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(l.cfg.Burst), lastFill: now}
		l.buckets[key] = b
	} else {
		elapsed := now.Sub(b.lastFill).Seconds()
		b.tokens += elapsed * l.cfg.RPS
		if b.tokens > float64(l.cfg.Burst) {
			b.tokens = float64(l.cfg.Burst)
		}
		b.lastFill = now
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// withRateLimit throttles requests per client IP according to s.RateLimit.
// It is a no-op when RateLimit is unset.
func (s *Server) withRateLimit(next http.Handler) http.Handler {
	if !s.RateLimit.enabled() {
		return next
	}
	lim := newLimiter(s.RateLimit)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !lim.allow(clientIP(r)) {
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientIP extracts the request's source IP, falling back to the raw
// RemoteAddr if it isn't in host:port form.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
