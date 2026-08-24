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

// minSweepInterval keeps eviction from running on nearly every request
// under a fast limit, where a bucket refills in milliseconds. Sweeping is
// O(clients), so it is worth doing rarely even though each sweep is cheap.
const minSweepInterval = time.Minute

// idleTTL is how long a bucket must go untouched before evicting it is
// indistinguishable from keeping it: after Burst/RPS seconds it has
// refilled to capacity, which is exactly the state the next request would
// create it in anyway. Evicting sooner would hand a throttled client a
// full bucket, so the TTL is derived from the limit rather than picked.
func (r RateLimit) idleTTL() time.Duration {
	return time.Duration(float64(r.Burst) / r.RPS * float64(time.Second))
}

// bucket is one client IP's token bucket. tokens is fractional so RPS
// values below 1 (e.g. 0.5 req/s) still refill correctly between requests.
type bucket struct {
	tokens   float64
	lastFill time.Time
}

// limiter is a per-key token bucket rate limiter, safe for concurrent use.
// Buckets are created lazily on first use and dropped once they have sat
// idle long enough to have refilled, so a server facing many distinct
// client IPs -- a public endpoint, or anything behind a NAT range or a
// crawler -- does not accumulate one live bucket per address it has ever
// seen for the lifetime of the process.
type limiter struct {
	cfg           RateLimit
	idleTTL       time.Duration
	sweepInterval time.Duration

	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time
}

func newLimiter(cfg RateLimit) *limiter {
	idleTTL := cfg.idleTTL()
	sweepInterval := idleTTL
	if sweepInterval < minSweepInterval {
		sweepInterval = minSweepInterval
	}

	return &limiter{
		cfg:           cfg,
		idleTTL:       idleTTL,
		sweepInterval: sweepInterval,
		buckets:       make(map[string]*bucket),
		lastSweep:     time.Now(),
	}
}

// evictIdleLocked drops every bucket that has gone idleTTL without a
// request. Such a bucket is necessarily full, so the next request from
// that client creates an identical one: eviction reclaims memory without
// changing a single allow/deny decision.
func (l *limiter) evictIdleLocked(now time.Time) {
	for key, b := range l.buckets {
		if now.Sub(b.lastFill) >= l.idleTTL {
			delete(l.buckets, key)
		}
	}
	l.lastSweep = now
}

// allow reports whether a request from key may proceed, consuming a token
// if so. It also sweeps out idle buckets on its own schedule, so the
// limiter needs no goroutine of its own to keep its memory bounded.
func (l *limiter) allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if now.Sub(l.lastSweep) >= l.sweepInterval {
		l.evictIdleLocked(now)
	}

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
