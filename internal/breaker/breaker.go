// Package breaker implements a minimal per-shard circuit breaker (guide
// section 6.3: "Apply a per-shard circuit breaker so one failing shard does
// not exhaust application worker threads.") It is intentionally simple
// (closed/open/half-open, fixed thresholds) rather than pulling in a
// third-party breaker library, since a shard outage must be contained even
// if the rest of the dependency graph is unavailable.
package breaker

import (
	"errors"
	"sync"
	"time"
)

type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Open:
		return "OPEN"
	case HalfOpen:
		return "HALF_OPEN"
	default:
		return "CLOSED"
	}
}

// ErrOpen is returned by Allow when the breaker is open and the reset
// timeout has not elapsed yet.
var ErrOpen = errors.New("breaker: circuit open")

// Breaker is safe for concurrent use.
type Breaker struct {
	mu               sync.Mutex
	state            State
	consecutiveFails int
	openedAt         time.Time

	// FailureThreshold is how many consecutive failures trip the breaker.
	FailureThreshold int
	// ResetTimeout is how long the breaker stays open before allowing a
	// single half-open probe request through.
	ResetTimeout time.Duration
}

// New returns a closed breaker with sensible defaults for a database
// backend: 5 consecutive failures trips it, and it probes again after 10s.
func New() *Breaker {
	return &Breaker{
		FailureThreshold: 5,
		ResetTimeout:     10 * time.Second,
	}
}

// Allow reports whether a call should be attempted right now. Callers must
// report the outcome via Success or Failure.
func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case Open:
		if time.Since(b.openedAt) >= b.ResetTimeout {
			b.state = HalfOpen
			return nil
		}
		return ErrOpen
	default:
		return nil
	}
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFails = 0
	b.state = Closed
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFails++
	if b.state == HalfOpen || b.consecutiveFails >= b.FailureThreshold {
		b.state = Open
		b.openedAt = time.Now()
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Registry hands out one Breaker per shard, created lazily.
type Registry struct {
	mu       sync.Mutex
	breakers map[string]*Breaker
}

func NewRegistry() *Registry {
	return &Registry{breakers: make(map[string]*Breaker)}
}

func (r *Registry) For(shardID string) *Breaker {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.breakers[shardID]
	if !ok {
		b = New()
		r.breakers[shardID] = b
	}
	return b
}

// Snapshot returns the current state of every known shard's breaker, for
// the /metrics and /v1/route debug endpoints.
func (r *Registry) Snapshot() map[string]State {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]State, len(r.breakers))
	for id, b := range r.breakers {
		out[id] = b.State()
	}
	return out
}
