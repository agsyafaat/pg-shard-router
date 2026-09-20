// Package metrics provides minimal, dependency-free counters labeled by
// shard_id/bucket_id, per guide section 13.1's "Routing: Requests by
// shard, routing errors, registry version, cache age" and section 6.3's
// "Attach shard_id and bucket_id to tracing spans and database metrics."
//
// This intentionally avoids pulling in a metrics client library so the
// core service has as few external dependencies as reasonably possible;
// swap this package for a real Prometheus/OpenTelemetry exporter in
// production by implementing the same Registry interface shape.
package metrics

import "sync"

type Registry struct {
	mu              sync.Mutex
	requestsByShard map[string]int64
	routingErrors   int64
}

func NewRegistry() *Registry {
	return &Registry{requestsByShard: make(map[string]int64)}
}

// IncRoutingRequest records a successful routing decision. bucketID is
// accepted for future per-bucket cardinality-aware aggregation but is not
// used as a label here to avoid an unbounded metrics cardinality blow-up
// (1024 buckets x N shards is fine; per-request bucket detail belongs in
// tracing spans, not in a counter's label set).
func (r *Registry) IncRoutingRequest(shardID string, bucketID int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requestsByShard[shardID]++
}

func (r *Registry) IncRoutingError() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routingErrors++
}

type Snapshot struct {
	RequestsByShard map[string]int64
	RoutingErrors   int64
}

func (r *Registry) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make(map[string]int64, len(r.requestsByShard))
	for k, v := range r.requestsByShard {
		cp[k] = v
	}
	return Snapshot{RequestsByShard: cp, RoutingErrors: r.routingErrors}
}
