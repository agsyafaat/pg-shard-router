// Package registry loads the shard map from its authoritative source and
// keeps a cache.Store up to date. Guide section 4 mandates etcd as that
// authoritative source; this package's Registry interface is the seam that
// lets the rest of the service (router, API, tests) stay independent of
// that choice, and lets local development run against a plain JSON file
// instead of a real etcd cluster.
package registry

import (
	"context"

	"github.com/example/shard-router/internal/cache"
)

// Registry loads the initial shard map into store and then keeps it current
// (via watch, poll, or whatever mechanism the implementation uses) until
// ctx is canceled. Start blocks until the first successful load, then
// returns while background synchronization continues; callers should run
// it in its own goroutine only if they need Start to return immediately
// without an initial load - the etcd implementation below wants the caller
// to wait for the first load before serving traffic.
type Registry interface {
	// Start performs the initial load and then launches whatever
	// background synchronization the implementation needs. It returns once
	// the initial load has succeeded, or ctx is canceled, or the initial
	// load fails.
	Start(ctx context.Context) error
	// Store returns the cache.Store this registry keeps updated.
	Store() *cache.Store
	// Close releases any underlying client/connection.
	Close() error
}
