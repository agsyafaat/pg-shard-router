// Package cache holds the application's local in-memory copy of the shard
// map (guide section 4.5): "each routing decision is subsequently a
// local-memory lookup." etcd (or any other Registry implementation) owns
// the authoritative data; this package only stores whatever the registry
// last successfully loaded, as an immutable snapshot swapped atomically so
// that route resolution never blocks on a lock and is safe to read
// concurrently from every request goroutine.
package cache

import (
	"sync/atomic"

	"github.com/example/shard-router/internal/model"
)

// Mapping is one immutable, versioned snapshot of the full shard map.
// A new Mapping is built and swapped in whenever the registry observes a
// change; the old one is simply garbage collected once no request still
// holds a reference to it.
type Mapping struct {
	// Revision is the etcd revision (or, for other registries, an
	// equivalent monotonic counter) this snapshot was built from. Used for
	// reconciliation and staleness checks (guide 4.5, 4.6).
	Revision int64
	Shards   map[string]model.ShardDef
	Buckets  map[int]model.BucketDef
}

// Clone returns a deep-enough copy suitable for building the next snapshot
// via copy-on-write, so registry watch handlers never mutate a Mapping that
// a request might currently be reading.
func (m *Mapping) Clone() *Mapping {
	out := &Mapping{
		Revision: m.Revision,
		Shards:   make(map[string]model.ShardDef, len(m.Shards)),
		Buckets:  make(map[int]model.BucketDef, len(m.Buckets)),
	}
	for k, v := range m.Shards {
		out.Shards[k] = v
	}
	for k, v := range m.Buckets {
		out.Buckets[k] = v
	}
	return out
}

// Store is the atomically-swapped holder for the current Mapping.
type Store struct {
	ptr atomic.Pointer[Mapping]
}

func NewStore() *Store {
	return &Store{}
}

// Load returns the current snapshot, or nil if the registry has not
// completed its first successful load yet (callers must handle this: the
// guide requires ordinary traffic to keep flowing on the last-known-good
// map, never to block waiting on etcd).
func (s *Store) Load() *Mapping {
	return s.ptr.Load()
}

// Swap installs a new snapshot as the current one.
func (s *Store) Swap(m *Mapping) {
	s.ptr.Store(m)
}

// Ready reports whether an initial mapping has been loaded.
func (s *Store) Ready() bool {
	return s.ptr.Load() != nil
}
