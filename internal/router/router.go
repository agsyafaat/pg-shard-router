// Package router implements the ShardRouter contract from guide section 6.1:
//
//	interface ShardRouter {
//	    Shard resolve(Object shardKey);
//	    Connection getWriteConnection(Object shardKey);
//	    Connection getReadConnection(Object shardKey, ReadConsistency consistency);
//	}
//
// This package only decides *which* logical shard/endpoint a request should
// use; it deliberately knows nothing about etcd or Postgres. That keeps the
// routing decision - the part with the trickiest correctness requirements
// (golden-vector hashing, migration state handling) - fully unit-testable
// with a fake mapping and zero network dependencies.
package router

import (
	"errors"
	"fmt"

	"github.com/example/shard-router/internal/cache"
	"github.com/example/shard-router/internal/hashing"
	"github.com/example/shard-router/internal/model"
)

// ErrNoMapping means the registry has not completed its first load yet.
var ErrNoMapping = errors.New("shard router: no shard mapping loaded yet")

// ErrShardUnavailable means the resolved shard is not in ACTIVE state
// (guide 16.1's ShardUnavailable exception).
type ErrShardUnavailable struct{ ShardID string }

func (e *ErrShardUnavailable) Error() string {
	return fmt.Sprintf("shard router: shard %q is not ACTIVE", e.ShardID)
}

// ErrUnknownBucket/ErrUnknownShard indicate a corrupt or incomplete mapping -
// a bucket with no assignment, or a bucket pointing at a shard_id that
// doesn't exist in the registry. These should never happen in a healthy
// system; the caller should treat them as a routing-layer bug or a
// mid-flight registry inconsistency and fail the request loudly rather than
// guess a shard.
type ErrUnknownBucket struct{ BucketID int }

func (e *ErrUnknownBucket) Error() string {
	return fmt.Sprintf("shard router: bucket %d has no shard assignment", e.BucketID)
}

type ErrUnknownShard struct {
	ShardID  string
	BucketID int
}

func (e *ErrUnknownShard) Error() string {
	return fmt.Sprintf("shard router: bucket %d references unknown shard %q", e.BucketID, e.ShardID)
}

// Resolved is the outcome of routing a single shard key. It carries enough
// information for the connection layer to pick the right endpoint for any
// read-consistency mode, and for observability code to attach shard_id /
// bucket_id labels (guide 6.3, 16.3).
type Resolved struct {
	BucketID int
	// ReadShard is the shard that should serve reads right now. During a
	// migration this is still the source shard until CUTOVER (guide 10.1's
	// state table), so in-flight reads keep working throughout the move.
	ReadShard model.ShardDef
	// WriteShards is the set of shards that must receive the write. It has
	// two entries during DUAL_WRITE/VERIFYING (source stays authoritative,
	// destination is kept current), and one entry otherwise.
	WriteShards []model.ShardDef
	// MigrationState is empty for a bucket that isn't being rebalanced.
	MigrationState model.MigrationState
}

// PrimaryWriteShard is the authoritative shard for the write - the one
// whose failure should fail the whole operation. Callers doing dual-write
// should still attempt WriteShards[1:] best-effort/async per guide 10.1.
func (r Resolved) PrimaryWriteShard() model.ShardDef {
	return r.WriteShards[0]
}

// Endpoint picks the right logical endpoint (writer vs reader) for the
// given consistency intent, per guide section 9's table.
func (r Resolved) ReadEndpoint(consistency model.ReadConsistency) model.Endpoint {
	switch consistency {
	case model.ReadEventual:
		if r.ReadShard.Reader != nil {
			return *r.ReadShard.Reader
		}
		// No replica endpoint configured - fall back to the writer rather
		// than fail a read that only wants eventual consistency.
		return r.ReadShard.Writer
	default: // STRONG, READ_YOUR_WRITES
		return r.ReadShard.Writer
	}
}

// Router resolves shard keys against whatever mapping is currently in the
// Store. It holds no mutable state of its own beyond a reference to the
// store and the configured bucket count, so it is safe to share across
// goroutines and cheap to construct in tests.
type Router struct {
	store       *cache.Store
	bucketCount int
}

// New builds a Router over store, using bucketCount virtual buckets
// (guide's recommended default is hashing.DefaultBucketCount = 1024).
func New(store *cache.Store, bucketCount int) *Router {
	if bucketCount <= 0 {
		bucketCount = hashing.DefaultBucketCount
	}
	return &Router{store: store, bucketCount: bucketCount}
}

// BucketCount returns the configured virtual bucket count.
func (r *Router) BucketCount() int { return r.bucketCount }

// Resolve maps shardKey to its virtual bucket and then to the shard(s) that
// should serve reads and writes right now, taking any in-flight bucket
// migration into account (guide section 10.1).
func (r *Router) Resolve(shardKey any) (Resolved, error) {
	m := r.store.Load()
	if m == nil {
		return Resolved{}, ErrNoMapping
	}

	bucketID := hashing.BucketFor(shardKey, r.bucketCount)

	bucket, ok := m.Buckets[bucketID]
	if !ok {
		return Resolved{}, &ErrUnknownBucket{BucketID: bucketID}
	}

	source, ok := m.Shards[bucket.ShardID]
	if !ok {
		return Resolved{}, &ErrUnknownShard{ShardID: bucket.ShardID, BucketID: bucketID}
	}

	res := Resolved{
		BucketID:       bucketID,
		ReadShard:      source,
		WriteShards:    []model.ShardDef{source},
		MigrationState: bucket.State,
	}

	// Migration-state-aware routing. This directly encodes the read/write
	// path table in guide section 10.1:
	//
	//   ACTIVE_SOURCE, COPYING          -> read source,  write source
	//   DUAL_WRITE, CDC_CATCHUP,
	//   VERIFYING                       -> read source,  write source+dest
	//   CUTOVER, DRAINING_SOURCE,
	//   ACTIVE_DEST                     -> read dest,    write dest
	switch bucket.State {
	case model.StateDualWrite, model.StateCDCCatchup, model.StateVerifying:
		if dest, ok := m.Shards[bucket.DestShardID]; ok {
			res.WriteShards = append(res.WriteShards, dest)
		}
		// else: destination not registered yet - degrade to source-only
		// write rather than fail the request; VERIFYING should catch this
		// as a migration defect before CUTOVER is published.

	case model.StateCutover, model.StateDrainingSrc, model.StateActiveDest:
		if dest, ok := m.Shards[bucket.DestShardID]; ok {
			res.ReadShard = dest
			res.WriteShards = []model.ShardDef{dest}
		} else {
			return Resolved{}, &ErrUnknownShard{ShardID: bucket.DestShardID, BucketID: bucketID}
		}
	}

	// Refuse to route to a shard that the control plane has marked
	// non-ACTIVE (guide 16.1's ShardUnavailable), except that a
	// MAINTENANCE/DRAINING source during an active migration is expected
	// and handled by the state machine above, not treated as an outage.
	for _, ws := range res.WriteShards {
		if ws.Status != model.ShardActive {
			return Resolved{}, &ErrShardUnavailable{ShardID: ws.ShardID}
		}
	}
	if res.ReadShard.Status != model.ShardActive {
		return Resolved{}, &ErrShardUnavailable{ShardID: res.ReadShard.ShardID}
	}

	return res, nil
}
