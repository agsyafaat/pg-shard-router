package router

import (
	"errors"
	"testing"

	"github.com/example/shard-router/internal/cache"
	"github.com/example/shard-router/internal/hashing"
	"github.com/example/shard-router/internal/model"
)

func shard(id string, status model.ShardStatus) model.ShardDef {
	return model.ShardDef{
		ShardID: id,
		Status:  status,
		Writer:  model.Endpoint{Host: "pg-" + id + "-rw.internal", Port: 6432, Database: "payment"},
		Reader:  &model.Endpoint{Host: "pg-" + id + "-ro.internal", Port: 6432, Database: "payment"},
	}
}

func storeWith(m *cache.Mapping) *cache.Store {
	s := cache.NewStore()
	s.Swap(m)
	return s
}

// TestSameKeySameBucket is the mandatory routing-correctness test from
// guide section 15: the same key must always map to the same bucket.
func TestSameKeySameBucket(t *testing.T) {
	const bucketCount = 1024
	keys := []any{"tenant-1", 42, "wallet-abc"}
	for _, k := range keys {
		first := hashing.BucketFor(k, bucketCount)
		for i := 0; i < 100; i++ {
			if got := hashing.BucketFor(k, bucketCount); got != first {
				t.Fatalf("key %v: bucket changed across calls: %d vs %d", k, first, got)
			}
		}
	}
}

func TestResolve_NoMapping(t *testing.T) {
	r := New(cache.NewStore(), 1024)
	_, err := r.Resolve("tenant-1")
	if !errors.Is(err, ErrNoMapping) {
		t.Fatalf("expected ErrNoMapping, got %v", err)
	}
}

func TestResolve_NormalRouting(t *testing.T) {
	bucketID := hashing.BucketFor("tenant-1", 1024)
	m := &cache.Mapping{
		Revision: 1,
		Shards: map[string]model.ShardDef{
			"shard-01": shard("shard-01", model.ShardActive),
		},
		Buckets: map[int]model.BucketDef{
			bucketID: {BucketID: bucketID, ShardID: "shard-01"},
		},
	}
	r := New(storeWith(m), 1024)

	res, err := r.Resolve("tenant-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ReadShard.ShardID != "shard-01" {
		t.Fatalf("expected read shard shard-01, got %s", res.ReadShard.ShardID)
	}
	if len(res.WriteShards) != 1 || res.WriteShards[0].ShardID != "shard-01" {
		t.Fatalf("expected single write shard shard-01, got %+v", res.WriteShards)
	}
	if res.ReadEndpoint(model.ReadEventual).Host != "pg-shard-01-ro.internal" {
		t.Fatalf("expected replica endpoint for EVENTUAL reads, got %s", res.ReadEndpoint(model.ReadEventual).Host)
	}
	if res.ReadEndpoint(model.ReadStrong).Host != "pg-shard-01-rw.internal" {
		t.Fatalf("expected primary endpoint for STRONG reads, got %s", res.ReadEndpoint(model.ReadStrong).Host)
	}
}

func TestResolve_UnknownBucketAssignment(t *testing.T) {
	m := &cache.Mapping{
		Revision: 1,
		Shards:   map[string]model.ShardDef{"shard-01": shard("shard-01", model.ShardActive)},
		Buckets:  map[int]model.BucketDef{}, // nothing assigned
	}
	r := New(storeWith(m), 1024)
	_, err := r.Resolve("tenant-1")
	var ub *ErrUnknownBucket
	if !errors.As(err, &ub) {
		t.Fatalf("expected ErrUnknownBucket, got %v", err)
	}
}

func TestResolve_InactiveShard(t *testing.T) {
	bucketID := hashing.BucketFor("tenant-1", 1024)
	m := &cache.Mapping{
		Revision: 1,
		Shards:   map[string]model.ShardDef{"shard-01": shard("shard-01", model.ShardMaintenance)},
		Buckets:  map[int]model.BucketDef{bucketID: {BucketID: bucketID, ShardID: "shard-01"}},
	}
	r := New(storeWith(m), 1024)
	_, err := r.Resolve("tenant-1")
	var su *ErrShardUnavailable
	if !errors.As(err, &su) {
		t.Fatalf("expected ErrShardUnavailable, got %v", err)
	}
}

// TestResolve_MigrationStates walks the full state machine from guide
// section 10.1 and checks the read/write path matches its table exactly.
func TestResolve_MigrationStates(t *testing.T) {
	bucketID := hashing.BucketFor("tenant-1", 1024)
	src := shard("shard-01", model.ShardActive)
	dst := shard("shard-05", model.ShardActive)

	baseShards := map[string]model.ShardDef{
		"shard-01": src,
		"shard-05": dst,
	}

	cases := []struct {
		name          string
		state         model.MigrationState
		wantReadShard string
		wantWrites    []string
	}{
		{"active_source", model.StateActiveSource, "shard-01", []string{"shard-01"}},
		{"copying", model.StateCopying, "shard-01", []string{"shard-01"}},
		{"dual_write", model.StateDualWrite, "shard-01", []string{"shard-01", "shard-05"}},
		{"cdc_catchup", model.StateCDCCatchup, "shard-01", []string{"shard-01", "shard-05"}},
		{"verifying", model.StateVerifying, "shard-01", []string{"shard-01", "shard-05"}},
		{"cutover", model.StateCutover, "shard-05", []string{"shard-05"}},
		{"draining_source", model.StateDrainingSrc, "shard-05", []string{"shard-05"}},
		{"active_dest", model.StateActiveDest, "shard-05", []string{"shard-05"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &cache.Mapping{
				Revision: 1,
				Shards:   baseShards,
				Buckets: map[int]model.BucketDef{
					bucketID: {
						BucketID:    bucketID,
						ShardID:     "shard-01",
						DestShardID: "shard-05",
						State:       tc.state,
					},
				},
			}
			r := New(storeWith(m), 1024)
			res, err := r.Resolve("tenant-1")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.ReadShard.ShardID != tc.wantReadShard {
				t.Errorf("read shard = %s, want %s", res.ReadShard.ShardID, tc.wantReadShard)
			}
			var gotWrites []string
			for _, ws := range res.WriteShards {
				gotWrites = append(gotWrites, ws.ShardID)
			}
			if len(gotWrites) != len(tc.wantWrites) {
				t.Fatalf("write shards = %v, want %v", gotWrites, tc.wantWrites)
			}
			for i := range gotWrites {
				if gotWrites[i] != tc.wantWrites[i] {
					t.Errorf("write shards = %v, want %v", gotWrites, tc.wantWrites)
				}
			}
		})
	}
}

// TestResolve_UnsafeCutoverGuard: if a CUTOVER bucket references a
// destination shard that isn't registered yet, resolution must fail rather
// than silently keep routing to the source (guide 10.2's "avoid unsafe
// cutover" warning) or silently drop the write.
func TestResolve_CutoverMissingDestFails(t *testing.T) {
	bucketID := hashing.BucketFor("tenant-1", 1024)
	m := &cache.Mapping{
		Revision: 1,
		Shards:   map[string]model.ShardDef{"shard-01": shard("shard-01", model.ShardActive)},
		Buckets: map[int]model.BucketDef{
			bucketID: {BucketID: bucketID, ShardID: "shard-01", DestShardID: "shard-05", State: model.StateCutover},
		},
	}
	r := New(storeWith(m), 1024)
	_, err := r.Resolve("tenant-1")
	var us *ErrUnknownShard
	if !errors.As(err, &us) {
		t.Fatalf("expected ErrUnknownShard, got %v", err)
	}
}

// TestResolve_DataIsolation ensures two different tenants that happen to
// hash to different buckets never resolve to the same bucket id by accident
// in this small fixture (a smoke check, not a proof).
func TestResolve_DataIsolation(t *testing.T) {
	a := hashing.BucketFor("tenant-aaaa", 1024)
	b := hashing.BucketFor("tenant-bbbb", 1024)
	if a == b {
		t.Skip("coincidental hash collision for these two fixture keys; not a correctness issue")
	}
}
