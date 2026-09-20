package registry

import (
	"testing"

	"github.com/example/shard-router/internal/cache"
	"github.com/example/shard-router/internal/model"
)

func emptyMapping() *cache.Mapping {
	return &cache.Mapping{
		Shards:  make(map[string]model.ShardDef),
		Buckets: make(map[int]model.BucketDef),
	}
}

func TestApplyKV_ShardPutAndDelete(t *testing.T) {
	m := emptyMapping()
	shardJSON := []byte(`{"shard_id":"shard-01","status":"ACTIVE","writer":{"host":"pg-shard01-rw.internal","port":6432,"database":"payment"},"credential_ref":"secret/payment/postgresql","region":"jakarta"}`)

	if err := applyKV(m, "/sharding/shards/shard-01", shardJSON, false); err != nil {
		t.Fatalf("apply shard put: %v", err)
	}
	got, ok := m.Shards["shard-01"]
	if !ok {
		t.Fatal("shard-01 not present after PUT")
	}
	if got.Status != model.ShardActive || got.Writer.Host != "pg-shard01-rw.internal" {
		t.Fatalf("unexpected decoded shard: %+v", got)
	}

	if err := applyKV(m, "/sharding/shards/shard-01", nil, true); err != nil {
		t.Fatalf("apply shard delete: %v", err)
	}
	if _, ok := m.Shards["shard-01"]; ok {
		t.Fatal("shard-01 still present after DELETE")
	}
}

func TestApplyKV_BucketPutAndDelete(t *testing.T) {
	m := emptyMapping()
	bucketJSON := []byte(`{"shard_id":"shard-03"}`)

	if err := applyKV(m, "/sharding/buckets/0527", bucketJSON, false); err != nil {
		t.Fatalf("apply bucket put: %v", err)
	}
	got, ok := m.Buckets[527]
	if !ok || got.ShardID != "shard-03" {
		t.Fatalf("unexpected bucket state: %+v (ok=%v)", got, ok)
	}

	if err := applyKV(m, "/sharding/buckets/0527", nil, true); err != nil {
		t.Fatalf("apply bucket delete: %v", err)
	}
	if _, ok := m.Buckets[527]; ok {
		t.Fatal("bucket 527 still present after DELETE")
	}
}

func TestApplyKV_IgnoresUnrelatedKeys(t *testing.T) {
	m := emptyMapping()
	if err := applyKV(m, "/sharding/version", []byte(`"3"`), false); err != nil {
		t.Fatalf("unexpected error for version key: %v", err)
	}
	if err := applyKV(m, "/sharding/migrations/mig-1", []byte(`{}`), false); err != nil {
		t.Fatalf("unexpected error for migration key: %v", err)
	}
	if len(m.Shards) != 0 || len(m.Buckets) != 0 {
		t.Fatalf("expected no shard/bucket mutation from unrelated keys, got %+v", m)
	}
}

func TestApplyKV_MalformedJSONReturnsError(t *testing.T) {
	m := emptyMapping()
	if err := applyKV(m, "/sharding/shards/shard-01", []byte(`not json`), false); err == nil {
		t.Fatal("expected error for malformed shard JSON")
	}
}
