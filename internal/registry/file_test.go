package registry

import (
	"context"
	"testing"
)

func TestFileRegistry_Load(t *testing.T) {
	r := NewFileRegistry("../../testdata/mapping.example.json")
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	m := r.Store().Load()
	if m == nil {
		t.Fatal("expected mapping to be loaded")
	}
	if len(m.Shards) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(m.Shards))
	}
	if len(m.Buckets) != 4 {
		t.Fatalf("expected 4 buckets, got %d", len(m.Buckets))
	}
	b, ok := m.Buckets[2]
	if !ok || b.ShardID != "shard-02" {
		t.Fatalf("bucket 2 should map to shard-02, got %+v (ok=%v)", b, ok)
	}
}
