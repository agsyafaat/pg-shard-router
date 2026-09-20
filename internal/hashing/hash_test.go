package hashing

import (
	"encoding/json"
	"os"
	"testing"
)

// TestDeterministic guards against the hash silently changing behavior
// between calls, processes, or refactors.
func TestDeterministic(t *testing.T) {
	keys := []any{1, 42, "tenant-abc", int64(9223372036854775807), 0}
	for _, k := range keys {
		a := BucketFor(k, DefaultBucketCount)
		b := BucketFor(k, DefaultBucketCount)
		if a != b {
			t.Fatalf("BucketFor(%v) not deterministic: %d != %d", k, a, b)
		}
		if a < 0 || a >= DefaultBucketCount {
			t.Fatalf("BucketFor(%v) = %d out of range [0,%d)", k, a, DefaultBucketCount)
		}
	}
}

// goldenVector mirrors testdata/golden_vectors.json. Every language
// implementation of this router must reproduce these exact bucket ids
// (guide section 15.1) before it is allowed to serve traffic - an
// unnoticed hash change would silently send an existing tenant's writes
// to a different physical shard.
//
// shard_key is stored as a JSON string (the exact bytes that get hashed),
// not a JSON number, so vectors are unambiguous across languages: JSON
// numbers don't guarantee round-tripping of large 64-bit integers, but the
// hashed representation is always "the key's decimal string form".
type goldenVector struct {
	ShardKey    string `json:"shard_key"`
	BucketCount int    `json:"bucket_count"`
	Bucket      int    `json:"bucket_id"`
}

func TestGoldenVectors(t *testing.T) {
	f, err := os.Open("../../testdata/golden_vectors.json")
	if err != nil {
		t.Fatalf("open golden vectors: %v", err)
	}
	defer f.Close()

	var vectors []goldenVector
	if err := json.NewDecoder(f).Decode(&vectors); err != nil {
		t.Fatalf("decode golden vectors: %v", err)
	}
	if len(vectors) == 0 {
		t.Fatal("golden vectors file is empty")
	}

	for _, v := range vectors {
		got := BucketFor(v.ShardKey, v.BucketCount)
		if got != v.Bucket {
			t.Errorf("BucketFor(%v, %d) = %d, want %d (golden vector mismatch - hash algorithm may have changed)",
				v.ShardKey, v.BucketCount, got, v.Bucket)
		}
	}
}
