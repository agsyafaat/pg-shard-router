// Package hashing implements the stable, cross-language hash required by
// guide section 6.2: it must never depend on a language runtime's built-in
// hash (which can change across restarts/versions/platforms), because a
// different bucket assignment for the same key silently scatters a tenant's
// data across shards.
//
// Algorithm (v1, frozen): SHA-256 of the UTF-8 decimal-string form of the
// key, first 8 bytes read as a big-endian uint64, mod bucketCount.
// If this algorithm ever needs to change, it must ship as an explicitly
// versioned v2 alongside a migration plan - never as a silent edit here.
package hashing

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// DefaultBucketCount matches the guide's recommended starting configuration
// (section 17.3): 1024 virtual buckets.
const DefaultBucketCount = 1024

// Algorithm identifies the hash version in use, so it can be attached to
// tracing/metrics labels and checked against golden vectors.
const Algorithm = "sha256-be64-v1"

// BucketFor returns the deterministic virtual bucket for shardKey.
// shardKey is formatted with %v, matching str(tenant_id) for the common
// case of integer or string business keys (tenant_id, merchant_id, ...).
func BucketFor(shardKey any, bucketCount int) int {
	if bucketCount <= 0 {
		bucketCount = DefaultBucketCount
	}
	raw := []byte(fmt.Sprintf("%v", shardKey))
	digest := sha256.Sum256(raw)
	n := binary.BigEndian.Uint64(digest[:8])
	return int(n % uint64(bucketCount))
}
