// Package model holds the plain data types that mirror the etcd shard
// registry key-space described in the implementation guide (section 4.3/4.4):
//
//	/sharding/version
//	/sharding/shards/<shard-id>
//	/sharding/buckets/<bucket-id>
//	/sharding/migrations/<migration-id>
//
// Nothing in this package talks to etcd or Postgres; it is pure data so it
// can be shared by the registry client, the router, and tests without
// pulling in any external dependency.
package model

import "fmt"

// ShardStatus is the operational state of a physical shard service, stored
// on the shard definition itself (not the bucket record).
type ShardStatus string

const (
	ShardActive      ShardStatus = "ACTIVE"
	ShardMaintenance ShardStatus = "MAINTENANCE"
	ShardDraining    ShardStatus = "DRAINING"
	ShardRetired     ShardStatus = "RETIRED"
)

// Endpoint is a logical, stable service endpoint (PgBouncer/HAProxy/VIP/DNS),
// never a physical PostgreSQL node address. See guide section 4.4/5.1.
type Endpoint struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
}

func (e Endpoint) String() string {
	return fmt.Sprintf("%s:%d/%s", e.Host, e.Port, e.Database)
}

func (e Endpoint) empty() bool { return e.Host == "" }

// ShardDef is the JSON value stored under /sharding/shards/<shard-id>.
// It intentionally never contains a username, password, or TLS key -
// only a pointer (CredentialRef) to a secret manager entry (guide 4.4).
type ShardDef struct {
	ShardID       string      `json:"shard_id"`
	Status        ShardStatus `json:"status"`
	Writer        Endpoint    `json:"writer"`
	Reader        *Endpoint   `json:"reader,omitempty"`
	CredentialRef string      `json:"credential_ref"`
	Region        string      `json:"region"`
}

// MigrationState is the bucket-migration state machine from guide section
// 10.1. It controls which shard(s) serve reads and which shard(s) receive
// writes while a virtual bucket is being rebalanced.
type MigrationState string

const (
	StateActiveSource  MigrationState = "ACTIVE_SOURCE"
	StateCopying       MigrationState = "COPYING"
	StateDualWrite     MigrationState = "DUAL_WRITE"
	StateCDCCatchup    MigrationState = "CDC_CATCHUP"
	StateVerifying     MigrationState = "VERIFYING"
	StateCutover       MigrationState = "CUTOVER"
	StateDrainingSrc   MigrationState = "DRAINING_SOURCE"
	StateActiveDest    MigrationState = "ACTIVE_DEST"
)

// BucketDef is the JSON value stored under /sharding/buckets/<bucket-id>.
// Per guide 4.4, bucket records stay small: they reference a shard_id (and,
// during a migration, a dest_shard_id) rather than duplicating connection
// details that already live on the shard definition.
type BucketDef struct {
	BucketID       int            `json:"bucket_id"`
	ShardID        string         `json:"shard_id"`
	DestShardID    string         `json:"dest_shard_id,omitempty"`
	State          MigrationState `json:"state,omitempty"`
	MappingVersion int64          `json:"mapping_version,omitempty"`
}

// ReadConsistency expresses the caller's intent, per guide section 9, rather
// than letting each call site pick an arbitrary endpoint.
type ReadConsistency string

const (
	ReadStrong         ReadConsistency = "STRONG"          // must hit the primary
	ReadYourWrites     ReadConsistency = "READ_YOUR_WRITES" // primary, or a caught-up replica
	ReadEventual       ReadConsistency = "EVENTUAL"         // replica is fine
)

// ParseReadConsistency defaults unknown/empty values to STRONG, which is the
// safe choice when a caller doesn't specify one.
func ParseReadConsistency(s string) ReadConsistency {
	switch ReadConsistency(s) {
	case ReadYourWrites:
		return ReadYourWrites
	case ReadEventual:
		return ReadEventual
	default:
		return ReadStrong
	}
}
