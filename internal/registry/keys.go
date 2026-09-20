package registry

import (
	"strconv"
	"strings"
)

// Key-space layout, exactly as specified in guide section 4.3:
//
//	/sharding/version
//	/sharding/shards/<shard-id>
//	/sharding/buckets/<bucket-id>
//	/sharding/migrations/<migration-id>
//	/sharding/maintenance/<shard-id>
const (
	prefix          = "/sharding/"
	versionKey      = prefix + "version"
	shardsPrefix    = prefix + "shards/"
	bucketsPrefix   = prefix + "buckets/"
	migrationPrefix = prefix + "migrations/"
)

// shardIDFromKey extracts "shard-01" from "/sharding/shards/shard-01".
func shardIDFromKey(key string) (string, bool) {
	if !strings.HasPrefix(key, shardsPrefix) {
		return "", false
	}
	return strings.TrimPrefix(key, shardsPrefix), true
}

// bucketIDFromKey extracts 527 from "/sharding/buckets/0527".
func bucketIDFromKey(key string) (int, bool) {
	if !strings.HasPrefix(key, bucketsPrefix) {
		return 0, false
	}
	idStr := strings.TrimPrefix(key, bucketsPrefix)
	id, err := strconv.Atoi(idStr)
	if err != nil {
		return 0, false
	}
	return id, true
}
