package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/example/shard-router/internal/cache"
	"github.com/example/shard-router/internal/model"
)

// fileDoc is the on-disk shape for FileRegistry: a plain JSON rendering of
// the same shards/buckets that would otherwise live under /sharding/ in
// etcd. Useful for local development, demos, and integration tests that
// don't want to stand up a real 3-member etcd cluster.
type fileDoc struct {
	Shards  map[string]model.ShardDef `json:"shards"`
	Buckets map[string]model.BucketDef `json:"buckets"`
}

// FileRegistry implements Registry by reading a static JSON file once. It
// does not watch for changes; it exists for local development and tests,
// not production use (production must use EtcdRegistry per guide section
// 4).
type FileRegistry struct {
	path  string
	store *cache.Store
}

func NewFileRegistry(path string) *FileRegistry {
	return &FileRegistry{path: path, store: cache.NewStore()}
}

func (r *FileRegistry) Store() *cache.Store { return r.store }
func (r *FileRegistry) Close() error        { return nil }

func (r *FileRegistry) Start(_ context.Context) error {
	data, err := os.ReadFile(r.path)
	if err != nil {
		return fmt.Errorf("file registry: read %s: %w", r.path, err)
	}
	var doc fileDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("file registry: decode %s: %w", r.path, err)
	}

	m := &cache.Mapping{
		Revision: 1,
		Shards:   doc.Shards,
		Buckets:  make(map[int]model.BucketDef, len(doc.Buckets)),
	}
	for idStr, b := range doc.Buckets {
		id, err := parseBucketID(idStr)
		if err != nil {
			return fmt.Errorf("file registry: bucket key %q: %w", idStr, err)
		}
		b.BucketID = id
		m.Buckets[id] = b
	}
	r.store.Swap(m)
	return nil
}

func parseBucketID(s string) (int, error) {
	var id int
	_, err := fmt.Sscanf(s, "%d", &id)
	return id, err
}
