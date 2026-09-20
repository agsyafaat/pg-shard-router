package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/example/shard-router/internal/cache"
	"github.com/example/shard-router/internal/model"
)

// EtcdConfig configures the etcd-backed registry. Endpoints are full base
// URLs (e.g. "http://etcd-01.internal:2379" or "https://..." for mTLS
// deployments per guide section 4.8) since requests go over etcd's
// grpc-gateway HTTP/JSON API.
type EtcdConfig struct {
	Endpoints   []string
	DialTimeout time.Duration
	// Username/Password enable etcd's own auth (RBAC per guide 4.8).
	Username string
	Password string
	// ReconcileInterval is how often the registry re-fetches the full
	// key-space as a safety net against a missed/interrupted watch stream
	// (guide 4.5: "periodic revision reconciliation protects against a
	// missed or interrupted watch stream").
	ReconcileInterval time.Duration
}

// EtcdRegistry implements Registry against a real etcd cluster, following
// the load-then-watch pattern from guide section 4.5:
//
//	etcd GET -> local map -> etcd WATCH -> incremental map updates
type EtcdRegistry struct {
	client *etcdClient
	store  *cache.Store
	cfg    EtcdConfig
	logger *slog.Logger

	cancel context.CancelFunc
}

// NewEtcdRegistry builds a registry client but does not load anything yet;
// call Start.
func NewEtcdRegistry(cfg EtcdConfig, logger *slog.Logger) (*EtcdRegistry, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("etcd registry: at least one endpoint is required")
	}
	if cfg.ReconcileInterval == 0 {
		cfg.ReconcileInterval = 60 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &EtcdRegistry{
		client: newEtcdClient(cfg),
		store:  cache.NewStore(),
		cfg:    cfg,
		logger: logger,
	}, nil
}

func (r *EtcdRegistry) Store() *cache.Store { return r.store }

func (r *EtcdRegistry) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

// Start performs the initial full load, then launches the watch loop and
// the periodic reconciliation loop in the background.
func (r *EtcdRegistry) Start(ctx context.Context) error {
	if r.cfg.Username != "" {
		if err := r.client.authenticate(ctx); err != nil {
			return fmt.Errorf("etcd registry: authenticate: %w", err)
		}
	}

	rev, err := r.loadAll(ctx)
	if err != nil {
		return fmt.Errorf("etcd registry: initial load: %w", err)
	}

	watchCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	go r.watchLoop(watchCtx, rev)
	go r.reconcileLoop(watchCtx)

	return nil
}

// loadAll fetches the entire /sharding/ key-space with a single Range
// request and builds a brand-new Mapping from it, returning the revision
// the snapshot was taken at.
func (r *EtcdRegistry) loadAll(ctx context.Context) (int64, error) {
	kvs, rev, err := r.client.rangePrefix(ctx, prefix)
	if err != nil {
		return 0, err
	}

	m := &cache.Mapping{
		Revision: rev,
		Shards:   make(map[string]model.ShardDef),
		Buckets:  make(map[int]model.BucketDef),
	}

	for _, e := range kvs {
		if err := applyKV(m, e.Key, e.Value, false); err != nil {
			r.logger.Warn("etcd registry: skipping malformed key", "key", e.Key, "error", err)
		}
	}

	r.store.Swap(m)
	r.logger.Info("etcd registry: initial load complete",
		"revision", m.Revision, "shards", len(m.Shards), "buckets", len(m.Buckets))
	return rev, nil
}

// watchLoop applies incremental updates from the given revision onward,
// copy-on-write against the current snapshot so concurrent readers are
// never affected. A temporary etcd outage should not stop ordinary traffic
// (guide 4.5): if the watch stream errors out or closes, this loop keeps
// retrying rather than clearing the store, so the last-known-good mapping
// keeps serving requests.
func (r *EtcdRegistry) watchLoop(ctx context.Context, fromRevision int64) {
	watchRev := fromRevision + 1
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		batches, err := r.client.watchPrefix(ctx, prefix, watchRev)
		if err != nil {
			r.logger.Warn("etcd registry: failed to open watch, will retry", "error", err)
			if !sleepOrDone(ctx, time.Second) {
				return
			}
			continue
		}

		for batch := range batches {
			if batch.Canceled {
				r.logger.Warn("etcd registry: watch canceled by server, will retry", "reason", batch.CancelReason)
				break
			}

			cur := r.store.Load()
			var next *cache.Mapping
			if cur != nil {
				next = cur.Clone()
			} else {
				next = &cache.Mapping{Shards: map[string]model.ShardDef{}, Buckets: map[int]model.BucketDef{}}
			}

			for _, ev := range batch.Events {
				switch ev.Type {
				case "PUT":
					if err := applyKV(next, ev.Key, ev.Value, false); err != nil {
						r.logger.Warn("etcd registry: skipping malformed watch event", "key", ev.Key, "error", err)
					}
				case "DELETE":
					_ = applyKV(next, ev.Key, nil, true)
				}
			}
			if batch.Revision > 0 {
				next.Revision = batch.Revision
				watchRev = batch.Revision + 1
			}
			r.store.Swap(next)

			r.logger.Debug("etcd registry: applied watch batch",
				"revision", next.Revision, "events", len(batch.Events))
		}

		if !sleepOrDone(ctx, time.Second) {
			return
		}
	}
}

// reconcileLoop periodically re-does a full load so a missed or silently
// dropped watch stream can't leave the local map stale forever (guide 4.5).
func (r *EtcdRegistry) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.loadAll(ctx); err != nil {
				r.logger.Warn("etcd registry: periodic reconciliation failed", "error", err)
			}
		}
	}
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// applyKV updates m in place for a single key/value. deleted indicates a
// delete event, in which case value is ignored.
func applyKV(m *cache.Mapping, key string, value []byte, deleted bool) error {
	if id, ok := shardIDFromKey(key); ok {
		if deleted {
			delete(m.Shards, id)
			return nil
		}
		var def model.ShardDef
		if err := json.Unmarshal(value, &def); err != nil {
			return fmt.Errorf("decode shard def: %w", err)
		}
		if def.ShardID == "" {
			def.ShardID = id
		}
		m.Shards[id] = def
		return nil
	}

	if id, ok := bucketIDFromKey(key); ok {
		if deleted {
			delete(m.Buckets, id)
			return nil
		}
		var def model.BucketDef
		if err := json.Unmarshal(value, &def); err != nil {
			return fmt.Errorf("decode bucket def: %w", err)
		}
		def.BucketID = id
		m.Buckets[id] = def
		return nil
	}

	// version / migrations / maintenance keys: not needed for routing
	// decisions themselves, so they're intentionally ignored here. A fuller
	// implementation would surface /sharding/maintenance/<shard-id> as an
	// additional signal to the breaker/pool layer.
	return nil
}
