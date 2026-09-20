// Package pool manages one pgxpool.Pool per (shard, endpoint) pair,
// created lazily, per guide section 6.3:
//
//	- Create pools lazily when a shard is first used by a process.
//	- Use low minimum pool sizes and bounded maximum sizes.
//	- Reap idle pools for rarely used shards if the driver supports it.
//	- Apply a per-shard circuit breaker so one failing shard does not
//	  exhaust application worker threads.
//
// This directly addresses the guide's connection-explosion warning
// (section 6.3): "100 application pods x 20 physical shards x 10 pooled
// connections = 20,000 possible backend-facing client connections." Pools
// are keyed by endpoint (writer/reader are different pools) and reaped
// after a period of no use.
package pool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/shard-router/internal/breaker"
	"github.com/example/shard-router/internal/model"
	"github.com/example/shard-router/internal/secrets"
)

// Config bounds every pool this Manager creates. Keep these low and let
// PgBouncer (per the guide) absorb the client-side multiplexing instead of
// widening these numbers.
type Config struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	// IdlePoolTTL: a per-endpoint pool that hasn't been used for this long
	// is closed and evicted, per the guide's "reap idle pools" guidance.
	IdlePoolTTL time.Duration
}

func DefaultConfig() Config {
	return Config{
		MaxConns:        10,
		MinConns:        0,
		MaxConnLifetime: 30 * time.Minute,
		MaxConnIdleTime: 5 * time.Minute,
		IdlePoolTTL:     10 * time.Minute,
	}
}

type entry struct {
	pool     *pgxpool.Pool
	lastUsed time.Time
}

// Manager hands out pooled connections for a resolved shard endpoint,
// resolving credentials via secrets.Provider and gating access through a
// per-shard breaker.Registry so a failing shard fails fast instead of
// exhausting caller goroutines/timeouts.
type Manager struct {
	cfg      Config
	secrets  secrets.Provider
	breakers *breaker.Registry

	mu    sync.Mutex
	pools map[string]*entry // keyed by shardID+"|"+endpoint.String()

	stopReap chan struct{}
}

func NewManager(cfg Config, secretsProvider secrets.Provider, breakers *breaker.Registry) *Manager {
	m := &Manager{
		cfg:      cfg,
		secrets:  secretsProvider,
		breakers: breakers,
		pools:    make(map[string]*entry),
		stopReap: make(chan struct{}),
	}
	go m.reapLoop()
	return m
}

func poolKey(shardID string, ep model.Endpoint) string {
	return shardID + "|" + ep.String()
}

// Acquire returns a live connection for the given shard/endpoint,
// respecting that shard's circuit breaker. Callers must Release (Conn has
// a Release method, same as pgxpool.Conn) when done.
func (m *Manager) Acquire(ctx context.Context, shard model.ShardDef, ep model.Endpoint) (*pgxpool.Conn, error) {
	b := m.breakers.For(shard.ShardID)
	if err := b.Allow(); err != nil {
		return nil, fmt.Errorf("pool: shard %s: %w", shard.ShardID, err)
	}

	p, err := m.poolFor(ctx, shard, ep)
	if err != nil {
		b.Failure()
		return nil, err
	}

	conn, err := p.Acquire(ctx)
	if err != nil {
		b.Failure()
		return nil, fmt.Errorf("pool: acquire from shard %s (%s): %w", shard.ShardID, ep, err)
	}
	b.Success()
	return conn, nil
}

func (m *Manager) poolFor(ctx context.Context, shard model.ShardDef, ep model.Endpoint) (*pgxpool.Pool, error) {
	key := poolKey(shard.ShardID, ep)

	m.mu.Lock()
	if e, ok := m.pools[key]; ok {
		e.lastUsed = time.Now()
		m.mu.Unlock()
		return e.pool, nil
	}
	m.mu.Unlock()

	// Resolve credentials and build the pool outside the lock - opening a
	// pool can involve network I/O and must not block other shards.
	creds, err := m.secrets.Resolve(shard.CredentialRef)
	if err != nil {
		return nil, fmt.Errorf("pool: resolve credentials for shard %s: %w", shard.ShardID, err)
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		creds.Username, creds.Password, ep.Host, ep.Port, ep.Database)

	pgCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pool: parse config for shard %s: %w", shard.ShardID, err)
	}
	pgCfg.MaxConns = m.cfg.MaxConns
	pgCfg.MinConns = m.cfg.MinConns
	pgCfg.MaxConnLifetime = m.cfg.MaxConnLifetime
	pgCfg.MaxConnIdleTime = m.cfg.MaxConnIdleTime

	newPool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		return nil, fmt.Errorf("pool: create pool for shard %s (%s): %w", shard.ShardID, ep, err)
	}

	m.mu.Lock()
	// Another goroutine may have raced us to create the same pool; keep
	// whichever was installed first and close the loser.
	if e, ok := m.pools[key]; ok {
		m.mu.Unlock()
		newPool.Close()
		return e.pool, nil
	}
	m.pools[key] = &entry{pool: newPool, lastUsed: time.Now()}
	m.mu.Unlock()

	return newPool, nil
}

// reapLoop closes pools that haven't been used within IdlePoolTTL.
func (m *Manager) reapLoop() {
	ttl := m.cfg.IdlePoolTTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	ticker := time.NewTicker(ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopReap:
			return
		case <-ticker.C:
			m.reapOnce(ttl)
		}
	}
}

func (m *Manager) reapOnce(ttl time.Duration) {
	now := time.Now()
	m.mu.Lock()
	var toClose []*pgxpool.Pool
	for key, e := range m.pools {
		if now.Sub(e.lastUsed) > ttl {
			toClose = append(toClose, e.pool)
			delete(m.pools, key)
		}
	}
	m.mu.Unlock()
	for _, p := range toClose {
		p.Close()
	}
}

// Close stops reaping and closes every pool. Call on shutdown.
func (m *Manager) Close() {
	close(m.stopReap)
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, e := range m.pools {
		e.pool.Close()
		delete(m.pools, key)
	}
}
