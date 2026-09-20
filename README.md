# shard-router

An application-level PostgreSQL shard-routing service, implementing the
architecture from *PostgreSQL Logical Sharding — Implementation Guide*
(v1.2): a business shard key (`tenant_id`, `merchant_id`, ...) is hashed to
one of a fixed number of virtual buckets, each bucket is mapped to a
physical PostgreSQL shard via an etcd-backed registry, and the service
routes reads/writes accordingly — including mid-flight bucket migrations.

## Architecture

```
etcd cluster (shard config, Raft)
        |  GET (initial) + WATCH (incremental)
        v
  local in-memory shard map  <-- this service, per instance
        |  hash(shard_key) -> virtual bucket -> shard_id
        v
  connection pool (per shard, lazy, circuit-broken)
        |
        v
  PostgreSQL shard (PgBouncer/HAProxy/VIP -> primary/replica)
```

| Package                | Responsibility                                                                 | Guide section |
|-------------------------|--------------------------------------------------------------------------------|---------------|
| `internal/model`        | Shard/bucket/migration types mirroring the etcd key-space                     | 4.3, 4.4      |
| `internal/hashing`      | Stable, versioned SHA-256 bucket hash + golden vectors                        | 6.2, 15.1     |
| `internal/cache`        | Lock-free, atomically-swapped local shard map                                 | 4.5           |
| `internal/router`       | `Resolve()`: bucket → shard(s), migration-state-aware read/write path         | 6.1, 9, 10.1  |
| `internal/registry`     | `Registry` interface: `EtcdRegistry` (HTTP/JSON gateway) + `FileRegistry`     | 4.5, 4.6      |
| `internal/secrets`      | `credential_ref` → DB credentials (never stored in etcd)                      | 4.4, 13.3     |
| `internal/pool`         | Lazy per-shard `pgxpool` connections, idle reaping                            | 6.3           |
| `internal/breaker`      | Per-shard circuit breaker                                                     | 6.3           |
| `internal/api`          | HTTP: `/v1/route`, `/v1/query`, `/healthz`, `/metrics`                        | —             |
| `cmd/shard-router`      | Wiring / entrypoint                                                            | —             |

### Why the etcd client talks HTTP/JSON instead of using `go.etcd.io/etcd/client/v3`

`clientv3` pulls in gRPC, zap, and `golang.org/x/*` as transitive
dependencies. To keep this service's dependency surface small (effectively
just the Postgres driver), `internal/registry/etcdhttp.go` talks to etcd's
own [grpc-gateway JSON API](https://etcd.io/docs/v3.5/dev-guide/api_grpc_gateway/)
(`/v3/kv/range`, `/v3/watch`) directly over `net/http`. It's the same gRPC
service, same key-space, same semantics — just a lighter client. Swap in
`clientv3` instead if your organization standardizes on it; the `Registry`
interface in `internal/registry/registry.go` is the seam.

## Migration-state-aware routing (guide §10.1)

`router.Resolve()` reads a bucket's `state` field and routes accordingly:

| State                                   | Reads  | Writes           |
|-------------------------------------------|--------|-------------------|
| `ACTIVE_SOURCE`, `COPYING`                | source | source            |
| `DUAL_WRITE`, `CDC_CATCHUP`, `VERIFYING`  | source | source **and** dest (dest best-effort/async) |
| `CUTOVER`, `DRAINING_SOURCE`, `ACTIVE_DEST` | dest | dest              |

A `CUTOVER` bucket whose destination isn't registered yet fails the
resolution rather than silently routing to source or dropping the write
(guide §10.2's "avoid unsafe cutover").

## etcd key-space (guide §4.3/§4.4)

```
/sharding/version
/sharding/shards/<shard-id>      e.g. {"shard_id":"shard-01","status":"ACTIVE",
                                        "writer":{"host":"pg-shard01-rw.internal","port":6432,"database":"payment"},
                                        "reader":{"host":"pg-shard01-ro.internal","port":6432,"database":"payment"},
                                        "credential_ref":"secret/payment/postgresql","region":"jakarta"}
/sharding/buckets/<bucket-id>    e.g. {"shard_id":"shard-01"}
                                 during migration: {"shard_id":"shard-01","dest_shard_id":"shard-05","state":"DUAL_WRITE"}
/sharding/migrations/<id>        (not yet consumed by the router; reserved for a migration-orchestrator service)
/sharding/maintenance/<shard-id> (not yet consumed by the router; reserved for planned-maintenance signaling)
```

Seed a local etcd for testing:

```sh
etcdctl put /sharding/shards/shard-01 '{"shard_id":"shard-01","status":"ACTIVE","writer":{"host":"127.0.0.1","port":5433,"database":"payment"},"credential_ref":"secret/payment/postgresql","region":"jakarta"}'
etcdctl put /sharding/buckets/0000 '{"shard_id":"shard-01"}'
```

## Configuration (env vars)

| Variable                    | Default                              | Notes |
|------------------------------|---------------------------------------|-------|
| `HTTP_ADDR`                  | `:8080`                              | |
| `REGISTRY_MODE`              | `etcd`                               | `etcd` or `file` (local dev only) |
| `ETCD_ENDPOINTS`             | `http://127.0.0.1:2379`              | comma-separated full base URLs |
| `ETCD_DIAL_TIMEOUT`          | `5s`                                 | |
| `ETCD_RECONCILE_INTERVAL`    | `60s`                                | full re-load safety net, guide §4.5 |
| `ETCD_USERNAME` / `ETCD_PASSWORD` | unset                            | etcd RBAC auth, guide §4.8 |
| `MAPPING_FILE`               | `testdata/mapping.example.json`      | used when `REGISTRY_MODE=file` |
| `BUCKET_COUNT`               | `1024`                               | guide §17.3 default |
| `POOL_MAX_CONNS`             | `10`                                 | per (shard, endpoint) pool, guide §6.3 |

Credentials: a shard's `credential_ref` (e.g. `secret/payment/postgresql`)
is resolved via `internal/secrets.Provider`. The bundled `EnvProvider`
(dev/CI only) reads `SECRET_PAYMENT_POSTGRESQL_USER` /
`SECRET_PAYMENT_POSTGRESQL_PASSWORD`. Implement `Provider` against Vault /
your cloud secret manager for production and wire it in in `cmd/shard-router/main.go`.

## Running

```sh
go build ./...
go test ./...             # hashing golden vectors, router migration-state matrix, registry decoding

# local dev without a real etcd cluster:
REGISTRY_MODE=file MAPPING_FILE=testdata/mapping.example.json BUCKET_COUNT=4 \
  SECRET_PAYMENT_POSTGRESQL_USER=postgres SECRET_PAYMENT_POSTGRESQL_PASSWORD=postgres \
  go run ./cmd/shard-router

# against real etcd:
ETCD_ENDPOINTS=http://etcd-01:2379,http://etcd-02:2379,http://etcd-03:2379 \
  go run ./cmd/shard-router
```

## API

```
GET  /healthz
GET  /v1/route?shard_key=tenant-123
POST /v1/query   {"shard_key": "tenant-123", "sql": "SELECT ...", "args": [...], "consistency": "EVENTUAL"}
POST /v1/query   {"shard_key": "tenant-123", "sql": "UPDATE ...", "args": [...], "write": true}
GET  /metrics
```

`/v1/query` is a data-plane endpoint meant for trusted internal
callers that already know the SQL they want to run (guide §7.3's
repository/service layer) — this service's job is to pick the right shard,
not to authorize arbitrary SQL from end users. Deploy it on an internal
network accordingly.

## What's intentionally not implemented yet

- Consuming `/sharding/migrations/<id>` and `/sharding/maintenance/<shard-id>`
  as first-class signals (currently a migration orchestrator would just
  drive bucket `state`/`dest_shard_id` directly).
- WAL-LSN-aware `READ_YOUR_WRITES` (guide §9's "advanced implementation");
  currently `READ_YOUR_WRITES` routes to the primary, which is correct but
  more conservative than necessary.
- mTLS wiring for the etcd HTTP client (the `Endpoints` field accepts
  `https://` URLs; supply a custom `http.Client` with the right
  `tls.Config` for your environment).
- A real secrets backend (Vault/KMS) — only the env-based dev provider ships.
