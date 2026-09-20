// Package api exposes the shard router over HTTP:
//
//	GET  /healthz              liveness/readiness
//	GET  /v1/route?shard_key=  routing decision only, for debugging/observability
//	POST /v1/query             route + execute a parameterized SQL statement
//	GET  /metrics              plain-text counters, labeled by shard_id/bucket_id (guide 13.1)
//
// /v1/query is meant to be called by trusted internal services that already
// know the SQL they want to run (a repository/service layer, per guide
// section 7.3) - this process's job is only to pick the right shard, not to
// authorize arbitrary SQL from end users. Deploy it accordingly (internal
// network / mTLS), the same way you would any data-plane component that
// executes parameterized queries on the caller's behalf.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/example/shard-router/internal/breaker"
	"github.com/example/shard-router/internal/metrics"
	"github.com/example/shard-router/internal/model"
	"github.com/example/shard-router/internal/pool"
	"github.com/example/shard-router/internal/router"
)

type Server struct {
	router   *router.Router
	pool     *pool.Manager
	breakers *breaker.Registry
	metrics  *metrics.Registry
	logger   *slog.Logger
	mux      *http.ServeMux
}

func NewServer(r *router.Router, p *pool.Manager, b *breaker.Registry, m *metrics.Registry, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{router: r, pool: p, breakers: b, metrics: m, logger: logger, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /v1/route", s.handleRoute)
	s.mux.HandleFunc("POST /v1/query", s.handleQuery)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if _, err := s.router.Resolve("healthcheck-probe-key"); err != nil {
		// A resolution error for a fixed probe key most likely means no
		// mapping has loaded yet - report not-ready rather than crash.
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "reason": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type routeResponse struct {
	BucketID       int    `json:"bucket_id"`
	ReadShardID    string `json:"read_shard_id"`
	WriteShardIDs  []string `json:"write_shard_ids"`
	MigrationState string `json:"migration_state,omitempty"`
	WriterEndpoint string `json:"writer_endpoint"`
	ReaderEndpoint string `json:"reader_endpoint,omitempty"`
}

func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	shardKey := r.URL.Query().Get("shard_key")
	if shardKey == "" {
		writeError(w, http.StatusBadRequest, "shard_key query parameter is required")
		return
	}

	res, err := s.router.Resolve(shardKey)
	if err != nil {
		s.metrics.IncRoutingError()
		writeRouterError(w, err)
		return
	}
	s.metrics.IncRoutingRequest(res.ReadShard.ShardID, res.BucketID)

	resp := routeResponse{
		BucketID:       res.BucketID,
		ReadShardID:    res.ReadShard.ShardID,
		MigrationState: string(res.MigrationState),
		WriterEndpoint: res.ReadShard.Writer.String(),
	}
	for _, ws := range res.WriteShards {
		resp.WriteShardIDs = append(resp.WriteShardIDs, ws.ShardID)
	}
	if res.ReadShard.Reader != nil {
		resp.ReaderEndpoint = res.ReadShard.Reader.String()
	}
	writeJSON(w, http.StatusOK, resp)
}

type queryRequest struct {
	ShardKey    any             `json:"shard_key"`
	SQL         string          `json:"sql"`
	Args        []any           `json:"args"`
	Consistency string          `json:"consistency"`
	Write       bool            `json:"write"` // true routes to the writer/primary write path
}

type queryResponse struct {
	BucketID     int              `json:"bucket_id"`
	ShardID      string           `json:"shard_id"`
	Columns      []string         `json:"columns,omitempty"`
	Rows         [][]any          `json:"rows,omitempty"`
	RowsAffected int64            `json:"rows_affected,omitempty"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.ShardKey == nil || req.SQL == "" {
		writeError(w, http.StatusBadRequest, "shard_key and sql are required")
		return
	}

	res, err := s.router.Resolve(req.ShardKey)
	if err != nil {
		s.metrics.IncRoutingError()
		writeRouterError(w, err)
		return
	}
	s.metrics.IncRoutingRequest(res.ReadShard.ShardID, res.BucketID)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if req.Write {
		s.handleWrite(ctx, w, res, req)
		return
	}
	s.handleRead(ctx, w, res, req)
}

func (s *Server) handleRead(ctx context.Context, w http.ResponseWriter, res router.Resolved, req queryRequest) {
	consistency := model.ParseReadConsistency(req.Consistency)
	ep := res.ReadEndpoint(consistency)

	conn, err := s.pool.Acquire(ctx, res.ReadShard, ep)
	if err != nil {
		writePoolError(w, res.ReadShard.ShardID, err)
		return
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, req.SQL, req.Args...)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("query failed on shard %s: %v", res.ReadShard.ShardID, err))
		return
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	cols := make([]string, len(fields))
	for i, f := range fields {
		cols[i] = string(f.Name)
	}

	var out [][]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			writeError(w, http.StatusBadGateway, "reading row values: "+err.Error())
			return
		}
		out = append(out, vals)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusBadGateway, "row iteration error: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, queryResponse{
		BucketID: res.BucketID,
		ShardID:  res.ReadShard.ShardID,
		Columns:  cols,
		Rows:     out,
	})
}

// handleWrite executes against the primary write shard, and best-effort
// against any secondary write shard from an in-flight DUAL_WRITE migration
// (guide section 10.1). The primary shard's result is authoritative; a
// secondary-write failure is logged, not surfaced as the request's error,
// since the migration's CDC/verification step is responsible for catching
// up a lagging destination.
func (s *Server) handleWrite(ctx context.Context, w http.ResponseWriter, res router.Resolved, req queryRequest) {
	primary := res.PrimaryWriteShard()

	conn, err := s.pool.Acquire(ctx, primary, primary.Writer)
	if err != nil {
		writePoolError(w, primary.ShardID, err)
		return
	}
	defer conn.Release()

	tag, err := conn.Exec(ctx, req.SQL, req.Args...)
	if err != nil {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("write failed on shard %s: %v", primary.ShardID, err))
		return
	}

	for _, secondary := range res.WriteShards[1:] {
		go func(shard model.ShardDef) {
			c2, err := s.pool.Acquire(context.Background(), shard, shard.Writer)
			if err != nil {
				s.logger.Warn("dual-write to migration destination failed to acquire connection",
					"shard_id", shard.ShardID, "error", err)
				return
			}
			defer c2.Release()
			if _, err := c2.Exec(context.Background(), req.SQL, req.Args...); err != nil {
				s.logger.Warn("dual-write to migration destination failed",
					"shard_id", shard.ShardID, "error", err)
			}
		}(secondary)
	}

	writeJSON(w, http.StatusOK, queryResponse{
		BucketID:     res.BucketID,
		ShardID:      primary.ShardID,
		RowsAffected: tag.RowsAffected(),
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	snap := s.metrics.Snapshot()
	fmt.Fprintf(w, "# HELP shard_router_routing_requests_total Routing decisions made, by shard_id\n")
	fmt.Fprintf(w, "# TYPE shard_router_routing_requests_total counter\n")
	for _, shardID := range sortedKeys(snap.RequestsByShard) {
		fmt.Fprintf(w, "shard_router_routing_requests_total{shard_id=%q} %d\n", shardID, snap.RequestsByShard[shardID])
	}

	fmt.Fprintf(w, "# HELP shard_router_routing_errors_total Routing resolution failures\n")
	fmt.Fprintf(w, "# TYPE shard_router_routing_errors_total counter\n")
	fmt.Fprintf(w, "shard_router_routing_errors_total %d\n", snap.RoutingErrors)

	fmt.Fprintf(w, "# HELP shard_router_breaker_state Circuit breaker state by shard_id (0=closed,1=half_open,2=open)\n")
	fmt.Fprintf(w, "# TYPE shard_router_breaker_state gauge\n")
	states := s.breakers.Snapshot()
	for _, shardID := range sortedBreakerKeys(states) {
		fmt.Fprintf(w, "shard_router_breaker_state{shard_id=%q} %d\n", shardID, breakerStateValue(states[shardID]))
	}
}

func breakerStateValue(s breaker.State) int {
	switch s {
	case breaker.HalfOpen:
		return 1
	case breaker.Open:
		return 2
	default:
		return 0
	}
}

func sortedKeys(m map[string]int64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedBreakerKeys(m map[string]breaker.State) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func writeRouterError(w http.ResponseWriter, err error) {
	switch err.(type) {
	case *router.ErrShardUnavailable:
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case *router.ErrUnknownBucket, *router.ErrUnknownShard:
		writeError(w, http.StatusInternalServerError, err.Error())
	default:
		if err == router.ErrNoMapping {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func writePoolError(w http.ResponseWriter, shardID string, err error) {
	writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("shard %s unavailable: %v", shardID, err))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
