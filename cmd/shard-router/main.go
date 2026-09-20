// Command shard-router runs the shard-routing service described in the
// PostgreSQL Logical Sharding Implementation Guide: it resolves a business
// shard key to a physical PostgreSQL shard using a virtual-bucket map
// sourced from etcd, and exposes that routing (plus query execution) over
// HTTP.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/shard-router/internal/api"
	"github.com/example/shard-router/internal/breaker"
	"github.com/example/shard-router/internal/config"
	"github.com/example/shard-router/internal/metrics"
	"github.com/example/shard-router/internal/pool"
	"github.com/example/shard-router/internal/registry"
	"github.com/example/shard-router/internal/router"
	"github.com/example/shard-router/internal/secrets"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("shard-router exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Registry: authoritative shard map source (guide section 4) ---
	var reg registry.Registry
	switch cfg.RegistryMode {
	case config.RegistryModeFile:
		logger.Warn("running with file-based registry - do not use in production", "path", cfg.MappingFile)
		reg = registry.NewFileRegistry(cfg.MappingFile)
	default:
		reg, err = registry.NewEtcdRegistry(registry.EtcdConfig{
			Endpoints:         cfg.EtcdEndpoints,
			DialTimeout:       cfg.EtcdDialTimeout,
			Username:          cfg.EtcdUsername,
			Password:          cfg.EtcdPassword,
			ReconcileInterval: cfg.EtcdReconcileInterval,
		}, logger)
		if err != nil {
			return err
		}
	}
	defer reg.Close()

	logger.Info("loading initial shard map")
	if err := reg.Start(ctx); err != nil {
		return err
	}
	logger.Info("initial shard map loaded")

	// --- Router (guide section 6) ---
	r := router.New(reg.Store(), cfg.BucketCount)

	// --- Connection pooling + circuit breaking (guide section 6.3) ---
	breakers := breaker.NewRegistry()
	poolCfg := pool.DefaultConfig()
	poolCfg.MaxConns = cfg.PoolMaxConns
	secretProvider := secrets.NewEnvProvider() // swap for Vault/KMS/etc in production
	connPool := pool.NewManager(poolCfg, secretProvider, breakers)
	defer connPool.Close()

	// --- HTTP API ---
	metricsRegistry := metrics.NewRegistry()
	srv := api.NewServer(r, connPool, breakers, metricsRegistry, logger)

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("shard-router listening", "addr", cfg.HTTPAddr, "registry_mode", cfg.RegistryMode)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}
