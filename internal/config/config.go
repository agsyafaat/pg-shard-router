// Package config loads shard-router configuration from environment
// variables, following 12-factor conventions so the service is easy to run
// under any orchestrator (Kubernetes, ECS, systemd, plain docker run).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// RegistryMode selects which Registry implementation main.go wires up.
type RegistryMode string

const (
	RegistryModeEtcd RegistryMode = "etcd"
	RegistryModeFile RegistryMode = "file" // local dev / tests only
)

type Config struct {
	HTTPAddr string

	RegistryMode RegistryMode

	// EtcdEndpoints are full base URLs, e.g. "http://etcd-01:2379" or
	// "https://etcd-01:2379" for mTLS deployments - requests go over
	// etcd's grpc-gateway HTTP/JSON API, not the raw gRPC port.
	EtcdEndpoints        []string
	EtcdDialTimeout      time.Duration
	EtcdReconcileInterval time.Duration
	EtcdUsername         string
	EtcdPassword         string

	MappingFile string // used when RegistryMode == file

	BucketCount int

	PoolMaxConns int32
}

func Load() (Config, error) {
	c := Config{
		HTTPAddr:              getEnv("HTTP_ADDR", ":8080"),
		RegistryMode:          RegistryMode(getEnv("REGISTRY_MODE", string(RegistryModeEtcd))),
		EtcdEndpoints:         splitCSV(getEnv("ETCD_ENDPOINTS", "http://127.0.0.1:2379")),
		EtcdDialTimeout:       getDuration("ETCD_DIAL_TIMEOUT", 5*time.Second),
		EtcdReconcileInterval: getDuration("ETCD_RECONCILE_INTERVAL", 60*time.Second),
		EtcdUsername:          os.Getenv("ETCD_USERNAME"),
		EtcdPassword:          os.Getenv("ETCD_PASSWORD"),
		MappingFile:           getEnv("MAPPING_FILE", "testdata/mapping.example.json"),
		BucketCount:           getInt("BUCKET_COUNT", 1024),
		PoolMaxConns:          int32(getInt("POOL_MAX_CONNS", 10)),
	}

	if c.RegistryMode != RegistryModeEtcd && c.RegistryMode != RegistryModeFile {
		return Config{}, fmt.Errorf("config: invalid REGISTRY_MODE %q (want %q or %q)",
			c.RegistryMode, RegistryModeEtcd, RegistryModeFile)
	}
	if c.RegistryMode == RegistryModeEtcd && len(c.EtcdEndpoints) == 0 {
		return Config{}, fmt.Errorf("config: ETCD_ENDPOINTS must not be empty")
	}
	return c, nil
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func splitCSV(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
