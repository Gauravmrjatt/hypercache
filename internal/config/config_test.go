package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultValues(t *testing.T) {
	cfg := Default()

	if cfg.Addr() != "127.0.0.1:6380" {
		t.Errorf("Addr = %q, want 127.0.0.1:6380", cfg.Addr())
	}
	if cfg.MetricsAddr() != "127.0.0.1:9090" {
		t.Errorf("MetricsAddr = %q, want 127.0.0.1:9090", cfg.MetricsAddr())
	}
	if cfg.Cache.ShardCount != 16 || cfg.Cache.EvictionPolicy != "lru" {
		t.Errorf("cache defaults = %+v", cfg.Cache)
	}
	if cfg.DefaultTTL() != 0 {
		t.Errorf("DefaultTTL = %v, want 0", cfg.DefaultTTL())
	}
	if cfg.CleanupInterval() != time.Second {
		t.Errorf("CleanupInterval = %v, want 1s", cfg.CleanupInterval())
	}
	if !cfg.Observability.MetricsEnabled {
		t.Error("metrics should be enabled by default")
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("default config should validate: %v", err)
	}
}

func TestLoadMergesOverDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hypercache.yaml")
	data := []byte(`
server:
  host: "0.0.0.0"
  port: 7000
cache:
  shard_count: 32
  eviction_policy: "none"
observability:
  metrics_port: 9191
`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Server.Host != "0.0.0.0" || cfg.Server.Port != 7000 {
		t.Errorf("server = %+v", cfg.Server)
	}
	if cfg.Cache.ShardCount != 32 || cfg.Cache.EvictionPolicy != "none" {
		t.Errorf("cache = %+v", cfg.Cache)
	}
	// Fields absent from the file keep their defaults.
	if cfg.Cache.MaxMemoryMB != 512 {
		t.Errorf("max_memory_mb = %d, want default 512", cfg.Cache.MaxMemoryMB)
	}
	if cfg.Cache.CleanupIntervalSeconds != 1 {
		t.Errorf("cleanup_interval_seconds = %d, want default 1", cfg.Cache.CleanupIntervalSeconds)
	}
	if cfg.Observability.MetricsPort != 9191 {
		t.Errorf("metrics_port = %d, want 9191", cfg.Observability.MetricsPort)
	}
	if !cfg.Observability.MetricsEnabled {
		t.Error("metrics_enabled should keep its default of true")
	}
}

func TestLoadErrors(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("expected an error for a missing file")
	}

	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("server: [\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Error("expected an error for malformed YAML")
	}
}

func TestValidateRejectsBadValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"port zero", func(c *Config) { c.Server.Port = 0 }},
		{"port too large", func(c *Config) { c.Server.Port = 70000 }},
		{"shard count", func(c *Config) { c.Cache.ShardCount = 0 }},
		{"negative memory", func(c *Config) { c.Cache.MaxMemoryMB = -1 }},
		{"metrics port", func(c *Config) { c.Observability.MetricsPort = -5 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected validation error for %s", tt.name)
			}
		})
	}
}
