package config

import (
	"fmt"
	"net"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the on-disk configuration for a HyperCache node.
type Config struct {
	Server        ServerConfig        `yaml:"server"`
	Cache         CacheConfig         `yaml:"cache"`
	Observability ObservabilityConfig `yaml:"observability"`
}

// ServerConfig configures the client-facing TCP listener.
type ServerConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
	// ReadTimeoutSeconds closes connections idle for longer than this.
	// Zero disables the timeout.
	ReadTimeoutSeconds int `yaml:"read_timeout_seconds"`
}

// CacheConfig configures the storage engine.
type CacheConfig struct {
	ShardCount int `yaml:"shard_count"`
	// MaxMemoryMB is the total memory budget; zero disables the limit.
	MaxMemoryMB            int    `yaml:"max_memory_mb"`
	EvictionPolicy         string `yaml:"eviction_policy"`
	DefaultTTLSeconds      int    `yaml:"default_ttl_seconds"`
	CleanupIntervalSeconds int    `yaml:"cleanup_interval_seconds"`
}

// ObservabilityConfig configures the metrics endpoint.
type ObservabilityConfig struct {
	MetricsEnabled bool `yaml:"metrics_enabled"`
	MetricsPort    int  `yaml:"metrics_port"`
}

// Default returns the built-in configuration.
func Default() Config {
	return Config{
		Server: ServerConfig{
			Host:               "127.0.0.1",
			Port:               6380,
			ReadTimeoutSeconds: 0,
		},
		Cache: CacheConfig{
			ShardCount:             16,
			MaxMemoryMB:            512,
			EvictionPolicy:         "lru",
			DefaultTTLSeconds:      0,
			CleanupIntervalSeconds: 1,
		},
		Observability: ObservabilityConfig{
			MetricsEnabled: true,
			MetricsPort:    9090,
		},
	}
}

// Load reads a YAML configuration file. Fields absent from the file keep
// their default values.
func Load(path string) (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	return cfg, nil
}

// Addr returns the client-facing listen address.
func (c Config) Addr() string {
	return net.JoinHostPort(c.Server.Host, fmt.Sprintf("%d", c.Server.Port))
}

// MetricsAddr returns the metrics endpoint listen address.
func (c Config) MetricsAddr() string {
	return net.JoinHostPort(c.Server.Host, fmt.Sprintf("%d", c.Observability.MetricsPort))
}

// ReadTimeout returns the idle connection timeout, zero when disabled.
func (c Config) ReadTimeout() time.Duration {
	return time.Duration(c.Server.ReadTimeoutSeconds) * time.Second
}

// DefaultTTL returns the TTL applied by SET when no EX/PX option is given.
// Zero means keys do not expire by default.
func (c Config) DefaultTTL() time.Duration {
	return time.Duration(c.Cache.DefaultTTLSeconds) * time.Second
}

// CleanupInterval returns the background expiry sweep interval.
func (c Config) CleanupInterval() time.Duration {
	return time.Duration(c.Cache.CleanupIntervalSeconds) * time.Second
}

// Validate checks configuration values that would otherwise fail at startup.
func (c Config) Validate() error {
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port must be between 1 and 65535, got %d", c.Server.Port)
	}
	if c.Cache.ShardCount < 1 {
		return fmt.Errorf("cache.shard_count must be >= 1, got %d", c.Cache.ShardCount)
	}
	if c.Cache.MaxMemoryMB < 0 {
		return fmt.Errorf("cache.max_memory_mb must be >= 0, got %d", c.Cache.MaxMemoryMB)
	}
	if c.Observability.MetricsEnabled && (c.Observability.MetricsPort < 1 || c.Observability.MetricsPort > 65535) {
		return fmt.Errorf("observability.metrics_port must be between 1 and 65535, got %d", c.Observability.MetricsPort)
	}
	return nil
}
