package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Gauravmrjatt/hypercache/internal/cache"
	"github.com/Gauravmrjatt/hypercache/internal/config"
	"github.com/Gauravmrjatt/hypercache/internal/metrics"
	"github.com/Gauravmrjatt/hypercache/internal/server"
)

const version = "0.1.0"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "hypercache:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath = flag.String("config", "", "path to a YAML config file")
		addr       = flag.String("addr", "", "listen address, overrides server.host/server.port")
		shards     = flag.Int("shards", 0, "shard count, overrides cache.shard_count (0 = use config)")
		maxMemory  = flag.Int("max-memory-mb", -1, "memory budget in MiB, overrides cache.max_memory_mb (-1 = use config)")
		eviction   = flag.String("eviction", "", "eviction policy (none|lru), overrides cache.eviction_policy")
		metricsURL = flag.String("metrics-addr", "", "metrics listen address, overrides observability.*")
		noMetrics  = flag.Bool("no-metrics", false, "disable the Prometheus metrics endpoint")
		showVer    = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Printf("hypercache %s\n", version)
		return nil
	}

	cfg := config.Default()
	if *configPath != "" {
		loaded, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		cfg = loaded
	}

	if *addr != "" {
		host, port, err := splitHostPort(*addr)
		if err != nil {
			return err
		}
		cfg.Server.Host = host
		cfg.Server.Port = port
	}
	if *shards > 0 {
		cfg.Cache.ShardCount = *shards
	}
	if *maxMemory >= 0 {
		cfg.Cache.MaxMemoryMB = *maxMemory
	}
	if *eviction != "" {
		cfg.Cache.EvictionPolicy = *eviction
	}
	if *noMetrics {
		cfg.Observability.MetricsEnabled = false
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	policy, err := cache.ParseEvictionPolicy(cfg.Cache.EvictionPolicy)
	if err != nil {
		return err
	}

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	cacheCfg := cache.Config{
		ShardCount:      cfg.Cache.ShardCount,
		MaxMemoryBytes:  int64(cfg.Cache.MaxMemoryMB) * 1024 * 1024,
		EvictionPolicy:  policy,
		CleanupInterval: cfg.CleanupInterval(),
	}
	c := cache.New(cacheCfg)
	c.StartJanitor(cacheCfg.CleanupInterval)
	defer c.Stop()

	collector := metrics.New(func() cache.Stats { return c.Stats() })

	if cfg.Observability.MetricsEnabled {
		metricsAddr := cfg.MetricsAddr()
		if *metricsURL != "" {
			metricsAddr = *metricsURL
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", collector)
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
		})
		metricsServer := &http.Server{Addr: metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Printf("metrics endpoint failed: %v", err)
			}
		}()
		go func() {
			stop := make(chan os.Signal, 1)
			signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
			<-stop
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = metricsServer.Shutdown(ctx)
		}()
		logger.Printf("metrics listening on http://%s/metrics", metricsAddr)
	}

	handler := server.NewHandler(c, collector, cfg.DefaultTTL())
	srv := server.New(server.Config{
		Addr:        cfg.Addr(),
		ReadTimeout: cfg.ReadTimeout(),
	}, handler, collector)

	// Graceful shutdown on SIGINT/SIGTERM.
	shutdownDone := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		logger.Printf("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Printf("shutdown: %v", err)
		}
		close(shutdownDone)
	}()

	logger.Printf("hypercache %s starting", version)
	logger.Printf("listening on %s (shards=%d max_memory=%dMiB policy=%s)",
		cfg.Addr(), cacheCfg.ShardCount, cfg.Cache.MaxMemoryMB, policy)

	if err := srv.ListenAndServe(); err != nil {
		if errors.Is(err, server.ErrServerClosed) {
			<-shutdownDone
			logger.Printf("stopped")
			return nil
		}
		return err
	}
	return nil
}

// splitHostPort splits "host:port" into its components.
func splitHostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid -addr %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid -addr %q: port must be between 1 and 65535", addr)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host, port, nil
}
