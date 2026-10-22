package cache

import (
	"sync"
	"time"
)

// Config configures a Cache.
type Config struct {
	// ShardCount is the number of independent key partitions.
	ShardCount int
	// MaxMemoryBytes is the total budget across all shards. Zero or less
	// disables memory limits. The budget is split evenly across shards.
	MaxMemoryBytes int64
	// EvictionPolicy controls behaviour once a shard is full.
	EvictionPolicy EvictionPolicy
	// CleanupInterval drives the background expired-key sweeper.
	// Zero or less disables the sweeper; expired keys are still removed
	// lazily on access.
	CleanupInterval time.Duration
}

// DefaultConfig returns sensible single-node defaults.
func DefaultConfig() Config {
	return Config{
		ShardCount:      16,
		MaxMemoryBytes:  512 * 1024 * 1024,
		EvictionPolicy:  EvictLRU,
		CleanupInterval: time.Second,
	}
}

// Stats aggregates counters across every shard.
type Stats struct {
	Shards      int
	Keys        int
	MemoryBytes int64
	Hits        uint64
	Misses      uint64
	Evictions   uint64
	Expired     uint64
}

// Cache is a sharded, thread-safe in-memory key-value store.
type Cache struct {
	shards []*Shard
	cfg    Config

	now func() time.Time

	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
}

// New creates a Cache with the given configuration.
func New(cfg Config) *Cache {
	if cfg.ShardCount < 1 {
		cfg.ShardCount = 1
	}
	if cfg.EvictionPolicy == "" {
		cfg.EvictionPolicy = EvictLRU
	}

	perShard := int64(0)
	if cfg.MaxMemoryBytes > 0 {
		perShard = cfg.MaxMemoryBytes / int64(cfg.ShardCount)
	}

	c := &Cache{
		shards: make([]*Shard, cfg.ShardCount),
		cfg:    cfg,
		now:    time.Now,
		stop:   make(chan struct{}),
	}
	for i := range c.shards {
		c.shards[i] = newShard(i, cfg.EvictionPolicy, perShard)
	}
	return c
}

// SetClock replaces the clock used for TTL and eviction bookkeeping. It is
// intended for tests.
func (c *Cache) SetClock(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	c.now = now
}

// ShardCount returns the configured number of shards.
func (c *Cache) ShardCount() int {
	return len(c.shards)
}

// shardFor maps a key onto its owning shard using FNV-1a.
func (c *Cache) shardFor(key string) *Shard {
	return c.shards[shardIndex(key, len(c.shards))]
}

func shardIndex(key string, shardCount int) int {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return int(h % uint32(shardCount))
}

// Set stores value under key. A ttl of zero means the key never expires.
// The value slice is not copied and must not be modified afterwards.
func (c *Cache) Set(key string, value []byte, ttl time.Duration) error {
	return c.shardFor(key).set(key, value, ttl, c.now())
}

// Get returns the value stored under key. Expired entries are removed and
// reported as misses. The returned slice must not be modified.
func (c *Cache) Get(key string) ([]byte, bool) {
	return c.shardFor(key).get(key, c.now())
}

// Del removes the given keys and returns how many live entries were removed.
func (c *Cache) Del(keys ...string) int {
	removed := 0
	for _, key := range keys {
		if c.shardFor(key).del(key) {
			removed++
		}
	}
	return removed
}

// Exists reports whether key is present and not expired.
func (c *Cache) Exists(key string) bool {
	return c.shardFor(key).exists(key, c.now())
}

// Expire applies a TTL to an existing key. A non-positive ttl deletes it.
// It reports whether the key existed.
func (c *Cache) Expire(key string, ttl time.Duration) bool {
	return c.shardFor(key).expire(key, ttl, c.now())
}

// TTL returns the remaining lifetime of key. When ok is false the key is
// missing or expired. A returned duration of -1 means no expiry is set.
func (c *Cache) TTL(key string) (time.Duration, bool) {
	return c.shardFor(key).ttl(key, c.now())
}

// Keys returns every non-expired key in the cache, sorted.
func (c *Cache) Keys() []string {
	now := c.now()
	var out []string
	for _, shard := range c.shards {
		out = append(out, shard.keys(now)...)
	}
	return out
}

// Flush removes every key from the cache.
func (c *Cache) Flush() {
	for _, shard := range c.shards {
		shard.flush()
	}
}

// Stats returns aggregated counters for the whole cache.
func (c *Cache) Stats() Stats {
	stats := Stats{Shards: len(c.shards)}
	for _, shard := range c.shards {
		s := shard.stats()
		stats.Keys += s.Keys
		stats.MemoryBytes += s.MemoryBytes
		stats.Hits += s.Hits
		stats.Misses += s.Misses
		stats.Evictions += s.Evictions
		stats.Expired += s.Expired
	}
	return stats
}

// ShardStats returns per-shard snapshots in shard order.
func (c *Cache) ShardStats() []ShardStats {
	out := make([]ShardStats, 0, len(c.shards))
	for _, shard := range c.shards {
		out = append(out, shard.stats())
	}
	return out
}

// CleanupExpired removes expired keys from every shard and returns the total
// number of keys removed.
func (c *Cache) CleanupExpired() int {
	now := c.now()
	removed := 0
	for _, shard := range c.shards {
		removed += shard.cleanupExpired(now)
	}
	return removed
}

// StartJanitor launches the background sweeper unless the interval is
// non-positive. Stop or Close must be called to terminate it.
func (c *Cache) StartJanitor(interval time.Duration) {
	if interval <= 0 {
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-ticker.C:
				c.CleanupExpired()
			}
		}
	}()
}

// Stop terminates the background sweeper. It is safe to call multiple times.
func (c *Cache) Stop() {
	c.stopOnce.Do(func() { close(c.stop) })
	c.wg.Wait()
}

// Close stops background work. The cache remains usable for reads and
// writes afterwards.
func (c *Cache) Close() {
	c.Stop()
}
