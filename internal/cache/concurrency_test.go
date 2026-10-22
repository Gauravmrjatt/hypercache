package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentAccess exercises every cache operation from many goroutines
// at once. Run it with -race to catch data races.
func TestConcurrentAccess(t *testing.T) {
	c := New(Config{
		ShardCount:      8,
		MaxMemoryBytes:  8 * 1024 * 1024,
		EvictionPolicy:  EvictLRU,
		CleanupInterval: 5 * time.Millisecond,
	})
	c.StartJanitor(5 * time.Millisecond)
	t.Cleanup(c.Stop)

	const (
		writers = 8
		readers = 8
		keys    = 200
		ops     = 500
	)

	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("key:%d", (w+i)%keys)
				switch i % 5 {
				case 0:
					_ = c.Set(key, []byte(fmt.Sprintf("value-%d-%d", w, i)), 0)
				case 1:
					_ = c.Set(key, []byte("short"), 10*time.Millisecond)
				case 2:
					c.Del(key)
				case 3:
					c.Expire(key, 5*time.Millisecond)
				case 4:
					_ = c.Set(key, []byte("t"), 0)
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("key:%d", (r+i)%keys)
				c.Get(key)
				c.Exists(key)
				c.TTL(key)
				if i%50 == 0 {
					c.Keys()
					c.Stats()
				}
			}
		}(r)
	}

	wg.Wait()

	// The cache must remain fully usable and internally consistent.
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("key:%d", i)
		if err := c.Set(key, []byte("final"), 0); err != nil {
			t.Fatalf("Set(%s) after load: %v", key, err)
		}
		value, ok := c.Get(key)
		if !ok || string(value) != "final" {
			t.Fatalf("Get(%s) = %q, %v", key, value, ok)
		}
	}

	stats := c.Stats()
	if stats.Keys != keys {
		t.Fatalf("keys = %d, want %d", stats.Keys, keys)
	}
	if stats.MemoryBytes <= 0 {
		t.Fatal("memory accounting should be positive")
	}
	for _, shard := range c.ShardStats() {
		if shard.MemoryBytes < 0 {
			t.Fatalf("shard %d has negative memory: %d", shard.ID, shard.MemoryBytes)
		}
	}
}

func TestConcurrentShardedWritesStayInSync(t *testing.T) {
	c := New(Config{ShardCount: 16, EvictionPolicy: EvictLRU})
	t.Cleanup(c.Stop)

	const writers = 16
	const perWriter = 300

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%d:%d", w, i)
				if err := c.Set(key, []byte(key), 0); err != nil {
					t.Errorf("Set(%s): %v", key, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if stats := c.Stats(); stats.Keys != writers*perWriter {
		t.Fatalf("keys = %d, want %d", stats.Keys, writers*perWriter)
	}
}
