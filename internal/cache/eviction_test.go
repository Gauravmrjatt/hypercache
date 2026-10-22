package cache

import (
	"fmt"
	"testing"
	"time"
)

func entrySize(key, value string) int64 {
	return estimateSize(key, []byte(value))
}

func newEvictionCache(t *testing.T, policy EvictionPolicy, maxMemory int64) *Cache {
	t.Helper()
	c := New(Config{ShardCount: 1, MaxMemoryBytes: maxMemory, EvictionPolicy: policy})
	t.Cleanup(c.Stop)
	return c
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	const valueLen = 100
	value := make([]byte, valueLen)
	single := entrySize("k0", string(value))
	maxMemory := 3 * single

	c := newEvictionCache(t, EvictLRU, maxMemory)

	for _, key := range []string{"k0", "k1", "k2"} {
		if err := c.Set(key, value, 0); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}
	if stats := c.Stats(); stats.Keys != 3 || stats.Evictions != 0 {
		t.Fatalf("stats = %+v, want 3 keys and 0 evictions", stats)
	}

	// Recency order is k2 (newest) .. k0 (oldest). Touching k0 makes k1 the
	// eviction candidate when k3 arrives.
	if _, ok := c.Get("k0"); !ok {
		t.Fatal("k0 should still be present before the fourth insert")
	}

	if err := c.Set("k3", value, 0); err != nil {
		t.Fatalf("Set(k3): %v", err)
	}

	stats := c.Stats()
	if stats.Keys != 3 {
		t.Fatalf("keys = %d, want 3 after eviction", stats.Keys)
	}
	if stats.Evictions != 1 {
		t.Fatalf("evictions = %d, want 1", stats.Evictions)
	}
	if stats.MemoryBytes > maxMemory {
		t.Fatalf("memory = %d, exceeds budget %d", stats.MemoryBytes, maxMemory)
	}
	if c.Exists("k1") {
		t.Error("k1 was the least recently used key and should have been evicted")
	}
	for _, key := range []string{"k0", "k2", "k3"} {
		if !c.Exists(key) {
			t.Errorf("key %s should still be present", key)
		}
	}
}

func TestLRUEvictionKeepsMemoryWithinBudget(t *testing.T) {
	const valueLen = 100
	value := make([]byte, valueLen)
	single := entrySize("key:000", string(value))

	c := newEvictionCache(t, EvictLRU, 5*single)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%03d", i)
		if err := c.Set(key, value, 0); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
		stats := c.Stats()
		if stats.MemoryBytes > 5*single {
			t.Fatalf("after %d inserts memory = %d, budget = %d", i+1, stats.MemoryBytes, 5*single)
		}
	}

	stats := c.Stats()
	if stats.Keys != 5 {
		t.Fatalf("keys = %d, want 5", stats.Keys)
	}
	if stats.Evictions != 95 {
		t.Fatalf("evictions = %d, want 95", stats.Evictions)
	}
	if !c.Exists("key:099") {
		t.Fatal("most recently written key must survive")
	}
	if c.Exists("key:000") {
		t.Fatal("oldest key should have been evicted")
	}
}

func TestNoEvictionPolicyRejectsWrites(t *testing.T) {
	const valueLen = 100
	value := make([]byte, valueLen)
	single := entrySize("k0", string(value))

	c := newEvictionCache(t, EvictNone, 2*single)

	for i := 0; i < 2; i++ {
		if err := c.Set(fmt.Sprintf("k%d", i), value, 0); err != nil {
			t.Fatalf("Set(k%d): %v", i, err)
		}
	}

	if err := c.Set("k2", value, 0); err != ErrOutOfMemory {
		t.Fatalf("Set(k2) error = %v, want ErrOutOfMemory", err)
	}
	if c.Exists("k2") {
		t.Fatal("rejected write must not be stored")
	}
	if stats := c.Stats(); stats.Evictions != 0 {
		t.Fatalf("evictions = %d, want 0", stats.Evictions)
	}

	// An overwrite that shrinks or keeps the size must still succeed.
	if err := c.Set("k0", value[:10], 0); err != nil {
		t.Fatalf("shrinking overwrite: %v", err)
	}
}

func TestOversizedEntryIsRejected(t *testing.T) {
	for _, policy := range []EvictionPolicy{EvictNone, EvictLRU} {
		c := newEvictionCache(t, policy, 100)
		if err := c.Set("big", make([]byte, 200), 0); err != ErrOutOfMemory {
			t.Fatalf("policy %s: Set error = %v, want ErrOutOfMemory", policy, err)
		}
	}
}

func TestNoMemoryLimitNeverEvicts(t *testing.T) {
	c := newEvictionCache(t, EvictLRU, 0)
	for i := 0; i < 500; i++ {
		if err := c.Set(fmt.Sprintf("k%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	stats := c.Stats()
	if stats.Keys != 500 || stats.Evictions != 0 {
		t.Fatalf("stats = %+v, want 500 keys and 0 evictions", stats)
	}
	if stats.MemoryBytes <= 0 {
		t.Fatal("memory accounting should still track usage without a limit")
	}
}

func TestExpiredKeysFreeBudgetWhenSwept(t *testing.T) {
	single := entrySize("k0", "value-0") // k0, k1 and k2 all share this size
	c := New(Config{
		ShardCount:     1,
		MaxMemoryBytes: 2 * single,
		EvictionPolicy: EvictNone,
	})
	t.Cleanup(c.Stop)
	clock := newFakeClock()
	c.SetClock(clock.Now)

	if err := c.Set("k0", []byte("value-0"), time.Second); err != nil {
		t.Fatalf("Set(k0): %v", err)
	}
	if err := c.Set("k1", []byte("value-1"), 0); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	// Budget is full even though k0 is already expired: lazy expiry has not
	// run yet and policy=none rejects new writes.
	if err := c.Set("k2", []byte("value-2"), 0); err != ErrOutOfMemory {
		t.Fatalf("Set(k2) error = %v, want ErrOutOfMemory", err)
	}

	clock.Advance(2 * time.Second)
	if removed := c.CleanupExpired(); removed != 1 {
		t.Fatalf("CleanupExpired removed %d, want 1", removed)
	}

	if err := c.Set("k2", []byte("value-2"), 0); err != nil {
		t.Fatalf("Set(k2) after sweep: %v", err)
	}
	if !c.Exists("k1") || !c.Exists("k2") {
		t.Fatal("both live keys should fit after the expired key was swept")
	}
}
