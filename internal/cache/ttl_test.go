package cache

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock so TTL tests never sleep.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTTLCache(t *testing.T) (*Cache, *fakeClock) {
	t.Helper()
	c := New(Config{ShardCount: 4, EvictionPolicy: EvictLRU})
	clock := newFakeClock()
	c.SetClock(clock.Now)
	t.Cleanup(c.Stop)
	return c, clock
}

func TestTTLExpiryOnRead(t *testing.T) {
	c, clock := newTTLCache(t)

	if err := c.Set("k", []byte("v"), time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit before expiry")
	}

	clock.Advance(500 * time.Millisecond)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("expected hit at half the TTL")
	}

	clock.Advance(600 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expected miss after expiry")
	}
	if c.Exists("k") {
		t.Fatal("Exists should be false after expiry")
	}

	stats := c.Stats()
	if stats.Keys != 0 {
		t.Fatalf("keys = %d, want 0 after lazy expiry", stats.Keys)
	}
	if stats.MemoryBytes != 0 {
		t.Fatalf("memory = %d, want 0 after lazy expiry", stats.MemoryBytes)
	}
}

func TestTTLCommandSemantics(t *testing.T) {
	c, clock := newTTLCache(t)

	if _, ok := c.TTL("missing"); ok {
		t.Fatal("TTL of missing key should report not found")
	}

	_ = c.Set("noexp", []byte("v"), 0)
	ttl, ok := c.TTL("noexp")
	if !ok || ttl != -1 {
		t.Fatalf("TTL = %v, %v; want -1, true", ttl, ok)
	}

	_ = c.Set("exp", []byte("v"), 10*time.Second)
	ttl, ok = c.TTL("exp")
	if !ok {
		t.Fatal("expected TTL to exist")
	}
	if ttl < 9*time.Second || ttl > 10*time.Second {
		t.Fatalf("TTL = %v, want close to 10s", ttl)
	}

	clock.Advance(4 * time.Second)
	ttl, ok = c.TTL("exp")
	if !ok || ttl != 6*time.Second {
		t.Fatalf("TTL after 4s = %v, %v; want 6s, true", ttl, ok)
	}

	clock.Advance(10 * time.Second)
	if _, ok := c.TTL("exp"); ok {
		t.Fatal("TTL after expiry should report not found")
	}
}

func TestExpireExtendsAndDeletes(t *testing.T) {
	c, _ := newTTLCache(t)

	if c.Expire("missing", time.Second) {
		t.Fatal("Expire on missing key should return false")
	}

	_ = c.Set("k", []byte("v"), 0)
	if !c.Expire("k", 30*time.Second) {
		t.Fatal("Expire on existing key should return true")
	}
	ttl, ok := c.TTL("k")
	if !ok || ttl != 30*time.Second {
		t.Fatalf("TTL = %v, %v; want 30s, true", ttl, ok)
	}

	// Non-positive TTL removes the key.
	if !c.Expire("k", -1) {
		t.Fatal("Expire with negative ttl should delete and report true")
	}
	if c.Exists("k") {
		t.Fatal("key should be gone after negative Expire")
	}
}

func TestCleanupExpiredSweepsAllShards(t *testing.T) {
	c, clock := newTTLCache(t)

	for i := 0; i < 50; i++ {
		key := string(rune('a'+i%26)) + string(rune('a'+i/26))
		if err := c.Set(key, []byte("v"), time.Second); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := c.Set("keep", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if removed := c.CleanupExpired(); removed != 0 {
		t.Fatalf("CleanupExpired removed %d keys before expiry", removed)
	}

	clock.Advance(2 * time.Second)

	removed := c.CleanupExpired()
	if removed != 50 {
		t.Fatalf("CleanupExpired removed %d keys, want 50", removed)
	}
	if removed := c.CleanupExpired(); removed != 0 {
		t.Fatalf("second sweep removed %d keys, want 0", removed)
	}
	if !c.Exists("keep") {
		t.Fatal("key without TTL should survive the sweep")
	}
}

func TestOverwriteRefreshesTTL(t *testing.T) {
	c, clock := newTTLCache(t)

	_ = c.Set("k", []byte("v1"), time.Second)
	clock.Advance(900 * time.Millisecond)
	_ = c.Set("k", []byte("v2"), time.Second)

	ttl, ok := c.TTL("k")
	if !ok || ttl != time.Second {
		t.Fatalf("TTL after overwrite = %v, %v; want 1s, true", ttl, ok)
	}

	// Overwriting without TTL clears the previous expiry.
	_ = c.Set("k", []byte("v3"), 0)
	ttl, ok = c.TTL("k")
	if !ok || ttl != -1 {
		t.Fatalf("TTL after TTL-less overwrite = %v, %v; want -1, true", ttl, ok)
	}
}
