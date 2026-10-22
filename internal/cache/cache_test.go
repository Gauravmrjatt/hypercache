package cache

import (
	"fmt"
	"testing"
	"time"
)

func newTestCache(t *testing.T) *Cache {
	t.Helper()
	c := New(Config{ShardCount: 4, MaxMemoryBytes: 0, EvictionPolicy: EvictLRU})
	t.Cleanup(c.Stop)
	return c
}

func TestSetGetDelete(t *testing.T) {
	c := newTestCache(t)

	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected miss for missing key")
	}
	if c.Exists("missing") {
		t.Fatal("Exists should be false for missing key")
	}

	if err := c.Set("user:1", []byte("Gaurav"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	value, ok := c.Get("user:1")
	if !ok || string(value) != "Gaurav" {
		t.Fatalf("Get = %q, %v; want Gaurav, true", value, ok)
	}
	if !c.Exists("user:1") {
		t.Fatal("Exists should be true after Set")
	}

	if err := c.Set("user:1", []byte("Ravi"), 0); err != nil {
		t.Fatalf("overwrite Set: %v", err)
	}
	value, ok = c.Get("user:1")
	if !ok || string(value) != "Ravi" {
		t.Fatalf("after overwrite Get = %q, %v; want Ravi, true", value, ok)
	}

	if removed := c.Del("user:1", "nope"); removed != 1 {
		t.Fatalf("Del removed = %d, want 1", removed)
	}
	if _, ok := c.Get("user:1"); ok {
		t.Fatal("expected miss after Del")
	}
}

func TestSetEmptyValue(t *testing.T) {
	c := newTestCache(t)
	if err := c.Set("empty", []byte(""), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	value, ok := c.Get("empty")
	if !ok {
		t.Fatal("expected hit for empty value")
	}
	if len(value) != 0 {
		t.Fatalf("value = %q, want empty", value)
	}
}

func TestKeysAreShardedAcrossShards(t *testing.T) {
	c := newTestCache(t)
	const count = 1000
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key:%d", i)
		if err := c.Set(key, []byte("v"), 0); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
	}

	stats := c.ShardStats()
	if len(stats) != 4 {
		t.Fatalf("shards = %d, want 4", len(stats))
	}
	nonEmpty := 0
	total := 0
	for _, s := range stats {
		total += s.Keys
		if s.Keys > 0 {
			nonEmpty++
		}
	}
	if total != count {
		t.Fatalf("total keys = %d, want %d", total, count)
	}
	if nonEmpty < 3 {
		t.Fatalf("only %d shards received keys, distribution looks broken", nonEmpty)
	}
}

func TestShardIndexIsStable(t *testing.T) {
	first := shardIndex("user:42", 16)
	for i := 0; i < 100; i++ {
		if got := shardIndex("user:42", 16); got != first {
			t.Fatalf("shardIndex not stable: %d != %d", got, first)
		}
	}
	if first < 0 || first >= 16 {
		t.Fatalf("shardIndex out of range: %d", first)
	}
}

func TestStatsCounters(t *testing.T) {
	c := newTestCache(t)

	_ = c.Set("a", []byte("1"), 0)
	c.Get("a")      // hit
	c.Get("b")      // miss
	c.Get("a")      // hit
	c.Del("a", "b") // one live removal

	stats := c.Stats()
	if stats.Hits != 2 {
		t.Errorf("hits = %d, want 2", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("misses = %d, want 1", stats.Misses)
	}
	if stats.Keys != 0 {
		t.Errorf("keys = %d, want 0", stats.Keys)
	}
	if stats.MemoryBytes != 0 {
		t.Errorf("memory = %d, want 0", stats.MemoryBytes)
	}
	if stats.Shards != 4 {
		t.Errorf("shards = %d, want 4", stats.Shards)
	}
}

func TestKeysReturnsSortedLiveKeys(t *testing.T) {
	c := newTestCache(t)
	for _, key := range []string{"c", "a", "b"} {
		if err := c.Set(key, []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	got := c.Keys()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("Keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Keys = %v, want %v", got, want)
		}
	}
}

func TestFlushRemovesEverything(t *testing.T) {
	c := newTestCache(t)
	_ = c.Set("a", []byte("1"), 0)
	_ = c.Set("b", []byte("2"), 0)
	c.Flush()

	if len(c.Keys()) != 0 {
		t.Fatalf("Keys after Flush = %v, want empty", c.Keys())
	}
	if stats := c.Stats(); stats.Keys != 0 || stats.MemoryBytes != 0 {
		t.Fatalf("stats after Flush = %+v", stats)
	}
}

func TestDefaultValueIsNotCopiedButIsImmutable(t *testing.T) {
	c := newTestCache(t)
	value := []byte("immutable")
	if err := c.Set("k", value, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, ok := c.Get("k")
	if !ok {
		t.Fatal("expected hit")
	}
	if &got[0] != &value[0] {
		t.Error("expected Set to store the caller's slice without copying")
	}
}

func TestJanitorStopsCleanly(t *testing.T) {
	c := New(Config{ShardCount: 2, EvictionPolicy: EvictLRU, CleanupInterval: 10 * time.Millisecond})
	_ = c.Set("k", []byte("v"), 10*time.Millisecond)
	c.StartJanitor(10 * time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	c.Stop()
	c.Stop() // idempotent

	if c.Exists("k") {
		t.Fatal("expected janitor to have removed the expired key")
	}
}
