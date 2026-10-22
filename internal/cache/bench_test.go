package cache

import (
	"fmt"
	"testing"
)

// benchCache returns a cache with 16 shards and no memory limit.
func benchCache(b *testing.B) *Cache {
	b.Helper()
	c := New(Config{ShardCount: 16, MaxMemoryBytes: 0, EvictionPolicy: EvictLRU})
	b.Cleanup(c.Stop)
	return c
}

// BenchmarkSetGet measures the combined write and read path on a hot key.
func BenchmarkSetGet(b *testing.B) {
	c := benchCache(b)
	value := []byte("benchmark-value-payload")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%d", i%10000)
		if err := c.Set(key, value, 0); err != nil {
			b.Fatal(err)
		}
		if _, ok := c.Get(key); !ok {
			b.Fatal("miss")
		}
	}
}

// BenchmarkGetHit measures read throughput on keys already in the cache.
func BenchmarkGetHit(b *testing.B) {
	c := benchCache(b)
	value := []byte("benchmark-value-payload")
	for i := 0; i < 10000; i++ {
		if err := c.Set(fmt.Sprintf("key:%d", i), value, 0); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := c.Get(fmt.Sprintf("key:%d", i%10000)); !ok {
			b.Fatal("miss")
		}
	}
}

// BenchmarkGetHitParallel measures concurrent read throughput across shards.
func BenchmarkGetHitParallel(b *testing.B) {
	c := benchCache(b)
	value := []byte("benchmark-value-payload")
	for i := 0; i < 10000; i++ {
		if err := c.Set(fmt.Sprintf("key:%d", i), value, 0); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, ok := c.Get(fmt.Sprintf("key:%d", i%10000)); !ok {
				b.Error("miss")
				return
			}
			i++
		}
	})
}

// BenchmarkSetParallel measures concurrent write throughput across shards.
func BenchmarkSetParallel(b *testing.B) {
	c := benchCache(b)
	value := []byte("benchmark-value-payload")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := c.Set(fmt.Sprintf("key:%d", i), value, 0); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

// BenchmarkSingleShardGetHit shows the cost of contending on one shard,
// which is the baseline the sharded design is meant to beat.
func BenchmarkSingleShardGetHit(b *testing.B) {
	c := New(Config{ShardCount: 1, MaxMemoryBytes: 0, EvictionPolicy: EvictLRU})
	b.Cleanup(c.Stop)
	value := []byte("benchmark-value-payload")
	for i := 0; i < 10000; i++ {
		if err := c.Set(fmt.Sprintf("key:%d", i), value, 0); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, ok := c.Get(fmt.Sprintf("key:%d", i%10000)); !ok {
				b.Error("miss")
				return
			}
			i++
		}
	})
}

// BenchmarkSetEvict measures writes under memory pressure where every
// insert also performs an eviction.
func BenchmarkSetEvict(b *testing.B) {
	value := make([]byte, 512)
	single := estimateSize("key:0000", value)
	c := New(Config{
		ShardCount:     1,
		MaxMemoryBytes: 64 * single,
		EvictionPolicy: EvictLRU,
	})
	b.Cleanup(c.Stop)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key:%04d", i%4096)
		if err := c.Set(key, value, 0); err != nil {
			b.Fatal(err)
		}
	}
}
