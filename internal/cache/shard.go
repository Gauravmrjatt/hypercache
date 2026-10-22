package cache

import (
	"container/list"
	"errors"
	"sort"
	"sync"
	"time"
)

// ErrOutOfMemory is returned when a write cannot be accepted because the
// entry alone would exceed the shard memory budget, or because the shard is
// full and the configured policy does not evict.
var ErrOutOfMemory = errors.New("out of memory")

// ShardStats is a point-in-time snapshot of a single shard.
type ShardStats struct {
	ID          int
	Keys        int
	MemoryBytes int64
	Hits        uint64
	Misses      uint64
	Evictions   uint64
	Expired     uint64
}

// Shard owns a subset of the keyspace. Every shard has its own mutex so a
// slow command on one shard never blocks the others.
type Shard struct {
	mu sync.Mutex

	id        int
	items     map[string]*Entry
	policy    EvictionPolicy
	maxMemory int64

	memory int64

	// lru holds entries ordered from most recently used (front) to least
	// recently used (back). It is only maintained for EvictLRU.
	lru      *list.List
	lruIndex map[string]*list.Element

	hits      uint64
	misses    uint64
	evictions uint64
	expired   uint64
}

func newShard(id int, policy EvictionPolicy, maxMemory int64) *Shard {
	return &Shard{
		id:        id,
		items:     make(map[string]*Entry),
		policy:    policy,
		maxMemory: maxMemory,
		lru:       list.New(),
		lruIndex:  make(map[string]*list.Element),
	}
}

// get returns the value stored under key and refreshes access metadata.
func (s *Shard) get(key string, now time.Time) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[key]
	if !ok {
		s.misses++
		return nil, false
	}
	if entry.expiredAt(now) {
		s.removeLocked(key, entry)
		s.expired++
		s.misses++
		return nil, false
	}
	entry.LastAccess = now
	entry.AccessCount++
	s.touchLocked(entry)
	s.hits++
	return entry.Value, true
}

// set stores value under key, replacing any existing entry. It returns
// ErrOutOfMemory when the write cannot fit in the shard budget.
func (s *Shard) set(key string, value []byte, ttl time.Duration, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	newSize := estimateSize(key, value)
	if s.maxMemory > 0 && newSize > s.maxMemory {
		return ErrOutOfMemory
	}

	if old, ok := s.items[key]; ok {
		if s.maxMemory > 0 && s.policy == EvictNone {
			if s.memory-old.Size+newSize > s.maxMemory {
				return ErrOutOfMemory
			}
		}
		s.memory += newSize - old.Size
		old.Value = value
		old.Size = newSize
		old.Version++
		if ttl > 0 {
			old.ExpiresAt = now.Add(ttl)
		} else {
			old.ExpiresAt = time.Time{}
		}
		s.touchLocked(old)
	} else {
		if s.maxMemory > 0 && s.policy == EvictNone && s.memory+newSize > s.maxMemory {
			return ErrOutOfMemory
		}
		entry := newEntry(key, value, ttl, 1, now)
		s.items[key] = entry
		s.memory += newSize
		if s.policy == EvictLRU {
			s.lruIndex[key] = s.lru.PushFront(entry)
		}
	}
	s.evictLocked()
	return nil
}

// del removes key and reports whether a live entry was removed.
func (s *Shard) del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[key]
	if !ok {
		return false
	}
	s.removeLocked(key, entry)
	return true
}

// expire sets a new TTL on key. A non-positive duration removes the key.
func (s *Shard) expire(key string, ttl time.Duration, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[key]
	if !ok {
		return false
	}
	if entry.expiredAt(now) {
		s.removeLocked(key, entry)
		s.expired++
		return false
	}
	if ttl <= 0 {
		s.removeLocked(key, entry)
		return true
	}
	entry.ExpiresAt = now.Add(ttl)
	return true
}

// ttl returns the remaining lifetime of key. The bool is false when the key
// is missing or already expired; a remaining duration of -1 means the key
// exists without an expiry.
func (s *Shard) ttl(key string, now time.Time) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[key]
	if !ok {
		return 0, false
	}
	if entry.expiredAt(now) {
		s.removeLocked(key, entry)
		s.expired++
		return 0, false
	}
	return entry.ttlAt(now), true
}

// exists reports whether key is present and not expired.
func (s *Shard) exists(key string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.items[key]
	if !ok {
		return false
	}
	if entry.expiredAt(now) {
		s.removeLocked(key, entry)
		s.expired++
		return false
	}
	return true
}

// keys returns all non-expired keys, deleting expired ones on the way.
func (s *Shard) keys(now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, 0, len(s.items))
	for key, entry := range s.items {
		if entry.expiredAt(now) {
			s.removeLocked(key, entry)
			s.expired++
			continue
		}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// flush removes every entry from the shard.
func (s *Shard) flush() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = make(map[string]*Entry)
	s.lru.Init()
	s.lruIndex = make(map[string]*list.Element)
	s.memory = 0
}

// cleanupExpired removes expired entries and returns how many were removed.
func (s *Shard) cleanupExpired(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for key, entry := range s.items {
		if entry.expiredAt(now) {
			s.removeLocked(key, entry)
			s.expired++
			removed++
		}
	}
	return removed
}

func (s *Shard) stats() ShardStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return ShardStats{
		ID:          s.id,
		Keys:        len(s.items),
		MemoryBytes: s.memory,
		Hits:        s.hits,
		Misses:      s.misses,
		Evictions:   s.evictions,
		Expired:     s.expired,
	}
}

// removeLocked deletes an entry from the map, the LRU list and the memory
// accounting. Callers must hold s.mu.
func (s *Shard) removeLocked(key string, entry *Entry) {
	delete(s.items, key)
	s.memory -= entry.Size
	if s.policy == EvictLRU {
		if elem, ok := s.lruIndex[key]; ok {
			s.lru.Remove(elem)
			delete(s.lruIndex, key)
		}
	}
}

// touchLocked refreshes recency metadata. Callers must hold s.mu.
func (s *Shard) touchLocked(entry *Entry) {
	if s.policy != EvictLRU {
		return
	}
	elem, ok := s.lruIndex[entry.Key]
	if !ok {
		elem = s.lru.PushFront(entry)
		s.lruIndex[entry.Key] = elem
		return
	}
	s.lru.MoveToFront(elem)
}
