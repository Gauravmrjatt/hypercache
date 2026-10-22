package cache

import (
	"fmt"
	"strings"
)

// EvictionPolicy decides which entries are removed when a shard reaches its
// memory budget.
type EvictionPolicy string

const (
	// EvictNone never removes entries. Writes that would push a shard past
	// its memory budget are rejected instead.
	EvictNone EvictionPolicy = "none"
	// EvictLRU removes the least recently used entries first.
	EvictLRU EvictionPolicy = "lru"
)

// ParseEvictionPolicy validates a configured policy name.
func ParseEvictionPolicy(s string) (EvictionPolicy, error) {
	switch EvictionPolicy(strings.ToLower(strings.TrimSpace(s))) {
	case EvictNone:
		return EvictNone, nil
	case EvictLRU:
		return EvictLRU, nil
	default:
		return "", fmt.Errorf("unknown eviction policy %q (supported: %s, %s)", s, EvictNone, EvictLRU)
	}
}

// evictLocked removes least-recently-used entries until the shard is back
// within its memory budget. Callers must hold s.mu.
//
// The shard invariant is that no single entry is larger than the budget, so
// eviction always terminates before removing the most recently used entry.
func (s *Shard) evictLocked() int {
	if s.maxMemory <= 0 || s.policy != EvictLRU {
		return 0
	}
	evicted := 0
	for s.memory > s.maxMemory && s.lru.Len() > 1 {
		back := s.lru.Back()
		if back == nil {
			break
		}
		entry := back.Value.(*Entry)
		s.removeLocked(entry.Key, entry)
		s.evictions++
		evicted++
	}
	return evicted
}
