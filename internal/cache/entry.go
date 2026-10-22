package cache

import "time"

// entryOverheadBytes approximates the per-entry overhead of the map slot,
// list element and struct fields when accounting memory.
const entryOverheadBytes = 96

// Entry is a single cached value together with the metadata required for
// TTL handling, eviction decisions, hot-key detection and replication.
type Entry struct {
	Key         string
	Value       []byte
	ExpiresAt   time.Time // zero value means the key never expires
	LastAccess  time.Time
	AccessCount uint64
	Size        int64
	Version     uint64
}

func newEntry(key string, value []byte, ttl time.Duration, version uint64, now time.Time) *Entry {
	e := &Entry{
		Key:        key,
		Value:      value,
		LastAccess: now,
		Version:    version,
	}
	if ttl > 0 {
		e.ExpiresAt = now.Add(ttl)
	}
	e.Size = estimateSize(key, value)
	return e
}

// estimateSize returns the approximate memory cost of storing key/value.
func estimateSize(key string, value []byte) int64 {
	return int64(len(key)+len(value)) + entryOverheadBytes
}

// expiredAt reports whether the entry is expired at the given instant.
func (e *Entry) expiredAt(now time.Time) bool {
	return !e.ExpiresAt.IsZero() && !now.Before(e.ExpiresAt)
}

// ttlAt returns the remaining lifetime of the entry:
//
//	-1 when the entry has no expiry,
//	-2 when the entry is already expired,
//	a positive duration otherwise.
func (e *Entry) ttlAt(now time.Time) time.Duration {
	if e.ExpiresAt.IsZero() {
		return -1
	}
	remaining := e.ExpiresAt.Sub(now)
	if remaining <= 0 {
		return -2
	}
	return remaining
}
