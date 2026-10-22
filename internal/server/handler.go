package server

import (
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Gauravmrjatt/hypercache/internal/cache"
	"github.com/Gauravmrjatt/hypercache/internal/metrics"
	"github.com/Gauravmrjatt/hypercache/internal/protocol"
)

// Handler executes protocol commands against the cache.
type Handler struct {
	cache      *cache.Cache
	metrics    *metrics.Collector
	defaultTTL time.Duration
}

// NewHandler builds a command handler.
func NewHandler(c *cache.Cache, m *metrics.Collector, defaultTTL time.Duration) *Handler {
	return &Handler{
		cache:      c,
		metrics:    m,
		defaultTTL: defaultTTL,
	}
}

// Handle dispatches one command and writes exactly one reply.
func (h *Handler) Handle(cmd protocol.Command, w *protocol.Writer) error {
	switch cmd.Name {
	case "PING":
		return h.ping(cmd, w)
	case "SET":
		return h.set(cmd, w)
	case "GET":
		return h.get(cmd, w)
	case "DEL":
		return h.del(cmd, w)
	case "EXISTS":
		return h.exists(cmd, w)
	case "EXPIRE":
		return h.expire(cmd, w)
	case "TTL":
		return h.ttl(cmd, w)
	case "KEYS":
		return h.keys(cmd, w)
	case "STATS":
		return h.stats(cmd, w)
	default:
		return w.Errorf("unknown command '%s'", strings.ToLower(cmd.Name))
	}
}

func (h *Handler) ping(cmd protocol.Command, w *protocol.Writer) error {
	if len(cmd.Args) != 0 {
		return wrongArgs(w, cmd.Name)
	}
	return w.Pong()
}

// set implements: SET key value [EX seconds | PX milliseconds]
func (h *Handler) set(cmd protocol.Command, w *protocol.Writer) error {
	if len(cmd.Args) < 2 {
		return wrongArgs(w, cmd.Name)
	}
	key, value := cmd.Args[0], cmd.Args[1]
	ttl := h.defaultTTL

	for i := 2; i < len(cmd.Args); i += 2 {
		if i+1 >= len(cmd.Args) {
			return w.Error(protocol.ErrSyntax.Error())
		}
		option := strings.ToUpper(cmd.Args[i])
		n, err := strconv.ParseInt(cmd.Args[i+1], 10, 64)
		if err != nil {
			return w.Error(protocol.ErrValueNotInteger.Error())
		}
		switch option {
		case "EX":
			if n <= 0 {
				return w.Errorf("invalid expire time in 'set' command")
			}
			ttl = time.Duration(n) * time.Second
		case "PX":
			if n <= 0 {
				return w.Errorf("invalid expire time in 'set' command")
			}
			ttl = time.Duration(n) * time.Millisecond
		default:
			return w.Error(protocol.ErrUnsupportedOption.Error())
		}
	}

	if err := h.cache.Set(key, []byte(value), ttl); err != nil {
		if err == cache.ErrOutOfMemory {
			return w.Error(protocol.ErrOutOfMemory.Error())
		}
		return w.Error(err.Error())
	}
	return w.OK()
}

func (h *Handler) get(cmd protocol.Command, w *protocol.Writer) error {
	if len(cmd.Args) != 1 {
		return wrongArgs(w, cmd.Name)
	}
	value, ok := h.cache.Get(cmd.Args[0])
	if !ok {
		return w.Nil()
	}
	return w.Bulk(value)
}

func (h *Handler) del(cmd protocol.Command, w *protocol.Writer) error {
	if len(cmd.Args) < 1 {
		return wrongArgs(w, cmd.Name)
	}
	return w.Int(int64(h.cache.Del(cmd.Args...)))
}

func (h *Handler) exists(cmd protocol.Command, w *protocol.Writer) error {
	if len(cmd.Args) < 1 {
		return wrongArgs(w, cmd.Name)
	}
	count := int64(0)
	for _, key := range cmd.Args {
		if h.cache.Exists(key) {
			count++
		}
	}
	return w.Int(count)
}

// expire implements: EXPIRE key seconds
func (h *Handler) expire(cmd protocol.Command, w *protocol.Writer) error {
	if len(cmd.Args) != 2 {
		return wrongArgs(w, cmd.Name)
	}
	seconds, err := strconv.ParseInt(cmd.Args[1], 10, 64)
	if err != nil {
		return w.Error(protocol.ErrValueNotInteger.Error())
	}
	if h.cache.Expire(cmd.Args[0], time.Duration(seconds)*time.Second) {
		return w.Int(1)
	}
	return w.Int(0)
}

// ttl implements: TTL key
//
// Replies with -2 when the key is missing or expired, -1 when the key has
// no expiry, otherwise the remaining seconds rounded up so a freshly set
// EX 60 key reports 60.
func (h *Handler) ttl(cmd protocol.Command, w *protocol.Writer) error {
	if len(cmd.Args) != 1 {
		return wrongArgs(w, cmd.Name)
	}
	remaining, ok := h.cache.TTL(cmd.Args[0])
	if !ok {
		return w.Int(-2)
	}
	if remaining < 0 {
		return w.Int(-1)
	}
	return w.Int(int64(math.Ceil(remaining.Seconds())))
}

// keys implements: KEYS [pattern]
func (h *Handler) keys(cmd protocol.Command, w *protocol.Writer) error {
	if len(cmd.Args) > 1 {
		return wrongArgs(w, cmd.Name)
	}
	keys := h.cache.Keys()
	if len(cmd.Args) == 1 {
		pattern := cmd.Args[0]
		filtered := make([]string, 0, len(keys))
		for _, key := range keys {
			ok, err := path.Match(pattern, key)
			if err != nil {
				return w.Error(protocol.ErrSyntax.Error())
			}
			if ok {
				filtered = append(filtered, key)
			}
		}
		keys = filtered
	}
	return w.Array(keys)
}

// stats implements: STATS
func (h *Handler) stats(cmd protocol.Command, w *protocol.Writer) error {
	if len(cmd.Args) != 0 {
		return wrongArgs(w, cmd.Name)
	}

	cacheStats := h.cache.Stats()
	snap := h.metrics.Snapshot()

	hitRatio := 0.0
	if total := cacheStats.Hits + cacheStats.Misses; total > 0 {
		hitRatio = float64(cacheStats.Hits) / float64(total)
	}

	lines := []string{
		fmt.Sprintf("uptime_seconds=%d", int64(snap.Uptime.Seconds())),
		fmt.Sprintf("shards=%d", cacheStats.Shards),
		fmt.Sprintf("keys=%d", cacheStats.Keys),
		fmt.Sprintf("memory_bytes=%d", cacheStats.MemoryBytes),
		fmt.Sprintf("hits=%d", cacheStats.Hits),
		fmt.Sprintf("misses=%d", cacheStats.Misses),
		fmt.Sprintf("hit_ratio=%.4f", hitRatio),
		fmt.Sprintf("evictions=%d", cacheStats.Evictions),
		fmt.Sprintf("expired_keys=%d", cacheStats.Expired),
		fmt.Sprintf("commands_total=%d", snap.CommandsTotal),
		fmt.Sprintf("parse_errors=%d", snap.ParseErrors),
		fmt.Sprintf("active_connections=%d", snap.ActiveConns),
		fmt.Sprintf("connections_total=%d", snap.TotalConns),
		fmt.Sprintf("avg_latency_us=%d", snap.AvgLatency.Microseconds()),
		fmt.Sprintf("p50_latency_us=%d", snap.P50Latency.Microseconds()),
		fmt.Sprintf("p95_latency_us=%d", snap.P95Latency.Microseconds()),
		fmt.Sprintf("p99_latency_us=%d", snap.P99Latency.Microseconds()),
		fmt.Sprintf("max_latency_us=%d", snap.MaxLatency.Microseconds()),
	}

	names := make([]string, 0, len(snap.Commands))
	for name := range snap.Commands {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		lines = append(lines, fmt.Sprintf("cmd_%s_total=%d", strings.ToLower(name), snap.Commands[name]))
	}

	for _, shard := range h.cache.ShardStats() {
		lines = append(lines,
			fmt.Sprintf("shard_%d_keys=%d", shard.ID, shard.Keys),
			fmt.Sprintf("shard_%d_memory_bytes=%d", shard.ID, shard.MemoryBytes),
			fmt.Sprintf("shard_%d_hits=%d", shard.ID, shard.Hits),
			fmt.Sprintf("shard_%d_evictions=%d", shard.ID, shard.Evictions),
		)
	}

	return w.Array(lines)
}

func wrongArgs(w *protocol.Writer, name string) error {
	return w.Errorf("wrong number of arguments for '%s' command", strings.ToLower(name))
}
