package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Gauravmrjatt/hypercache/internal/cache"
)

// latencyBuckets are the upper bounds, in seconds, of the command latency
// histogram.
var latencyBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5,
}

// knownCommands are the commands whose counters exist from startup. Anything
// a client sends outside this list is folded into "OTHER" so a hostile
// client cannot grow the counter map.
var knownCommands = []string{
	"PING", "SET", "GET", "DEL", "EXISTS", "EXPIRE", "TTL", "KEYS", "STATS", "OTHER",
}

type histogram struct {
	counts  []atomic.Uint64
	sumNano atomic.Uint64
	total   atomic.Uint64
	maxNano atomic.Uint64
}

func newHistogram() *histogram {
	return &histogram{counts: make([]atomic.Uint64, len(latencyBuckets))}
}

func (h *histogram) observe(d time.Duration) {
	secs := d.Seconds()
	for i, upper := range latencyBuckets {
		if secs <= upper {
			h.counts[i].Add(1)
			break
		}
	}
	h.total.Add(1)
	h.sumNano.Add(uint64(d))
	for {
		prev := h.maxNano.Load()
		if uint64(d) <= prev || h.maxNano.CompareAndSwap(prev, uint64(d)) {
			break
		}
	}
}

func (h *histogram) mean() time.Duration {
	total := h.total.Load()
	if total == 0 {
		return 0
	}
	return time.Duration(h.sumNano.Load() / total)
}

// quantile returns an upper-bucket estimate of the given quantile, clamped
// to the observed maximum so the result never exceeds the true tail.
func (h *histogram) quantile(q float64) time.Duration {
	total := h.total.Load()
	if total == 0 {
		return 0
	}
	target := uint64(q * float64(total))
	if target == 0 {
		target = 1
	}
	maxObserved := time.Duration(h.maxNano.Load())
	var cumulative uint64
	for i, upper := range latencyBuckets {
		cumulative += h.counts[i].Load()
		if cumulative >= target {
			bound := time.Duration(upper * float64(time.Second))
			if maxObserved > 0 && bound > maxObserved {
				return maxObserved
			}
			return bound
		}
	}
	return maxObserved
}

// Snapshot is a point-in-time view of the collector, used by the STATS
// command and by the Prometheus export.
type Snapshot struct {
	Uptime        time.Duration
	CommandsTotal uint64
	Commands      map[string]uint64
	ParseErrors   uint64
	ActiveConns   int64
	TotalConns    uint64
	AvgLatency    time.Duration
	P50Latency    time.Duration
	P95Latency    time.Duration
	P99Latency    time.Duration
	MaxLatency    time.Duration
}

// Collector records command, latency and connection metrics.
type Collector struct {
	start      time.Time
	cacheStats func() cache.Stats

	commands    map[string]*atomic.Uint64
	latency     *histogram
	parseErrors atomic.Uint64
	activeConns atomic.Int64
	totalConns  atomic.Uint64
}

// New creates a Collector. cacheStats may be nil, in which case cache-derived
// series are omitted from the export.
func New(cacheStats func() cache.Stats) *Collector {
	c := &Collector{
		start:      time.Now(),
		cacheStats: cacheStats,
		commands:   make(map[string]*atomic.Uint64, len(knownCommands)),
		latency:    newHistogram(),
	}
	for _, name := range knownCommands {
		c.commands[name] = &atomic.Uint64{}
	}
	return c
}

// IncCommand increments the counter for a command name.
func (c *Collector) IncCommand(name string) {
	if _, ok := c.commands[name]; !ok {
		name = "OTHER"
	}
	c.commands[name].Add(1)
}

// IncParseError counts a malformed protocol line.
func (c *Collector) IncParseError() {
	c.parseErrors.Add(1)
}

// ObserveLatency records the duration of one command.
func (c *Collector) ObserveLatency(d time.Duration) {
	c.latency.observe(d)
}

// ConnOpened records a new client connection.
func (c *Collector) ConnOpened() {
	c.activeConns.Add(1)
	c.totalConns.Add(1)
}

// ConnClosed records a closed client connection.
func (c *Collector) ConnClosed() {
	c.activeConns.Add(-1)
}

// Snapshot returns the current metric view.
func (c *Collector) Snapshot() Snapshot {
	commands := make(map[string]uint64, len(c.commands))
	var total uint64
	for name, counter := range c.commands {
		v := counter.Load()
		commands[name] = v
		total += v
	}
	return Snapshot{
		Uptime:        time.Since(c.start),
		CommandsTotal: total,
		Commands:      commands,
		ParseErrors:   c.parseErrors.Load(),
		ActiveConns:   c.activeConns.Load(),
		TotalConns:    c.totalConns.Load(),
		AvgLatency:    c.latency.mean(),
		P50Latency:    c.latency.quantile(0.50),
		P95Latency:    c.latency.quantile(0.95),
		P99Latency:    c.latency.quantile(0.99),
		MaxLatency:    time.Duration(c.latency.maxNano.Load()),
	}
}

// ServeHTTP exposes the metrics in Prometheus text exposition format.
func (c *Collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	c.writePrometheus(w)
}

func (c *Collector) writePrometheus(out io.Writer) {
	var b strings.Builder

	writeHelpType := func(name, help, typ string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}

	snap := c.Snapshot()

	writeHelpType("hypercache_commands_total", "Total commands processed by name.", "counter")
	names := make([]string, 0, len(snap.Commands))
	for name := range snap.Commands {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(&b, "hypercache_commands_total{cmd=%q} %d\n", name, snap.Commands[name])
	}

	writeHelpType("hypercache_parse_errors_total", "Malformed protocol lines rejected.", "counter")
	fmt.Fprintf(&b, "hypercache_parse_errors_total %d\n", snap.ParseErrors)

	writeHelpType("hypercache_command_duration_seconds", "Command handling latency.", "histogram")
	fmt.Fprintf(&b, "hypercache_command_duration_seconds_count %d\n", c.latency.total.Load())
	fmt.Fprintf(&b, "hypercache_command_duration_seconds_sum %s\n", formatFloat(float64(c.latency.sumNano.Load())/1e9))
	var cumulative uint64
	for i, upper := range latencyBuckets {
		cumulative += c.latency.counts[i].Load()
		fmt.Fprintf(&b, "hypercache_command_duration_seconds_bucket{le=%q} %d\n", formatFloat(upper), cumulative)
	}
	fmt.Fprintf(&b, "hypercache_command_duration_seconds_bucket{le=\"+Inf\"} %d\n", c.latency.total.Load())

	writeHelpType("hypercache_connections_active", "Currently open client connections.", "gauge")
	fmt.Fprintf(&b, "hypercache_connections_active %d\n", snap.ActiveConns)
	writeHelpType("hypercache_connections_total", "Client connections accepted since start.", "counter")
	fmt.Fprintf(&b, "hypercache_connections_total %d\n", snap.TotalConns)

	writeHelpType("hypercache_uptime_seconds", "Process uptime in seconds.", "gauge")
	fmt.Fprintf(&b, "hypercache_uptime_seconds %s\n", formatFloat(snap.Uptime.Seconds()))

	if c.cacheStats != nil {
		stats := c.cacheStats()

		writeHelpType("hypercache_keys", "Number of keys currently stored.", "gauge")
		fmt.Fprintf(&b, "hypercache_keys %d\n", stats.Keys)
		writeHelpType("hypercache_memory_bytes", "Estimated memory used by cache entries.", "gauge")
		fmt.Fprintf(&b, "hypercache_memory_bytes %d\n", stats.MemoryBytes)
		writeHelpType("hypercache_shards", "Number of configured shards.", "gauge")
		fmt.Fprintf(&b, "hypercache_shards %d\n", stats.Shards)

		writeHelpType("hypercache_cache_hits_total", "Total cache hits.", "counter")
		fmt.Fprintf(&b, "hypercache_cache_hits_total %d\n", stats.Hits)
		writeHelpType("hypercache_cache_misses_total", "Total cache misses.", "counter")
		fmt.Fprintf(&b, "hypercache_cache_misses_total %d\n", stats.Misses)
		writeHelpType("hypercache_evictions_total", "Entries evicted for memory pressure.", "counter")
		fmt.Fprintf(&b, "hypercache_evictions_total %d\n", stats.Evictions)
		writeHelpType("hypercache_expired_keys_total", "Keys removed because they expired.", "counter")
		fmt.Fprintf(&b, "hypercache_expired_keys_total %d\n", stats.Expired)
	}

	if _, err := io.WriteString(out, b.String()); err != nil {
		return
	}
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
