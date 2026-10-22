package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Gauravmrjatt/hypercache/internal/cache"
)

func TestCollectorCounters(t *testing.T) {
	c := New(nil)

	c.IncCommand("GET")
	c.IncCommand("GET")
	c.IncCommand("SET")
	c.IncCommand("NOT_A_KNOWN_COMMAND")
	c.IncParseError()
	c.ConnOpened()
	c.ConnOpened()
	c.ConnClosed()
	c.ObserveLatency(200 * time.Microsecond)
	c.ObserveLatency(2 * time.Millisecond)

	snap := c.Snapshot()
	if snap.Commands["GET"] != 2 {
		t.Errorf("GET = %d, want 2", snap.Commands["GET"])
	}
	if snap.Commands["SET"] != 1 {
		t.Errorf("SET = %d, want 1", snap.Commands["SET"])
	}
	if snap.Commands["OTHER"] != 1 {
		t.Errorf("OTHER = %d, want 1", snap.Commands["OTHER"])
	}
	if _, ok := snap.Commands["NOT_A_KNOWN_COMMAND"]; ok {
		t.Error("raw unknown command names must not create counter entries")
	}
	if snap.CommandsTotal != 4 {
		t.Errorf("CommandsTotal = %d, want 4", snap.CommandsTotal)
	}
	if snap.ParseErrors != 1 {
		t.Errorf("ParseErrors = %d, want 1", snap.ParseErrors)
	}
	if snap.ActiveConns != 1 || snap.TotalConns != 2 {
		t.Errorf("conns = %d active / %d total, want 1 / 2", snap.ActiveConns, snap.TotalConns)
	}
	if snap.AvgLatency <= 0 || snap.MaxLatency < 2*time.Millisecond {
		t.Errorf("latency avg=%v max=%v", snap.AvgLatency, snap.MaxLatency)
	}
	if snap.P50Latency <= 0 || snap.P99Latency < snap.P50Latency {
		t.Errorf("quantiles p50=%v p99=%v", snap.P50Latency, snap.P99Latency)
	}
}

func TestQuantileNeverExceedsObservedMax(t *testing.T) {
	c := New(nil)
	for i := 0; i < 20; i++ {
		c.ObserveLatency(50 * time.Microsecond)
	}
	snap := c.Snapshot()
	if snap.P99Latency > snap.MaxLatency {
		t.Errorf("p99 = %v exceeds max = %v", snap.P99Latency, snap.MaxLatency)
	}
	if snap.MaxLatency != 50*time.Microsecond {
		t.Errorf("max = %v, want 50µs", snap.MaxLatency)
	}
}

func TestServeHTTPExportsPrometheusFormat(t *testing.T) {
	c := cache.New(cache.Config{ShardCount: 2, EvictionPolicy: cache.EvictLRU})
	t.Cleanup(c.Stop)
	_ = c.Set("a", []byte("1"), 0)
	_, _ = c.Get("a")
	_, _ = c.Get("missing")

	collector := New(func() cache.Stats { return c.Stats() })
	collector.IncCommand("GET")
	collector.ObserveLatency(time.Millisecond)

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	collector.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Errorf("content type = %q", ct)
	}

	body := rec.Body.String()
	wants := []string{
		"# TYPE hypercache_commands_total counter",
		`hypercache_commands_total{cmd="GET"} 1`,
		"hypercache_parse_errors_total 0",
		"hypercache_command_duration_seconds_bucket{le=\"+Inf\"} 1",
		"hypercache_connections_active 0",
		"hypercache_cache_hits_total 1",
		"hypercache_cache_misses_total 1",
		"hypercache_keys 1",
		"hypercache_shards 2",
		"hypercache_uptime_seconds ",
	}
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q\n%s", want, body)
		}
	}
}

func TestServeHTTPWithoutCacheStats(t *testing.T) {
	collector := New(nil)
	rec := httptest.NewRecorder()
	collector.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	body := rec.Body.String()
	if strings.Contains(body, "hypercache_keys") {
		t.Error("cache series should be omitted when no cache provider is configured")
	}
	if !strings.Contains(body, "hypercache_commands_total") {
		t.Error("command series should always be exported")
	}
}

func TestHistogramRecordsPerBucketCounts(t *testing.T) {
	h := newHistogram()
	h.observe(150 * time.Microsecond) // lands in le=0.00025
	h.observe(2 * time.Millisecond)   // lands in le=0.0025
	h.observe(10 * time.Second)       // beyond the last bucket: +Inf only

	if h.total.Load() != 3 {
		t.Fatalf("total = %d, want 3", h.total.Load())
	}
	if got := h.counts[1].Load(); got != 1 { // le=0.00025
		t.Errorf("le=0.00025 count = %d, want 1", got)
	}
	if got := h.counts[4].Load(); got != 1 { // le=0.0025
		t.Errorf("le=0.0025 count = %d, want 1", got)
	}
	if got := h.counts[len(h.counts)-1].Load(); got != 0 { // le=5
		t.Errorf("le=5 count = %d, want 0 (overflow goes to +Inf)", got)
	}
	if h.maxNano.Load() != uint64(10*time.Second) {
		t.Errorf("max = %d", h.maxNano.Load())
	}
	if h.mean() != time.Duration((150*time.Microsecond+2*time.Millisecond+10*time.Second)/3) {
		t.Errorf("mean = %v", h.mean())
	}
}
