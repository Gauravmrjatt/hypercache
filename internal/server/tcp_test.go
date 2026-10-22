package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Gauravmrjatt/hypercache/internal/cache"
	"github.com/Gauravmrjatt/hypercache/internal/metrics"
)

type testServer struct {
	srv     *Server
	addr    string
	cache   *cache.Cache
	metrics *metrics.Collector
}

func startTestServer(t *testing.T, readTimeout time.Duration) *testServer {
	t.Helper()

	c := cache.New(cache.Config{ShardCount: 4, MaxMemoryBytes: 0, EvictionPolicy: cache.EvictLRU})
	t.Cleanup(c.Stop)

	m := metrics.New(func() cache.Stats { return c.Stats() })
	handler := NewHandler(c, m, 0)
	srv := New(Config{Addr: "127.0.0.1:0", ReadTimeout: readTimeout}, handler, m)

	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(srv.ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	return &testServer{srv: srv, addr: srv.Addr().String(), cache: c, metrics: m}
}

// client is a minimal protocol client for tests.
type client struct {
	conn net.Conn
	r    *bufio.Reader
	t    *testing.T
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := &client{conn: conn, r: bufio.NewReader(conn), t: t}
	t.Cleanup(func() { _ = conn.Close() })
	return c
}

// send writes one command line and returns the full reply. Multi-line
// replies (arrays) are joined with newlines. It must only be called from the
// test goroutine; use trySend from worker goroutines.
func (c *client) send(line string) string {
	c.t.Helper()
	reply, err := c.trySend(line)
	if err != nil {
		c.t.Fatalf("send %q: %v", line, err)
	}
	return reply
}

// trySend is the goroutine-safe variant of send.
func (c *client) trySend(line string) (string, error) {
	if _, err := c.conn.Write([]byte(line + "\n")); err != nil {
		return "", err
	}
	return c.tryReadReply()
}

func (c *client) tryReadReply() (string, error) {
	first, ok := c.readLine()
	if !ok {
		return "", fmt.Errorf("connection closed waiting for reply")
	}
	if !strings.HasPrefix(first, "*") {
		return first, nil
	}
	n, err := strconv.Atoi(strings.TrimPrefix(first, "*"))
	if err != nil {
		return "", fmt.Errorf("bad array header %q: %w", first, err)
	}
	lines := make([]string, 0, n)
	for i := 0; i < n; i++ {
		line, ok := c.readLine()
		if !ok {
			return "", fmt.Errorf("connection closed after %d of %d array elements", i, n)
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n"), nil
}

// readReply reads one reply, failing the test on protocol errors.
func (c *client) readReply() string {
	c.t.Helper()
	reply, err := c.tryReadReply()
	if err != nil {
		c.t.Fatalf("read reply: %v", err)
	}
	return reply
}

// readLine reads a single line. It reports ok=false when the connection is
// closed or the read times out.
func (c *client) readLine() (string, bool) {
	_ = c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", false
	}
	return strings.TrimRight(line, "\r\n"), true
}

func TestBasicCommandFlow(t *testing.T) {
	ts := startTestServer(t, 0)
	c := dial(t, ts.addr)

	if got := c.send("PING"); got != "PONG" {
		t.Errorf("PING = %q, want PONG", got)
	}
	if got := c.send("SET user:1 Gaurav"); got != "OK" {
		t.Errorf("SET = %q, want OK", got)
	}
	if got := c.send("GET user:1"); got != "Gaurav" {
		t.Errorf("GET = %q, want Gaurav", got)
	}
	if got := c.send("GET user:2"); got != "(nil)" {
		t.Errorf("GET missing = %q, want (nil)", got)
	}
	if got := c.send("EXISTS user:1 user:2"); got != "1" {
		t.Errorf("EXISTS = %q, want 1", got)
	}
	if got := c.send("DEL user:1"); got != "1" {
		t.Errorf("DEL = %q, want 1", got)
	}
	if got := c.send("DEL user:1"); got != "0" {
		t.Errorf("DEL again = %q, want 0", got)
	}
}

func TestQuotedAndEmptyValues(t *testing.T) {
	ts := startTestServer(t, 0)
	c := dial(t, ts.addr)

	if got := c.send(`SET msg "hello world"`); got != "OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := c.send("GET msg"); got != "hello world" {
		t.Errorf("GET = %q, want hello world", got)
	}
	if got := c.send(`SET empty ""`); got != "OK" {
		t.Fatalf("SET empty = %q", got)
	}
	if got := c.send("GET empty"); got != "" {
		t.Errorf("GET empty = %q, want empty line", got)
	}
}

func TestTTLFlowOverWire(t *testing.T) {
	ts := startTestServer(t, 0)
	c := dial(t, ts.addr)

	if got := c.send("TTL nope"); got != "-2" {
		t.Errorf("TTL missing = %q, want -2", got)
	}
	if got := c.send("SET forever v"); got != "OK" {
		t.Fatalf("SET = %q", got)
	}
	if got := c.send("TTL forever"); got != "-1" {
		t.Errorf("TTL no-expiry = %q, want -1", got)
	}
	if got := c.send("SET temp v EX 60"); got != "OK" {
		t.Fatalf("SET = %q", got)
	}
	ttl, err := strconv.Atoi(c.send("TTL temp"))
	if err != nil {
		t.Fatalf("TTL reply not an integer: %q", c.send("TTL temp"))
	}
	if ttl < 59 || ttl > 60 {
		t.Errorf("TTL = %d, want 59..60", ttl)
	}
	if got := c.send("EXPIRE temp 30"); got != "1" {
		t.Errorf("EXPIRE = %q, want 1", got)
	}
	if got := c.send("EXPIRE nope 30"); got != "0" {
		t.Errorf("EXPIRE missing = %q, want 0", got)
	}
	if got := c.send("EXPIRE temp abc"); got != "ERR value is not an integer or out of range" {
		t.Errorf("EXPIRE bad integer = %q", got)
	}
}

func TestErrorReplies(t *testing.T) {
	ts := startTestServer(t, 0)
	c := dial(t, ts.addr)

	tests := []struct {
		line string
		want string
	}{
		{"BOGUS", "ERR unknown command 'bogus'"},
		{"GET", "ERR wrong number of arguments for 'get' command"},
		{"SET onlykey", "ERR wrong number of arguments for 'set' command"},
		{"TTL", "ERR wrong number of arguments for 'ttl' command"},
		{`SET k "unterminated`, "ERR unterminated quoted string"},
		{"SET k v EX", "ERR syntax error"},
		{"SET k v ZZ 1", "ERR unsupported option"},
		{"SET k v EX 0", "ERR invalid expire time in 'set' command"},
	}

	for _, tt := range tests {
		if got := c.send(tt.line); got != tt.want {
			t.Errorf("%q => %q, want %q", tt.line, got, tt.want)
		}
	}
}

func TestBlankLineIsIgnored(t *testing.T) {
	ts := startTestServer(t, 0)
	c := dial(t, ts.addr)

	if _, err := c.conn.Write([]byte("\n\nPING\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := c.readReply(); got != "PONG" {
		t.Errorf("reply = %q, want only PONG", got)
	}
}

func TestKeysAndStatsCommands(t *testing.T) {
	ts := startTestServer(t, 0)
	c := dial(t, ts.addr)

	for _, key := range []string{"b", "a", "c"} {
		if got := c.send("SET " + key + " v"); got != "OK" {
			t.Fatalf("SET %s = %q", key, got)
		}
	}
	if got := c.send("KEYS"); got != "a\nb\nc" {
		t.Errorf("KEYS = %q, want a\\nb\\nc", got)
	}
	if got := c.send("KEYS a*"); got != "a" {
		t.Errorf("KEYS a* = %q, want a", got)
	}

	stats := c.send("STATS")
	if !strings.Contains(stats, "shards=4") {
		t.Errorf("STATS missing shards=4:\n%s", stats)
	}
	if !strings.Contains(stats, "keys=3") {
		t.Errorf("STATS missing keys=3:\n%s", stats)
	}
	if !strings.Contains(stats, "cmd_ping_total=") {
		t.Errorf("STATS missing per-command counters:\n%s", stats)
	}
}

func TestShardedSetGetFromMultipleConnections(t *testing.T) {
	ts := startTestServer(t, 0)

	const (
		clients = 16
		ops     = 200
	)

	// Dial from the test goroutine so failures are reported correctly.
	conns := make([]*client, clients)
	for i := range conns {
		conns[i] = dial(t, ts.addr)
	}

	var wg sync.WaitGroup
	errs := make(chan string, clients)

	for i, c := range conns {
		wg.Add(1)
		go func(id int, c *client) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				key := fmt.Sprintf("c%d:key%d", id, j)
				value := fmt.Sprintf("val%d", j)
				if reply, err := c.trySend("SET " + key + " " + value); err != nil || reply != "OK" {
					errs <- fmt.Sprintf("SET %s = %q, %v", key, reply, err)
					return
				}
				if reply, err := c.trySend("GET " + key); err != nil || reply != value {
					errs <- fmt.Sprintf("GET %s = %q, want %s (%v)", key, reply, value, err)
					return
				}
			}
		}(i, c)
	}
	wg.Wait()
	close(errs)

	for msg := range errs {
		t.Error(msg)
	}
	if t.Failed() {
		return
	}

	if stats := ts.cache.Stats(); stats.Keys != clients*ops {
		t.Fatalf("keys = %d, want %d", stats.Keys, clients*ops)
	}
}

func TestShutdownClosesConnections(t *testing.T) {
	ts := startTestServer(t, 0)
	c := dial(t, ts.addr)

	if got := c.send("PING"); got != "PONG" {
		t.Fatalf("PING = %q", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := ts.srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// The open connection should be closed by Shutdown.
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.r.ReadString('\n'); err == nil {
		t.Fatal("expected the connection to be closed after shutdown")
	}

	// New connections must be refused.
	if conn, err := net.DialTimeout("tcp", ts.addr, time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("expected new connections to be refused after shutdown")
	}
}

func TestReadTimeoutClosesIdleConnections(t *testing.T) {
	ts := startTestServer(t, 150*time.Millisecond)
	c := dial(t, ts.addr)

	if got := c.send("PING"); got != "PONG" {
		t.Fatalf("PING = %q", got)
	}

	_ = c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.r.ReadString('\n'); err == nil {
		t.Fatal("expected idle connection to be closed by the read timeout")
	}
}
