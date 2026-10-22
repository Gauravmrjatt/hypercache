# HyperCache

An in-memory key-value cache written in Go, built as a final-year systems
project. The point of the project is not to replace Redis, but to build one
from scratch in order to understand the parts that make Redis what it is:
sharding, TTL expiry, eviction, a wire protocol, metrics, and eventually
replication and failure handling.

In one sentence: HyperCache is a single-node TCP server that accepts simple
text commands (`SET`, `GET`, ...), spreads the keyspace over independent
in-memory shards, expires keys by TTL, evicts under memory pressure, and
reports its own state as Prometheus metrics.

Right now HyperCache is a working single-node server. The next steps are
replication and failover, then persistence, then hot-key/adaptive features and
a Redis comparison. The status section near the bottom keeps track of what is
actually done.

## Running it

Go 1.26+ required.

```bash
go run ./cmd/hypercache                       # defaults: 127.0.0.1:6380, metrics on 9090
go run ./cmd/hypercache --config configs/hypercache.yaml
go run ./cmd/hypercache --addr 127.0.0.1:7000 --shards 32 --max-memory-mb 256 --eviction lru
go run ./cmd/hypercache --version
```

The protocol is plain text, so `nc` is enough to play with it:

```text
$ nc 127.0.0.1 6380
PING
PONG

SET user:1 Gaurav
OK

GET user:1
Gaurav

SET session:9 "active session" EX 60
OK

TTL session:9
60
```

A fuller command reference (all reply forms and error messages) is in
[docs/protocol.md](docs/protocol.md).

## What is implemented

| Command | What it does |
|---|---|
| `PING` | replies `PONG` |
| `SET key value [EX s \| PX ms]` | stores a key, optional TTL |
| `GET key` | returns the value or `(nil)` |
| `DEL key [key ...]` | deletes keys, returns how many |
| `EXISTS key [key ...]` | counts live keys |
| `EXPIRE key seconds` | adds/changes the TTL (<= 0 deletes) |
| `TTL key` | -2 missing, -1 no expiry, else seconds left |
| `KEYS [pattern]` | glob filter, debug use only |
| `STATS` | keys, memory, hit rate, evictions, latency percentiles, per-shard counters |

Non-command features:

- one goroutine per connection, one mutex per shard (16 by default)
- keys expire lazily on access and get swept every second
- per-shard memory accounting; either LRU eviction or reject-writes-when-full
  (`eviction_policy: none|lru`)
- Prometheus `/metrics` endpoint plus `/healthz` on a second port
- YAML config + flag overrides, shutdown on Ctrl-C / SIGTERM

## Architecture

Everything runs in one process. The client port (6380) and the metrics port
(9090) are separate listeners.

```
        client (nc, telnet, app code, tests)
                       |
                       | TCP 127.0.0.1:6380, line-per-command text protocol
                       v
+---------------------------------------------------------------+
| server (internal/server)                                      |
|   accept loop            -> one goroutine per connection       |
|   read line              -> tokenizer -> protocol.Command      |
|   dispatch               -> handler for that command           |
|   write reply            -> buffered, flushed once per command |
+---------------------------------------------------------------+
                       |
                       | route: shardIndex = fnv1a(key) % shardCount
                       v
+---------------------------------------------------------------+
| cache (internal/cache)                                        |
|                                                               |
|  shard 0        shard 1             shard 15                  |
|  +---------+    +---------+         +---------+               |
|  | map     |    | map     |         | map     |               |
|  | LRU list|    | LRU list|   ...   | LRU list|               |
|  | mutex   |    | mutex   |         | mutex   |               |
|  +---------+    +---------+         +---------+               |
|                                                               |
|  every Entry: value, ExpiresAt, LastAccess,                   |
|               AccessCount, Version, estimated Size            |
+---------------------------------------------------------------+
                       |
                       v
+---------------------------------------------------------------+
| background + observability                                    |
|   janitor goroutine        - sweeps expired keys every 1s      |
|   metrics collector        - command counters, latency hist.,  |
|                              hits/misses, evictions, conns     |
|                              -> HTTP :9090 /metrics + /healthz |
|   SIGINT/SIGTERM handler   - stops accepting, drains conns     |
+---------------------------------------------------------------+
```

The two listeners share the same collector, so `STATS` over TCP and
`/metrics` over HTTP show the same numbers.

### What happens on SET

```
SET session:9 "active session" EX 60

1. read the line, tokenize          -> Command{SET, [session:9, active session, EX, 60]}
2. parse the options                -> ttl = 60s
3. hash the key                     -> fnv1a("session:9") % 16 = shard 7
4. lock shard 7
5. new Entry                        -> ExpiresAt = now + 60s,
                                       Size = len(key)+len(value)+96
6. add size to the shard total
7. push Entry to the LRU front
8. while shard over budget: pop LRU back, subtract its size,
   count an eviction (with policy "none": reject instead, reply ERR)
9. unlock
10. reply OK
```

### What happens on GET

```
GET session:9

1..3. same parse and routing, lands on the same shard 7
4. lock shard 7
5. lookup: missing?                                -> unlock, reply (nil)
          expired (now >= ExpiresAt)?              -> delete, unlock, reply (nil)
          live                     -> LastAccess = now,
                                      AccessCount++,
                                      move to LRU front,
                                      count the hit (misses are counted
                                      on the missing/expired paths)
6. unlock
7. write the raw value bytes + newline
```

Step 5 is why expiry needs no background work to be correct for reads: the
entry cannot be answered as live after its deadline, because every read
re-checks the deadline under the shard lock. The janitor only exists so
expired keys nobody reads stop occupying memory.

### Why the shards matter

The point of the whole `hash -> shard` step is that a `GET h` on shard 3 never
has to wait for a mutex held by `GET a` on shard 1. In the architecture above
the only shared structures between connections are the metrics counters
(atomic) and the listener. Everything else on the hot path is per shard.
That is what the benchmark in the section below measures: 16 shards serve
parallel reads roughly 3x faster than one shard on the same hardware.

## Design notes

These are the decisions worth remembering, and why.

**Sharding.** Keys map to shards with plain FNV-1a: `hash(key) % shardCount`.
Each shard owns a `map[string]*Entry` guarded by its own mutex, so unrelated
keys never contend. I first considered one global lock or an actor-per-shard
model; a global mutex serialises everything, an actor loop means queues and
more plumbing, and per-shard locks get both properties for free at this scale.
The downside shows up with a single hot key: it always lands on one shard no
matter how many shards we configure (this is what the hot-key experiments
should look at later).

**TTL.** The expiry time is stored on the entry (`ExpiresAt`). Reads check it
and delete the entry if it passed, and a janitor sweeps every second so
expired-but-untouched memory gets freed too. This is the classic lazy + active
combination. One trade-off: with `eviction_policy: none`, expired keys still
hold memory until they are read or swept, which can reject a write that would
have fit a moment later. Tests cover this case specifically.

**Memory accounting.** Everything is an estimate: `len(key) + len(value) + 96`
bytes. The 96 bytes stand in for the map slot, list element and struct
overhead. Real memory is higher (Go's allocator, garbage collector), but the
estimate is stable and good enough to drive eviction. The budget is split
evenly across shards, which means one shard can fill up while others still
have room; correct across the whole cache but uneven per shard.

**Eviction.** LRU via a per-shard `container/list`: every read and write moves
the entry to the front, eviction pops from the back until the shard is inside
the budget. `none` is the other option, useful for tests because a rejected
write is a clearer failure than a silent eviction. LFU and the "adaptive"
variants are future work, behind the same interface.

**Protocol.** Line-based text so it can be driven by `nc`/`telnet` during development.
Known wart: values containing a raw newline still get stored, and reading them
back breaks the framing of the reply. Redis solves this with
a length-prefixed bulk type; a RESP implementation is the intended fix and
would also let `redis-benchmark` drive HyperCache directly, which matters for
the comparison experiments.

## Config

`configs/hypercache.yaml` with comments; any field you omit keeps the
default. Flags (e.g. `--shards`) override the file.

```yaml
server:
  host: "127.0.0.1"
  port: 6380
  read_timeout_seconds: 0     # close idle connections; 0 disables

cache:
  shard_count: 16
  max_memory_mb: 512          # 0 disables the limit
  eviction_policy: "lru"      # or "none"
  default_ttl_seconds: 0      # applied by SET when EX/PX is absent
  cleanup_interval_seconds: 1

observability:
  metrics_enabled: true
  metrics_port: 9090
```

## Benchmarks

`go test -bench=. ./internal/cache/` (Apple M2, 500 ms per case, not tuned):

```
BenchmarkGetHitParallel-8      7.7M ops    83 ns/op
BenchmarkSetParallel-8         5.1M ops   144 ns/op
BenchmarkGetHit-8              4.5M ops   136 ns/op
BenchmarkSetGet-8              2.8M ops   206 ns/op
BenchmarkSingleShardGetHit-8   2.4M ops   260 ns/op
BenchmarkSetEvict-8            2.3M ops   254 ns/op
```

The single-shard row is the baseline to beat: 16 shards cut parallel read
latency by roughly 3x. These numbers are only for the storage layer though.
The real experiment (still pending) is the same workload end-to-end through
the TCP server compared against Redis.

## Project status

Done:

- storage engine, sharding, TTL, eviction
- TCP server and text protocol
- STATS + Prometheus metrics
- config handling, graceful shutdown
- tests (unit, integration, concurrency, race) and storage benchmarks

Not done yet, in the order I plan to do it:

- replication first (async primary -> replica, then heartbeat/failover)
- snapshot + WAL persistence
- hot-key detection and read routing
- protocol switch to RESP
- network benchmark tool (SET/GET mix, Zipfian keys) and Redis comparison

## Repository layout

```
cmd/hypercache/     main: flags, config wiring, shutdown
internal/cache/     shards, entries, eviction, janitor (+ tests + benchmarks)
internal/protocol/  command tokenizer and reply writer
internal/server/    TCP listener, connection loop, command dispatch
internal/config/    YAML config, defaults, validation
internal/metrics/   counters, latency histogram, Prometheus output
configs/            example config
docs/               protocol spec
```

## Tests

```bash
go test ./...                      # everything
go test -race ./...                # race detector, run this after touching concurrency
go test -bench=. ./internal/cache/ # storage benchmarks
```

Tests cover the parser (quoting, escapes, malformed input), cache semantics
(set/get/del, expiry with an injected clock, LRU order, budget limits),
protocol behaviour over a real socket (error replies, idle timeout, shutdown),
and the Prometheus output format. `TestConcurrentAccess` hammers a cache with
16 goroutines doing mixed reads/writes/deletes/expiry; run it under `-race`.

## Known limitations

- single node only; no replication, persistence or auth
- the text protocol framing breaks on values with newlines (see Design notes)
- the shard memory budget is per shard, not global, so hot shards fill first
- `KEYS` scans everything, it is a debug command
- no TLS, bind to localhost or a trusted network

## Demo ideas for the presentation

1. basic session with nc: SET/GET, TTL expiring live, DEL
2. two servers with different shard counts under `hey`/custom loadgen, showing
   per-shard spread in STATS
3. eviction demo: 1 MiB budget, write past it, watch `evictions=` climb
4. dashboard: run load while `/metrics` is scraped by Prometheus/Grafana
