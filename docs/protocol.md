# HyperCache MVP Protocol

HyperCache uses a line-based text protocol over TCP so that `nc`, `telnet` and
small scripts can drive the server during development.

A RESP implementation is planned later. It fixes the newline-in-values
limitation below and lets `redis-cli` and `redis-benchmark` run against
HyperCache directly.

## Transport

- TCP, default `127.0.0.1:6380`.
- One command per line, terminated by `\n` (a trailing `\r` is ignored).
- One reply per command, always terminated by `\n`.
- Blank lines are ignored and produce no reply.
- The maximum command line size is 1 MiB.

## Request grammar

```text
command [arg ...]
```

- The first token is the command name; matching is case-insensitive
  (`set`, `Set` and `SET` are identical).
- Arguments are separated by spaces or tabs.
- Double quotes group an argument that contains whitespace.
- Inside quotes the escapes `\"`, `\\`, `\n`, `\t` and `\r` are recognised.
- Quotes in the middle of a token concatenate with the surrounding text:
  `ab"cd"ef` becomes `abcdef`.
- An empty quoted string `""` produces an empty argument.

Examples:

```text
SET user:1 Gaurav
SET greeting "hello world"
SET banner "line\nbreak"
DEL user:1 user:2 user:3
```

## Reply forms

| Form | Example | Meaning |
|---|---|---|
| Simple status | `OK` | Successful mutation. |
| Status | `PONG` | Reply to `PING`. |
| Error | `ERR unknown command 'bogus'` | The command could not be executed. |
| Nil | `(nil)` | `GET` on a missing or expired key. |
| Integer | `59` | Bare decimal integer, may be negative. |
| Bulk value | `Gaurav` | Raw value bytes on their own line. |
| Array | `*3` then three lines | Count header followed by that many lines. |

Every command produces exactly one reply.

## Commands

| Command | Reply | Notes |
|---|---|---|
| `PING` | `PONG` | Liveness check, no arguments. |
| `SET key value [EX s \| PX ms]` | `OK` or error | `EX`/`PX` set the TTL; without them the configured default TTL applies (0 = none). `EX`/`PX` must be positive. |
| `GET key` | value, `(nil)` | Misses include expired keys. |
| `DEL key [key ...]` | integer | Number of live keys removed. |
| `EXISTS key [key ...]` | integer | Number of live keys present. |
| `EXPIRE key seconds` | `1` / `0` | Non-positive values delete the key and still reply `1` if it existed. |
| `TTL key` | integer | `-2` missing or expired, `-1` no expiry, otherwise remaining seconds rounded up. |
| `KEYS [pattern]` | array | Glob pattern via Go `path.Match` (`*`, `?`, `[...]`). Debug use only. |
| `STATS` | array | `key=value` lines: uptime, keys, memory, hits, misses, hit ratio, evictions, expired keys, command totals, latency percentiles, and per-shard counters. |

### Error replies

| Reply | Cause |
|---|---|
| `ERR unknown command 'x'` | Command not implemented. |
| `ERR wrong number of arguments for 'x' command` | Arity check failed. |
| `ERR value is not an integer or out of range` | An integer argument could not be parsed. |
| `ERR syntax error` | Incomplete option list, for example `SET k v EX`. |
| `ERR unsupported option` | Unknown `SET` option. |
| `ERR invalid expire time in 'set' command` | `EX`/`PX` value was zero or negative. |
| `ERR out of memory` | The shard memory budget cannot hold the entry. |
| `ERR unterminated quoted string` | A `"` was never closed. |
| `ERR invalid escape sequence: \x` | Unknown escape inside quotes. |
| `ERR command line too large` | Line exceeded 1 MiB. |

## Example session

```text
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

EXPIRE user:1 30
1

EXISTS user:1 user:missing
1

DEL user:1
1

KEYS
*1
session:9

GET user:1
(nil)

STATS
*23
uptime_seconds=14
shards=16
...
```

## Known limitations of this version

1. **Newlines in values.** Bulk replies are a single line, so a value that
   contains a raw `\n` byte breaks reply framing. The quoting escapes make it
   possible to *store* such a value, but reading it back will span two lines.
   RESP support removes this limitation.
2. **No authentication or TLS.** Bind to trusted interfaces only.
3. **`KEYS` scans every shard.** Use it for debugging, not in production paths.
4. **No client-side pipelining guarantees.** Replies are sent in order per
   connection, but there is no request ID to correlate pipelined commands.
5. **Blocking-free by design.** There are no blocking commands (`BLPOP` and
   friends) and therefore no client-side latency amplification.
