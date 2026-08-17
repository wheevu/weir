# Architecture

The core has no runtime dependencies.

`Parser` accepts arbitrary chunks and emits only complete, checksum-valid frames. A frame with an oversized length field poisons only that parser (its `bad()` flag), never the server; valid frames decoded before it are still returned.

`BoundedQueue` applies backpressure and closes cleanly.

`Pipeline` places accepted events on a persistence queue before append and only then places them on the processing queue. A test-only `WEIR_TEST_ACK_DELAY_MS` environment variable can delay ACK delivery to make async-lifetime races deterministic in integration tests; it has no effect in normal operation.

`Log::replay` scans valid frames from the append-only file and ignores incomplete trailing data.

## Transport and connection lifecycle

The server runs a single epoll thread that owns all connection state. There is no per-connection thread and no shared mutable connection data.

Each connection keeps:

```text
Parser    - incremental frame state
out       - pending outbound ACK/ERR bytes
gen       - monotonic identity, never reused
read_open - false once the peer's write side is closed (EOF)
pending   - persistence completions still outstanding
```

### Socket ability vs connection lifetime

Three facts are tracked separately:

- `EPOLLRDHUP`/`EPOLLHUP` means the peer stopped sending. If readable data is still pending, it is drained first; `recv() == 0` is what actually records EOF.
- A peer write-half close does not disable the outbound path: earned ACKs for already-admitted events are still delivered.
- The connection object is destroyed only when `!read_open && pending == 0 && out.empty()`. A half-closed connection therefore lingers only as long as its own responses take to deliver, and then closes without leaking.

`EPOLLERR` is treated as immediate teardown: the socket is broken.

### Epoll interest mask

The mask is derived from state on every change, through one helper:

```text
EPOLLRDHUP | (read_open ? EPOLLIN : 0) | (!out.empty() ? EPOLLOUT : 0)
```

`EPOLLOUT` is armed only while outbound bytes exist, so idle connections never busy-loop the loop. Nonblocking `send()` may write fewer bytes than requested; the remainder stays in `out` and the mask keeps `EPOLLOUT` until the drain completes.

### Asynchronous completions

Persistence runs on the pipeline thread; completions are marshalled to the epoll thread through a mutex-guarded queue plus an eventfd wake. Each completion carries `(fd, gen, id, ok)`. On delivery the epoll thread checks that the connection still exists and that its generation matches. Since generations are monotonic and never reused, a stale completion can never reach a later connection that happens to reuse the same numeric fd.

### Teardown paths

Every path (client close, half-close drain, malformed frame, fatal socket error, shutdown) funnels through a single `drop()` that removes the epoll registration, closes the fd, and erases the connection exactly once. Malformed input from one client can only poison that client's parser; the server and all other connections are unaffected.
