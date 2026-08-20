# Weir

Weir is a small Linux-first C++20 event-ingestion service.

[![CI](https://github.com/wheevu/weir/actions/workflows/linux.yml/badge.svg)](https://github.com/wheevu/weir/actions/workflows/linux.yml)

![Weir architecture](docs/ARCHITECTURE.svg)

It runs one of three nonblocking TCP transports: a legacy `epoll` server, a C++20 coroutine server over `epoll`, and the same coroutine server over `io_uring` poll mode. Events arrive as length-delimited `WR01` frames, are appended and flushed to an append-only log before acknowledgement, then processed by worker threads. Metrics are served as Prometheus text over a local HTTP endpoint.

Stable-storage durability and crash-recovery guarantees are still under development.

## Quickstart

Requirements: CMake 3.20+ and a C++20 compiler.

```sh
./scripts/build.sh default
```

Run the server and a client:

```sh
./build-make/weir-server --port 9000 --log events.wrl
./build-make/weir-server --port 9000 --log events.wrl --backend io_uring
./build-make/weir-producer 127.0.0.1 9000 hello
./build-make/weir-inspect-log events.wrl
./build-make/weir-replay events.wrl
```

Prometheus output is served on `--metrics-port`.

Linux provides the nonblocking TCP server (epoll, coroutine, or io_uring backend) on port 9000. macOS builds the core and CLI tools, but the server reports that networking is unavailable.

## What it demonstrates

- Linux networking with nonblocking TCP across three backends: legacy `epoll`, C++20 coroutines over `epoll`, and coroutines over `io_uring` (`--backend epoll|coroutine|io_uring`)
- Incremental framing over a byte stream
- Bounded concurrent queues with explicit capacity limits
- Persistence before processing acknowledgement
- Worker ownership and coordinated shutdown
- Append-only recovery and replay
- Prometheus-compatible metrics and structured logs

## Benchmarks

Measured 2026-08-20 on the Lima VM: Ubuntu 24.04 ARM64, kernel 7.0.0-28-generic, 4 vCPU, 6 GiB, g++ 15.2, git ffd5174.
Campaign: 3 backends x 4 connection levels x 3 samples, payload 1024 B, window 32, 5 s warmup, 30 s per sample.
Raw JSON, environment metadata, and the bench binary checksum are in `results/bench/`; full results and reading in [docs/IOURING-SHOOTOUT.md](docs/IOURING-SHOOTOUT.md).

Throughput (accepted events/s, mean of 3 samples):

| connections | epoll | coroutine | io_uring |
|------------:|------:|----------:|---------:|
| 1,000 | 149,895 | 140,244 | 147,389 |
| 5,000 | 137,232 | 125,695 | 127,006 |
| 10,000 | 118,600 | 107,159 | 117,947 |
| 20,000 | 121,833 | 108,476 | 98,575 |

The durable persistence pipeline (one log write per accepted event) saturates first, so throughput is transport-boundary-independent. The transport choice shows up in admission, memory, and latency:

- io_uring rejects fewer events than epoll or coroutine at every connection level (34-42% vs 48-53%).
- io_uring uses the least memory: about 6.5 KB per connection at 20,000 connections versus 8.4 KB for epoll and 8.6 KB for coroutine.
- With the original 256-slot ring, io_uring paid a multi-millisecond latency tail; sizing the ring to the connection scale (default 4096) brought p999 at 1,000 connections from 2.34 ms to 0.28 ms, at epoll's level. The 20,000-connection tail is a structural property of slot-based polling and is still being investigated.

Caveats: samples are 30 s, not the three five-minute samples the [benchmark protocol](docs/BENCHMARK.md) asks for; the 10,000- and 20,000-connection rows were verified on battery power with host memory pressure, which throttles absolute numbers about 2x; absolute numbers reflect the fsync-free log. The numbers compare backends on this VM, not absolute throughput.

## Verified on Linux

- GCC 13.3 and Clang 18.1 with strict warnings
- Real nonblocking TCP/epoll transport (syscalls confirmed with `strace`)
- C++20 coroutine server: single run loop, awaitable sockets, no blocking calls
- `io_uring` poll-mode transport with configurable ring size and seeded failpoint stress (CI, 20 seeds)
- Fragmented and coalesced TCP frames
- TCP half-close with pending asynchronous ACKs
- Partial nonblocking response writes
- Concurrent clients and malformed-client isolation
- Metrics HTTP endpoint on 127.0.0.1
- Linux process/socket integration tests
- ASan/UBSan and Valgrind-clean tested paths

Still in progress: `fdatasync` durability, process-level crash recovery, Prometheus/Grafana deployment, full observability, and the five-minute publication-protocol benchmark runs.

## Event lifecycle

![Weir event lifecycle](docs/LIFECYCLE.svg)

The lifecycle is validated, queued, appended and flushed, acknowledged, queued for processing, and handled by workers.
The append step uses userspace `flush()`, not `fdatasync()`.

## Overview

1. A TCP client sends length-delimited `WR01` frames.
2. The parser tolerates arbitrary read fragmentation and rejects bad checksums.
3. Valid events enter a bounded durability queue, are appended and flushed to an append-only log, then enter the processing queue.
4. Worker threads process only events that were successfully appended and flushed.
5. Weir exposes Prometheus-compatible metrics over a local HTTP endpoint on port 9100 by default.

The binary frame is: magic `WR01`, big-endian event id, big-endian payload length, payload, and FNV-1a checksum.

The service is intentionally single-process and has no runtime dependency beyond libc and pthreads.

Linux runtime integration tests are disabled by default because the server uses Linux-specific networking APIs (`epoll`, `io_uring`).
Enable them on Linux with `-DWEIR_LINUX_INTEGRATION_TESTS=ON` when configuring CMake, then run `ctest --preset default` to include real-socket transport scenarios (fragmentation, half-close, fd reuse, slow readers, malformed-client isolation).

Use `cmake --preset asan` for AddressSanitizer and UBSan.

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md): structure overview and milestone scope.
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md): the `WR01` wire format and framing.
- [`docs/PERSISTENCE.md`](docs/PERSISTENCE.md): append-only log, flush, and recovery.
- [`docs/FAILURES.md`](docs/FAILURES.md): failure behavior and error handling.
- [`docs/BENCHMARK.md`](docs/BENCHMARK.md): the repeatable benchmark protocol.
- [`docs/IOURING-SHOOTOUT.md`](docs/IOURING-SHOOTOUT.md): backend comparison results and the latency-tail investigation.