# Benchmark method

This document defines the repeatable benchmark protocol for the epoll, coroutine, and future io_uring backends.

## Measurements

- Sustained concurrent connections at 1,000, 5,000, 10,000, and 20,000 clients.
- Completed protocol events per second.
- Round-trip latency at p50, p99, and p999.
- Resident memory delta divided by established connections.
- Connection churn rate and rejected events.

Latency must be recorded in an HDR histogram.
Arithmetic averages are not a substitute for percentile data.

## Run rules

1. Run on the Linux VM with a fixed vCPU, memory, kernel, compiler, and governor configuration.
2. Raise the file descriptor limit before starting either server or client.
3. Warm each backend for 30 seconds before recording data.
4. Record three five-minute samples per connection level.
5. Use the same payload, protocol operation rate, client process, and persistence configuration for every backend.
6. Save raw histogram data, server configuration, compiler version, kernel version, and Git revision beside every result.

Do not compare a tuned backend with an untuned backend.
Do not publish a result when a run has dropped connections or silently rejected events without recording those outcomes.

## Current status

All three backends (epoll, coroutine, io_uring) pass the full integration suite on Linux, including the slow-reader scenario, seeded io_uring failpoint stress, and ASan/UBSan runs.
The comparative campaign has been measured once (2026-08-20): 3 backends x 4 connection levels x 3 samples, 30 s each with a 5 s warmup, payload 1024 B, window 32, on Ubuntu 24.04 ARM64 (kernel 7.0.0-28, g++ 15.2, git ffd5174).
Raw JSON, environment metadata, and the bench binary checksum are saved in `results/bench/`; results and interpretation are in `docs/IOURING-SHOOTOUT.md`.

The campaign run deviates from the publication rules above in two ways: samples are 30 s (rule 4 asks for three five-minute samples), and the warmup is 5 s (rule 3 asks for 30 s).
The 30 s samples are consistent across all three replicates per cell, but the five-minute protocol remains the bar for publishing absolute numbers.
Rule 4 is currently infeasible at the campaign's payload: the persistence log grows 1022 bytes per accepted event, so one five-minute sample writes 29-46 GB against ~26 GB of usable VM disk (see the shootout's known limitations).
Run rules 1, 2, 5, and 6 were followed: single fixed VM configuration, raised fd limit, identical payload/client/persistence for every backend, and full provenance saved beside the results.

After the campaign, the io_uring latency tail was traced to the transport's fixed 256-entry ring and fixed by sizing it to the connection scale (default 4096, `WEIR_URING_RING_SIZE` override).
Verified by A/B at 1,000 and 5,000 connections (p999 2.34 to 0.28 ms and 8.2 to 3.1 ms) with the full integration suite and seeded failpoint stress green.
The 10,000- and 20,000-connection rows still need a clean re-run: the verification ran on battery power with host memory pressure, which throttles absolute numbers about 2x.
Re-runs must be on AC power with a quiet host; evidence in `results/bench/ring-ab.md`.
