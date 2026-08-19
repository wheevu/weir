# io_uring versus epoll

This is the writeup scaffold for the Weir v2 backend comparison.

## Claim boundary

The comparison will report measured throughput, p50 latency, p99 latency, p999 latency, and memory per connection.
It will not claim that io_uring is universally faster.

The server protocol and persistence pipeline must remain identical between backends.
Only the transport implementation may vary.

## Results

Measured 2026-08-20 on the Lima VM (Ubuntu 24.04, kernel 7.0.0-28-generic, ARM64, 4 vCPU, 6 GiB, g++ 15.2, git ffd5174).
Campaign: 3 backends x 4 connection levels x 3 samples, payload 1024 B, window 32, 5 s warmup, 30 s per sample.
Raw JSON per sample, environment metadata, and the bench binary checksum are in `results/bench/`.
Every run: zero failed connections, server exit code 0.

Throughput (accepted events/s, mean of 3 samples):

| connections | epoll | coroutine | io_uring |
|------------:|------:|----------:|---------:|
| 1,000 | 149,895 | 140,244 | 147,389 |
| 5,000 | 137,232 | 125,695 | 127,006 |
| 10,000 | 118,600 | 107,159 | 117,947 |
| 20,000 | 121,833 | 108,476 | 98,575 |

Latency p50 (us, mean):

| connections | epoll | coroutine | io_uring |
|------------:|------:|----------:|---------:|
| 1,000 | 103,525 | 104,268 | 109,860 |
| 5,000 | 566,056 | 567,192 | 568,852 |
| 10,000 | 1,271,573 | 1,305,128 | 1,385,868 |
| 20,000 | 2,440,386 | 2,512,388 | 3,015,006 |

Latency p999 (us, mean):

| connections | epoll | coroutine | io_uring |
|------------:|------:|----------:|---------:|
| 1,000 | 215,788 | 202,463 | 2,599,070 |
| 5,000 | 937,427 | 1,081,956 | 5,721,031 |
| 10,000 | 2,093,657 | 2,406,831 | 5,844,064 |
| 20,000 | 3,846,177 | 4,143,972 | 13,290,351 |

Events rejected in-band with `ERR queue` (share of all replies, mean):

| connections | epoll | coroutine | io_uring |
|------------:|------:|----------:|---------:|
| 1,000 | 48% | 52% | 39% |
| 5,000 | 49% | 53% | 42% |
| 10,000 | 50% | 52% | 38% |
| 20,000 | 48% | 50% | 34% |

Server RSS (MB, max of 3 samples):

| connections | epoll | coroutine | io_uring |
|------------:|------:|----------:|---------:|
| 1,000 | 18 | 20 | 18 |
| 5,000 | 49 | 50 | 40 |
| 10,000 | 89 | 91 | 69 |
| 20,000 | 167 | 172 | 129 |

## Reading

Throughput is transport-boundary-independent: the durable persistence pipeline (one log write per accepted event) saturates first, so all three backends land within a few percent of each other.
The transport choice shows up elsewhere.

- io_uring rejects fewer events than epoll or coroutine at every connection level (34-42% vs 48-53%).
  Its admission behavior is the closest to the pipeline's real capacity.
- io_uring uses the least memory: about 6.5 KB per connection at 20,000 connections versus 8.4 KB for epoll and 8.6 KB for coroutine.
- io_uring pays with its latency tail: as measured with the original 256-slot ring, p999 is 10-40x worse than epoll/coroutine (2.6-13.3 ms versus 0.2-4.1 ms), and the observed maximum is 10-19 ms versus 0.2-5 ms.
  The tail was stable across all three samples at every level, so it is a property of the backend under saturation, not noise.
  Its cause and fix are in the next section.

## Latency tail: cause and fix (2026-08-20)

The tail was real and its cause is now known: the io_uring ring held only 256 poll slots while the benchmark runs 1,000-20,000 concurrent connections.
Each connection with data pending holds one poll slot, and under load the armed polls are re-armed immediately, so the slots stay with whichever connections got them first.
Connections beyond the first 256 wait one or more loop cycles for a slot, which appears as a multi-millisecond tail.
epoll has no such cap: every file descriptor is registered with the kernel at once.

The fix sizes the ring for the connection scale: the default is now 4096 entries, tunable with `WEIR_URING_RING_SIZE` (256-32768) for experiments.
Ring memory is ~0.4 MB at 4096 entries, which is negligible.

Verified by A/B on the same host within the same minute (details in `results/bench/ring-ab.md`):

| connections | p999 with 256 slots | p999 with 4096 slots |
|------------:|--------------------:|---------------------:|
| 1,000 | 2.34 ms | 0.28 ms |
| 5,000 | 8.20 ms | 3.11 ms |

The 1,000-connection case also shows max latency dropping from 18.7 ms to 0.29 ms (66x), and admission improving from 37% to 31% rejected.
The tail at 1,000 connections is now at epoll's level (0.28 ms vs 0.22 ms).
Regression coverage: full integration suite and 20/20 seeded io_uring failpoint stress pass on the changed transport.

The 10,000- and 20,000-connection rows still need a clean re-run: the machine ran on battery power during the verification, which throttled absolute throughput roughly 2x and added host-level stalls above ~10 ms.
At 20,000 connections the slot fix trades differently: 4096 slots improve p50 (2.7 ms to 0.7 ms), admission (34% to 27% rejected), and memory (128 to 97 MB) but reduce throughput (112k to 73k ops/s) and leave p999 in the 12-22 ms range at every ring size from 256 to 8192.
The server loop is not CPU-bound in any configuration (78-90% of one core), so the remaining 20,000-connection tail is a structural property of slot-based polling with a single drain loop, not a tuning miss.

### The 20,000-connection tail: investigation result

Follow-up measurements (2026-08-20, details in `results/bench/ring-ab.md`) rule out the tuning hypotheses:

- A drain cap that bounds completions per loop cycle (256 or 1024) shortens cycles but defers every event that misses the cap by a cycle: at 20k, p999 improves only slightly (19.1 to 13.4 ms) while p50 degrades 1.2 to 4.4 ms and admission from 27% to 36% rejected.
- A full-coverage ring (32768 slots, all connections polled at once) without a cap is catastrophic (p50 13.5 ms, throughput 24.6k): longer cycles dominate.
- A same-moment control run at 20k on the degraded host shows epoll at p999 6.2 ms versus io_uring at 11.5 ms; the campaign's healthy-host epoll number was 3.85 ms, so the host inflates p999 by roughly 2.4 ms at this scale, leaving io_uring a real ~5 ms structural delta.
- The trace instrumentation shows ~675 poll completions and ~1.5 ms per loop cycle at 20k with 4096 slots, with a 1024-entry trace ring saturating every cycle.

The remaining suspect is the reply path: each reply needs a write-poll re-arm, which is a remove/re-arm round trip through the ring (two SQE generations plus their completions), and under a full ring that round trip can stall for multiple cycles.
The evaluated options all trade one metric for another; the fix that would actually remove the round trip is a two-poll transport (a stable read poll per connection plus a separately armed write poll), which is a rework of the transport state machine and is deferred.
Until then, io_uring is the right backend up to 10,000 connections; epoll is the right choice at 20,000.
`WEIR_URING_DRAIN_CAP` (0 = unlimited, the default) exists for experiments but is not recommended.

## Open questions

- Does the two-poll transport rework (stable read poll plus separately armed write poll) close the 20,000-connection gap?
- Absolute re-measurement of all rows on a clean host (AC power, no memory pressure) with the 4096-slot default.

## Planned figures

- Throughput versus concurrent connections.
- p50, p99, and p999 latency versus concurrent connections.
- Memory per connection versus concurrent connections.
- Rejected events and connection failures by backend.

## Known limitations

The current Lima environment is Ubuntu ARM64 with four virtual CPUs and three GiB of memory.
Results from this environment will not be presented as representative of x86 production hardware.
Campaign samples are 30 s with a 5 s warmup; the publication protocol in BENCHMARK.md requires three five-minute samples per level.
That protocol is infeasible at payload 1024 on this VM: the persistence log grows 1022 bytes per accepted event, so a single five-minute sample writes 29-46 GB, more than the VM's usable disk; a paced or smaller-payload variant is required first.
The persistence log is fsync-free (write plus flush per event), so absolute numbers reflect that configuration.
The Mac host throttles the VM on battery power (roughly 2x throughput loss); benchmark re-runs need AC power.
