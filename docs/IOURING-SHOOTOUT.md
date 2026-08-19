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
- io_uring pays with its latency tail: p999 is 10-40x worse than epoll/coroutine (2.6-13.3 ms versus 0.2-4.1 ms), and the observed maximum is 10-19 ms versus 0.2-5 ms.
  The tail is stable across all three samples at every level, so it is a property of the backend under saturation, not noise.
  Its mechanism is not yet investigated; the leading hypothesis is batching in the single ring drain loop rather than the 100 ms idle timeout.

## Open questions

- Why does io_uring's p999 grow with connection count (2.6 ms at 1,000 to 13.3 ms at 20,000) while its p50 tracks epoll?
- Does SQPOLL or a larger ring change the tail?

## Planned figures

- Throughput versus concurrent connections.
- p50, p99, and p999 latency versus concurrent connections.
- Memory per connection versus concurrent connections.
- Rejected events and connection failures by backend.

## Known limitations

The current Lima environment is Ubuntu ARM64 with four virtual CPUs and six GiB of memory.
Results from this environment will not be presented as representative of x86 production hardware.
Campaign samples are 30 s with a 5 s warmup; the publication protocol in BENCHMARK.md requires three five-minute samples per level.
The persistence log is fsync-free (write plus flush per event), so absolute numbers reflect that configuration.
