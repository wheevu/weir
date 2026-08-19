# io_uring versus epoll

This is the writeup scaffold for the Weir v2 backend comparison.

## Claim boundary

The comparison will report measured throughput, p50 latency, p99 latency, p999 latency, and memory per connection.
It will not claim that io_uring is universally faster.

The server protocol and persistence pipeline must remain identical between backends.
Only the transport implementation may vary.

## Results

Results are intentionally absent until the benchmark harness produces reproducible HDR histogram data.

## Planned figures

- Throughput versus concurrent connections.
- p50, p99, and p999 latency versus concurrent connections.
- Memory per connection versus concurrent connections.
- Rejected events and connection failures by backend.

## Known limitations

The current Lima environment is Ubuntu ARM64 with four virtual CPUs and six GiB of memory.
Results from this environment will not be presented as representative of x86 production hardware.
