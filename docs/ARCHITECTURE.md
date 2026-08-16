# Architecture

The core has no runtime dependencies.

`Parser` accepts arbitrary chunks and emits only complete, checksum-valid frames.

`BoundedQueue` applies backpressure and closes cleanly.

`Pipeline` places accepted events on a persistence queue before append and only then places them on the processing queue.

`Log::replay` scans valid frames from the append-only file and ignores incomplete trailing data.
