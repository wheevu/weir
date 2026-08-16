# Persistence and Recovery

The log is an append-only binary file containing the same frames as the wire protocol.
Each successful append is flushed before the event is placed on the processing queue.
This ordering is the core durability invariant: processing never observes an event that was not appended successfully.

On startup, `Log::recover()` scans complete, checksum-valid frames, truncates the incomplete or corrupt tail, and returns the next event id.
`Log::replay()` reads complete valid frames without changing the file.
Corrupt frames are skipped by the parser's resynchronization rules; operators should preserve the original file for investigation before repair.

This is local durability, not replicated storage.
The current implementation does not fsync the directory or provide a transaction record, so a power-loss guarantee beyond the operating system's flushed file state is not claimed.
