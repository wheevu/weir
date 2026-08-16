# Benchmarks

`run.sh` measures the local producer-to-server path and writes raw newline-delimited JSON.
It reports measurements only; it does not contain expected throughput or latency numbers.
Use a dedicated temporary log and port, and retain the raw file with its build/compiler metadata.

Schema: `schema`, `timestamp_utc`, `command`, `iterations`, `elapsed_ns`, `elapsed_seconds`, `events_per_second`, `build_dir`, `git_revision`.

The script waits for both the ingestion TCP port and the metrics HTTP endpoint before measuring.
