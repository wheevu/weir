#!/usr/bin/env bash
# Benchmark campaign: results/bench/<backend>-<conns>-<sample>.json
#
# Usage: scripts/bench-campaign.sh [build-dir] [duration-s] [samples]
#
# Runs the full backend x connection-level matrix with the same payload,
# window, warmup, client, and persistence configuration everywhere, and
# records environment metadata beside the results (docs/BENCHMARK.md run
# rules 1, 3, 5, 6).
set -u

BUILD="${1:-build-uring}"
DURATION="${2:-30}"
SAMPLES="${3:-3}"
WARMUP=5
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/results/bench"
mkdir -p "$OUT"
ulimit -n 65535

{
  echo "kernel=$(uname -r) arch=$(uname -m)"
  echo "compiler=$(g++ --version | head -1)"
  echo "git=$(git -C "$ROOT" rev-parse --short HEAD)"
  echo "bench_bin=$(sha256sum "$ROOT/$BUILD/weir_bench" | cut -d' ' -f1)"
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "payload=1024 window=32 warmup=${WARMUP}s duration=${DURATION}s samples=${SAMPLES}"
} > "$OUT/meta.txt"

for backend in epoll coroutine io_uring; do
  for conns in 1000 5000 10000 20000; do
    for ((sample = 1; sample <= SAMPLES; sample++)); do
      f="$OUT/$backend-$conns-$sample.json"
      if [[ -s "$f" ]]; then
        echo "skip $backend $conns sample $sample (exists)"
        continue
      fi
      if ! "$ROOT/$BUILD/weir_bench" --server "$ROOT/$BUILD/weir-server" \
          --backend "$backend" --connections "$conns" --duration "$DURATION" \
          --warmup "$WARMUP" --payload 1024 --out "$f"; then
        echo "FAILED $backend $conns sample $sample" >> "$OUT/meta.txt"
        rm -f "$f"
      fi
      echo "done $backend $conns sample $sample: $(head -c 140 "$f")"
    done
  done
done
echo "campaign complete: $(ls "$OUT" | wc -l) files in $OUT"