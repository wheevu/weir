#!/usr/bin/env bash
# Seeded slow_reader stress campaign for the io_uring backend.
#
# Usage: scripts/uring-stress.sh [build-dir] [seeds] [worker-id] [start-seed]
#
# Runs `seeds` slow_reader scenarios with WEIR_URING_SEED set, one server per
# run. The seed activates the transport failpoints (completion reordering and
# deferred submissions), so every run sweeps both kernel interleavings.
# Failures are reported to stdout and leave a trace dump for diagnosis.
set -u

BUILD="${1:-build-uring}"
SEEDS="${2:-250}"
WORKER="${3:-0}"
START="${4:-1}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TRACES="$ROOT/results/traces"
mkdir -p "$TRACES"

pass=0
fail=0
failed_seeds=""
for ((i = 0; i < SEEDS; i++)); do
  seed=$((START + i))
  trace="$TRACES/worker-$WORKER-seed-$seed.trace"
  rm -f "$trace"
  if WEIR_URING_SEED="$seed" WEIR_URING_TRACE_FILE="$trace" \
      WEIR_BACKEND=io_uring python3 "$ROOT/tests/integration/transport_test.py" \
      --server "$BUILD/weir-server" --inspect "$BUILD/weir-inspect-log" \
      --scenario slow_reader >/dev/null 2>&1; then
    pass=$((pass + 1))
    rm -f "$trace"
  else
    fail=$((fail + 1))
    failed_seeds="$failed_seeds $seed"
  fi
  if ((i % 25 == 24)); then
    echo "worker $WORKER progress: $((i + 1))/$SEEDS pass=$pass fail=$fail"
  fi
done
echo "worker $WORKER done: pass=$pass fail=$fail failed_seeds:$failed_seeds"
# Nonzero exit so the script can serve as an acceptance gate in CI.
if ((fail > 0)); then exit 1; fi
exit 0
