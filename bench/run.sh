#!/bin/sh
set -eu
[ "$(uname -s)" = Linux ] || { printf '%s\n' 'bench/run.sh requires Linux server support' >&2; exit 2; }
build=${WEIR_BUILD_DIR:-build-make}
iterations=${1:-1000}
output=${2:-bench/results.jsonl}
port=${WEIR_PORT:-19000}
log=$(mktemp "${TMPDIR:-/tmp}/weir-bench.XXXXXX")
server="$build/weir-server"
producer="$build/weir-producer"
trap 'kill "$pid" 2>/dev/null || true; rm -f "$log"' EXIT INT TERM
"$server" --port "$port" --metrics-port "$((port + 1))" --log "$log" >/dev/null 2>&1 & pid=$!
i=0
while [ "$i" -lt 100 ]; do
  if curl -fsS "http://127.0.0.1:$((port + 1))/metrics" >/dev/null 2>&1 && "$producer" 127.0.0.1 "$port" readiness >/dev/null 2>&1; then break; fi
  sleep 0.02
  i=$((i + 1))
done
[ "$i" -lt 50 ] || { printf '%s\n' 'server did not become ready' >&2; exit 1; }
start=$(date +%s%N)
i=0
while [ "$i" -lt "$iterations" ]; do "$producer" 127.0.0.1 "$port" payload >/dev/null; i=$((i + 1)); done
end=$(date +%s%N)
elapsed_ns=$((end - start))
[ "$elapsed_ns" -gt 0 ] || elapsed_ns=1
mkdir -p "$(dirname "$output")"
timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
revision=$(git rev-parse HEAD 2>/dev/null || printf '%s' unknown)
printf '{"schema":"weir.benchmark.v1","timestamp_utc":"%s","command":"%s","iterations":%s,"elapsed_ns":%s,"elapsed_seconds":%.9f,"events_per_second":%.3f,"build_dir":"%s","git_revision":"%s"}\n' "$timestamp" "$0 $*" "$iterations" "$elapsed_ns" "$(awk -v n="$elapsed_ns" 'BEGIN { print n / 1000000000 }')" "$(awk -v i="$iterations" -v n="$elapsed_ns" 'BEGIN { print i * 1000000000 / n }')" "$build" "$revision" > "$output"
