#!/bin/sh
set -eu
[ "$(uname -s)" = Linux ] || { printf '%s\n' 'crash-recovery.sh requires Linux server support' >&2; exit 2; }
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
build=${WEIR_BUILD_DIR:-"$root/build-make"}
case "$build" in /*) ;; *) build="$root/$build" ;; esac
tmpdir=${TMPDIR:-/tmp}
run_id="$$-$(date +%s)-${RANDOM:-0}"
log=${1:-"$tmpdir/weir-recovery-$run_id.log"}
port=${WEIR_PORT:-$((19000 + ($$ % 1000)))}
metrics_port=$((port + 1))
rm -f "$log"
start_server() { "$build/weir-server" --port "$port" --metrics-port "$metrics_port" --log "$log" >/dev/null 2>&1 & pid=$!; }
wait_ready() { i=0; while [ "$i" -lt 100 ]; do curl -fsS "http://127.0.0.1:$metrics_port/metrics" >/dev/null 2>&1 && return 0; sleep 0.02; i=$((i + 1)); done; return 1; }
start_server
trap 'kill "${pid:-}" 2>/dev/null || true; rm -f "$log"' EXIT INT TERM
wait_ready
"$build/weir-producer" 127.0.0.1 "$port" before-crash >/dev/null
kill "$pid" 2>/dev/null || true
wait "$pid" 2>/dev/null || true
printf 'WR01' >> "$log"
start_server
wait_ready
reply=$("$build/weir-producer" 127.0.0.1 "$port" after-restart)
case "$reply" in *"OK 2"*) ;; *) printf '%s\n' "continuity check failed: $reply" >&2; exit 1;; esac
inspection=$($build/weir-inspect-log "$log")
printf '%s\n' "$inspection" | grep -Eq '^records=2$' || { printf '%s\n' "exact recovery count check failed:" "$inspection" >&2; exit 1; }
printf '%s\n' "$inspection" | grep -Eq '^1 bytes=12 checksum=[0-9]+$' || { printf '%s\n' "missing exact first record:" "$inspection" >&2; exit 1; }
printf '%s\n' "$inspection" | grep -Eq '^2 bytes=13 checksum=[0-9]+$' || { printf '%s\n' "missing exact second record:" "$inspection" >&2; exit 1; }
printf '%s\n' "recovery log: $log" "$inspection"
