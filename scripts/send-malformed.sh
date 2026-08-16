#!/bin/sh
set -eu
[ "$(uname -s)" = Linux ] || { printf '%s\n' 'send-malformed.sh requires Linux server support' >&2; exit 2; }
host=${1:-127.0.0.1}
port=${2:-9000}
metrics_port=${3:-$((port + 1))}

python3 - "$host" "$port" "$metrics_port" <<'PY'
import socket, struct, sys
host, port, metrics_port = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
def send(data):
    with socket.create_connection((host, port), timeout=2) as s:
        s.sendall(data)
        try: s.recv(32)
        except socket.timeout: pass

# Bad checksum must not terminate the service.
payload = b"bad-checksum"
frame = b"WR01" + struct.pack(">QI", 1, len(payload)) + payload + struct.pack(">I", 0)
send(frame)
# Oversized length must be rejected and scoped to this connection.
with socket.create_connection((host, port), timeout=2) as s:
    s.sendall(b"WR01" + struct.pack(">QI", 2, 1024 * 1024 + 1) + b"\0" * 4)
    try:
        if s.recv(1) != b"":
            raise RuntimeError("oversized frame connection was not rejected")
    except ConnectionResetError:
        pass
try:
    with socket.create_connection((host, metrics_port), timeout=2) as s:
        s.sendall(b"GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n")
        if b"200 OK" not in s.recv(4096):
            raise RuntimeError("metrics endpoint did not respond 200 OK")
except OSError as exc:
    raise SystemExit(f"server is not alive after malformed input: {exc}")
print("sent bad-checksum and oversized-length cases")
PY
