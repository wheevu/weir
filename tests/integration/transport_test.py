#!/usr/bin/env python3
"""Linux transport integration tests for Weir.

Starts real weir-server processes and exercises them over real TCP sockets.
Linux only. Registered with CTest when WEIR_LINUX_INTEGRATION_TESTS=ON.

Every scenario starts its own server on private ports, waits for readiness by
probing the port (not by sleeping), captures server output, and always shuts
the server down in a finally block so no orphan processes survive failures.
"""

import argparse
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FAILURES = []


def checksum(payload):
    c = 2166136261
    for b in payload:
        c = ((c ^ b) * 16777619) & 0xFFFFFFFF
    return c


def make_frame(event_id, payload):
    return (b"WR01" + struct.pack(">QI", event_id, len(payload))
            + payload + struct.pack(">I", checksum(payload)))


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    def __init__(self, server_bin, inspect_bin, extra_env=None):
        self.port = free_port()
        self.metrics_port = free_port()
        self.tmp = tempfile.mkdtemp(prefix="weir-transport-")
        self.log_path = os.path.join(self.tmp, "events.wrl")
        self.inspect_bin = inspect_bin
        env = dict(os.environ)
        if extra_env:
            env.update(extra_env)
        self.proc = subprocess.Popen(
            [server_bin, "--port", str(self.port),
             "--metrics-port", str(self.metrics_port),
             "--log", self.log_path],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            env=env, text=True)
        if not self.wait_ready(5.0):
            out = ""
            if self.proc.stdout:
                out = self.proc.stdout.read()
            raise RuntimeError("server did not become ready on port %s:\n%s"
                               % (self.port, out))

    def wait_ready(self, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                return False
            try:
                s = socket.create_connection(("127.0.0.1", self.port),
                                             timeout=0.2)
                s.close()
                return True
            except OSError:
                time.sleep(0.05)
        return False

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        if self.proc.stdout:
            self.proc.stdout.read()

    def inspect_log(self):
        out = subprocess.check_output([self.inspect_bin, self.log_path],
                                      text=True)
        return out.strip()

    def fetch_metrics(self):
        s = socket.create_connection(("127.0.0.1", self.metrics_port),
                                     timeout=2)
        s.sendall(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n"
                  b"Connection: close\r\n\r\n")
        data = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
        s.close()
        return data


def connect(port, timeout=5):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(timeout)
    s.connect(("127.0.0.1", port))
    return s


def recv_ack_ids(sock, expected, timeout=10):
    """Read ACK lines until `expected` distinct ids arrive or the peer closes.

    Fails loudly instead of spinning when the connection ends early.
    """
    deadline = time.monotonic() + timeout
    buf = b""
    ids = []
    while len(ids) < expected:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("ack timeout: %d/%d ids" % (len(ids), expected))
        sock.settimeout(remaining)
        chunk = sock.recv(1 << 16)
        if not chunk:
            raise RuntimeError("server closed connection: %d/%d ids"
                               % (len(ids), expected))
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            parts = line.split()
            if parts and parts[0] == b"OK":
                ids.append(int(parts[1]))
    return ids


def scenario_metrics(server_bin, inspect_bin):
    srv = Server(server_bin, inspect_bin)
    try:
        c = connect(srv.port)
        c.sendall(make_frame(1, b"metric-probe"))
        ids = recv_ack_ids(c, 1)
        c.close()
        assert ids == [1], ids
        body = srv.fetch_metrics()
        assert b"HTTP/1.1 200 OK" in body, body[:80]
        text = body.split(b"\r\n\r\n", 1)[1].decode()
        assert "weir_validated_total 1" in text, text
        assert "weir_durable_total 1" in text, text
        assert "weir_processed_total 1" in text, text
    finally:
        srv.stop()


def scenario_valid_frame(server_bin, inspect_bin):
    srv = Server(server_bin, inspect_bin)
    try:
        c = connect(srv.port)
        c.sendall(make_frame(7, b"hello"))
        ids = recv_ack_ids(c, 1)
        c.close()
        # The server assigns its own monotonic ids starting at 1.
        assert ids == [1], ids
        assert "records=1" in srv.inspect_log(), srv.inspect_log()
    finally:
        srv.stop()


def scenario_fragmented_frame(server_bin, inspect_bin):
    srv = Server(server_bin, inspect_bin)
    try:
        c = connect(srv.port)
        frame = make_frame(9, b"fragment-me")
        ids = []
        for cut in range(1, len(frame)):
            c.sendall(frame[:cut])
            time.sleep(0.002)
            c.sendall(frame[cut:])
            ids += recv_ack_ids(c, 1)
        c.close()
        assert ids == list(range(1, len(frame))), ids
        assert "records=%d" % (len(frame) - 1) in srv.inspect_log(), \
            srv.inspect_log()
    finally:
        srv.stop()


def scenario_coalesced_frames(server_bin, inspect_bin):
    srv = Server(server_bin, inspect_bin)
    try:
        c = connect(srv.port)
        c.sendall(make_frame(10, b"a") + make_frame(11, b"b")
                  + make_frame(12, b"c"))
        ids = recv_ack_ids(c, 3)
        c.close()
        assert ids == [1, 2, 3], ids
        assert "records=3" in srv.inspect_log(), srv.inspect_log()
    finally:
        srv.stop()


def scenario_half_close_final_frame(server_bin, inspect_bin):
    """Valid frame + immediate SHUT_WR must preserve the frame and deliver
    its ACK. Baseline lost 39/40 trials; the fix must hold across 30."""
    srv = Server(server_bin, inspect_bin)
    trials = 30
    try:
        for i in range(trials):
            c = connect(srv.port)
            c.sendall(make_frame(100 + i, b"final-frame"))
            c.shutdown(socket.SHUT_WR)
            ids = recv_ack_ids(c, 1)
            c.close()
            assert ids == [i + 1], (i, ids)
        assert "records=%d" % trials in srv.inspect_log(), srv.inspect_log()
    finally:
        srv.stop()


def scenario_ack_after_half_close(server_bin, inspect_bin):
    """Several events, then SHUT_WR: all earned ACKs must arrive even though
    persistence completions land after the peer stopped sending."""
    srv = Server(server_bin, inspect_bin)
    try:
        c = connect(srv.port)
        for i in range(5):
            c.sendall(make_frame(200 + i, b"multi-%d" % i))
        c.shutdown(socket.SHUT_WR)
        ids = recv_ack_ids(c, 5)
        c.close()
        assert ids == [1, 2, 3, 4, 5], ids
        assert "records=5" in srv.inspect_log(), srv.inspect_log()
    finally:
        srv.stop()


SCENARIOS = {
    "metrics_endpoint": scenario_metrics,
    "valid_frame": scenario_valid_frame,
    "fragmented_frame": scenario_fragmented_frame,
    "coalesced_frames": scenario_coalesced_frames,
    "half_close_final_frame": scenario_half_close_final_frame,
    "ack_after_half_close": scenario_ack_after_half_close,
}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--server", required=True,
                    help="path to the weir-server binary")
    ap.add_argument("--inspect", required=True,
                    help="path to the weir-inspect-log binary")
    ap.add_argument("--scenario", choices=sorted(SCENARIOS),
                    help="run a single scenario")
    args = ap.parse_args()

    names = [args.scenario] if args.scenario else sorted(SCENARIOS)
    for name in names:
        started = time.monotonic()
        try:
            SCENARIOS[name](args.server, args.inspect)
            print("PASS %-22s %.1fs" % (name, time.monotonic() - started))
        except Exception as exc:  # noqa: BLE001 - report and continue
            FAILURES.append((name, exc))
            print("FAIL %-22s %r" % (name, exc))
    if FAILURES:
        print("%d scenario(s) failed" % len(FAILURES))
        return 1
    print("all transport scenarios passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
