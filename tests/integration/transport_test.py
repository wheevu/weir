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
            if self.proc.poll() is None:
                self.proc.kill()
                self.proc.wait()
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
                                      text=True, timeout=30)
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


def scenario_fd_reuse_guard(server_bin, inspect_bin):
    """A stale persistence completion must never reach a new connection.

    Deterministic setup: client A submits an event whose ACK is delayed
    (WEIR_TEST_ACK_DELAY_MS), then triggers a server-side drop via an
    oversized length field. A's connection is destroyed while its completion
    is still in flight. Churn connections follow, then B connects; A's stale
    completion fires while B is live. B must never see A's ACK.
    """
    srv = Server(server_bin, inspect_bin,
                 extra_env={"WEIR_TEST_ACK_DELAY_MS": "400"})
    try:
        # A: one valid event, then an oversized length field that makes the
        # parser throw, forcing the server to drop the connection while the
        # valid event's completion is still delayed.
        a = connect(srv.port)
        a.sendall(make_frame(5000, b"from-a"))
        bad = (b"WR01" + struct.pack(">QI", 5001, 1024 * 1024 + 1)
               + b"\0" * 4)
        a.sendall(bad)
        a.close()
        time.sleep(0.3)  # let the server process the drop

        for _ in range(8):  # churn so B takes the lowest free fd
            c = connect(srv.port)
            c.close()
            time.sleep(0.01)

        b = connect(srv.port)
        b.sendall(make_frame(6000, b"from-b"))
        ids = recv_ack_ids(b, 1)
        # Wait past the stale completion's delivery window and make sure
        # nothing else arrives on B's connection.
        b.settimeout(0.8)
        try:
            extra = b.recv(1 << 16)
        except socket.timeout:
            extra = b""
        b.close()
        assert ids == [2], (ids, extra)
        assert b"OK 1" not in extra, extra
        # A's event was persisted before its connection died.
        assert "records=2" in srv.inspect_log(), srv.inspect_log()
        # Limitation: the test cannot observe the server-side fd number, so
        # it proves "a stale completion never reaches a new client" but not
        # that B specifically reused A's fd. The generation check itself is
        # exercised whenever B lands on A's fd (the kernel hands out the
        # lowest free fd, which the churn makes overwhelmingly likely).
    finally:
        srv.stop()


def scenario_client_churn(server_bin, inspect_bin):
    """Rapid connect/send/close cycles must not leak fds or break the server."""
    srv = Server(server_bin, inspect_bin)
    try:
        fd_count = lambda: len(os.listdir("/proc/%d/fd" % srv.proc.pid))
        before = fd_count()
        for i in range(200):
            c = connect(srv.port)
            c.sendall(make_frame(7000 + i, b"churn"))
            c.close()
        # The server processes closes asynchronously; wait for the fd count
        # to stabilize (bounded) instead of asserting on a fixed sleep.
        deadline = time.monotonic() + 5
        after = fd_count()
        while after > before + 5 and time.monotonic() < deadline:
            time.sleep(0.05)
            after = fd_count()
        assert after <= before + 5, (before, after)
        c = connect(srv.port)
        c.sendall(make_frame(9999, b"still-alive"))
        ids = recv_ack_ids(c, 1)
        c.close()
        assert ids == [201], ids
        assert "records=201" in srv.inspect_log(), srv.inspect_log()
    finally:
        srv.stop()


def scenario_mid_frame_disconnect(server_bin, inspect_bin):
    """A client that disconnects mid-frame must not hurt the server."""
    srv = Server(server_bin, inspect_bin)
    try:
        c = connect(srv.port)
        c.sendall(b"WR01\x00\x00")  # partial header, then vanish
        c.close()
        time.sleep(0.1)
        d = connect(srv.port)
        d.sendall(make_frame(8000, b"after-disconnect"))
        ids = recv_ack_ids(d, 1)
        d.close()
        assert ids == [1], ids
        assert "records=1" in srv.inspect_log(), srv.inspect_log()
    finally:
        srv.stop()


def scenario_malformed_isolation(server_bin, inspect_bin):
    """Malformed traffic from client A must not affect client B."""
    srv = Server(server_bin, inspect_bin)
    try:
        a = connect(srv.port)
        a.sendall(bytes(range(256)) * 8)  # garbage, no magic
        a.sendall(make_frame(9000, b"bad-checksum")[:-1])  # truncated frame
        oversized = (b"WR01" + struct.pack(">QI", 9001, 1024 * 1024 + 1)
                     + b"\0" * 4)
        a.sendall(oversized)  # poisons A's parser, not the server

        b = connect(srv.port)
        for i in range(3):
            b.sendall(make_frame(9002 + i, b"valid-%d" % i))
        ids = recv_ack_ids(b, 3)
        b.close()
        a.close()
        assert ids == [1, 2, 3], ids
        assert "records=3" in srv.inspect_log(), srv.inspect_log()
        c = connect(srv.port)  # server still healthy
        c.sendall(make_frame(9005, b"still-alive"))
        ids = recv_ack_ids(c, 1)
        c.close()
        assert ids == [4], ids
    finally:
        srv.stop()


def scenario_slow_reader(server_bin, inspect_bin):
    """A client that does not read ACKs for a while must get every ACK,
    in order, without truncation or duplication.

    The event count is sized so the scenario also terminates quickly under
    ASan, where flush-per-event persistence is several times slower. The
    client shrinks its receive window so the server's ACK path must
    experience real send-buffer pressure.
    """
    srv = Server(server_bin, inspect_bin,
                 extra_env={"WEIR_TEST_SNDBUF": "8192"})
    count = 4000
    try:
        c = connect(srv.port, timeout=60)
        c.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        c.setsockopt(socket.SOL_TCP, socket.TCP_WINDOW_CLAMP, 4096)
        for chunk_start in range(0, count, 500):
            buf = b"".join(make_frame(10000 + i, b"x" * 1024)
                           for i in range(chunk_start, chunk_start + 500))
            c.sendall(buf)
            time.sleep(0.01)  # pace so the server can buffer completions
        time.sleep(0.8)  # deliberately not reading while ACKs accumulate
        ids = recv_ack_ids(c, count, timeout=60)
        c.close()
        assert len(ids) == count and len(set(ids)) == count, len(ids)
        assert ids == sorted(ids), "ACK order must be preserved"
        assert "records=%d" % count in srv.inspect_log(), srv.inspect_log()
    finally:
        srv.stop()


SCENARIOS = {
    "metrics_endpoint": scenario_metrics,
    "valid_frame": scenario_valid_frame,
    "fragmented_frame": scenario_fragmented_frame,
    "coalesced_frames": scenario_coalesced_frames,
    "half_close_final_frame": scenario_half_close_final_frame,
    "ack_after_half_close": scenario_ack_after_half_close,
    "fd_reuse_guard": scenario_fd_reuse_guard,
    "client_churn": scenario_client_churn,
    "mid_frame_disconnect": scenario_mid_frame_disconnect,
    "malformed_isolation": scenario_malformed_isolation,
    "slow_reader": scenario_slow_reader,
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
            print("PASS %-22s %.1fs" % (name, time.monotonic() - started),
                  flush=True)
        except Exception as exc:  # noqa: BLE001 - report and continue
            FAILURES.append((name, exc))
            print("FAIL %-22s %r" % (name, exc), flush=True)
    if FAILURES:
        print("%d scenario(s) failed" % len(FAILURES))
        return 1
    print("all transport scenarios passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
