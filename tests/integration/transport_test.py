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
import threading
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


def backend():
    return os.environ.get("WEIR_BACKEND")


def metric(srv, name):
    body = srv.fetch_metrics().split(b"\r\n\r\n", 1)[1].decode()
    for line in body.splitlines():
        if line.startswith(name + " "):
            return int(line.split()[1])
    return None


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
        command = [server_bin, "--port", str(self.port),
                   "--metrics-port", str(self.metrics_port),
                   "--log", self.log_path]
        backend = env.get("WEIR_BACKEND")
        if backend:
            command.extend(["--backend", backend])
        self.proc = subprocess.Popen(
            command,
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


def scenario_parser_flood_isolation(server_bin, inspect_bin):
    """A client flooding malformed parser traffic (the noisy neighbor) must
    not delay or drop ACKs for a separate client's valid events.

    The flood runs in its own background thread that streams continuously while
    the healthy client submits real events on the main thread, so the server's
    single epoll thread is genuinely interleaving both peers at once rather than
    draining the flood in a quiet gap between sequential sends. The flood pushes
    a large no-magic byte stream and ends with an oversized length that poisons
    only its own parser. A real (linear) resync keeps the epoll thread free so
    the quiet neighbor's valid events are still acknowledged in order and made
    durable. A quadratic resync would pin the single epoll thread inside the
    flood's feed() and starve the quiet neighbor past its ACK deadline.

    The healthy client's full ACK batch must arrive inside a bounded deadline,
    which proves the flood never starved it.
    """
    srv = Server(server_bin, inspect_bin)
    # Fastened to one outer try/finally so a failure anywhere in the scenario
    # body, including the readiness barrier, still unblocks, joins, and closes
    # the flood before being reported. The flood must never survive a failed
    # scenario as an orphan thread or socket.
    flood_done = threading.Event()
    # Set only after the flood socket has connected and pushed a substantial
    # first burst, so the scenario never races ahead and lets the healthy
    # client run before the noisy neighbor is provably live.
    flood_live = threading.Event()
    # Standard-library channel for surfacing background-thread failures into
    # the main scenario thread instead of swallowing them.
    flood_error = []
    flood_thread = None
    healthy_socket = None
    # Exception raised by the scenario body (readiness barrier, healthy
    # traffic, or post-conditions). Re-raised after cleanup unless an explicit
    # flood/cleanup failure supersedes it.
    body_error = None
    ids = None
    elapsed = None
    try:
        good = connect(srv.port)
        healthy_socket = good
        expected = 10
        # Flood streams on a dedicated thread so it is live for the whole
        # window in which the healthy client sends and receives its ACKs.

        def flood():
            try:
                f = connect(srv.port)
                # Short timeout makes the writer interruptible: a stalled send
                # raises socket.timeout instead of blocking the shutdown
                # forever, and we treat that as a retry while the flood is
                # still active (see below). Any other socket error before
                # shutdown is a genuine flood failure to record.
                f.settimeout(0.2)
                try:
                    # Substantial first burst: all-zero bytes that never begin a
                    # magic sequence. This must be on the wire (and acknowledged
                    # by the OS socket layer) before we release the scenario to
                    # start healthy traffic. A socket.timeout here just means the
                    # send buffer is momentarily full: retry rather than fail.
                    for _ in range(4):
                        try:
                            f.sendall(bytes(4096))
                        except socket.timeout:
                            continue
                    flood_live.set()
                    while not flood_done.is_set():
                        try:
                            f.sendall(bytes(4096))  # all zeros: no magic
                            # Keep the flood continuous without monopolizing
                            # CPU in the test process on slower runners.
                            time.sleep(0.001)
                        except socket.timeout:
                            # Buffer full / peer slow: stay live and retry
                            # rather than treating a timeout as a flood failure.
                            continue
                except OSError as exc:
                    # A socket error before we signaled shutdown is an
                    # unexpected flood failure: the server dropped the noisy
                    # neighbor on its own, which this scenario must not allow.
                    # Record it so the main thread turns it into a real test
                    # failure instead of silently suppressing it. A socket
                    # error after flood_done is set is expected (shutdown).
                    if not flood_done.is_set():
                        flood_error.append(exc)
                finally:
                    # Final poison payload: oversized length poisons only this
                    # parser. Sent even if the streaming loop was interrupted,
                    # and tolerated if the connection is already gone.
                    try:
                        f.sendall(b"WR01" + struct.pack(">QI", 1, 1024 * 1024 + 1)
                                  + b"\0" * 4)
                    except OSError:
                        pass
                    f.close()
            except BaseException as exc:  # capture any background failure
                flood_error.append(exc)

        flood_thread = threading.Thread(target=flood, daemon=True)
        flood_thread.start()
        # Prove the flood connection has connected and sent a substantial first
        # burst before healthy traffic starts.
        if not flood_live.wait(timeout=5.0):
            if flood_error:
                raise RuntimeError("flood thread failed before going live: %r"
                                   % (flood_error[0],))
            raise RuntimeError("flood connection never became live")
        # Submit the healthy client's valid events while the flood streams.
        # Keep this bounded but configurable for slower CI runners; quadratic
        # parser resync still pushes this well past the deadline.
        ack_timeout = float(os.environ.get("WEIR_PARSER_FLOOD_ACK_TIMEOUT",
                                           "10"))
        start = time.monotonic()
        for i in range(expected):
            good.sendall(make_frame(9100 + i, b"good-%d" % i))
        ack_deadline = start + ack_timeout
        timeout = max(0.1, ack_deadline - time.monotonic())
        ids = recv_ack_ids(good, expected, timeout=timeout)
        elapsed = time.monotonic() - start
        assert ids == list(range(1, expected + 1)), ids
        assert elapsed <= ack_timeout, ("healthy ACKs took %.2fs, expected < %.1fs"
                                        % (elapsed, ack_timeout))
        assert "records=%d" % expected in srv.inspect_log(), srv.inspect_log()
    except BaseException as exc:  # noqa: BLE001 - capture to re-raise post-cleanup
        body_error = exc
    finally:
        # Always runs for every exit path above: signal the flood to stop, join
        # it, and close both sockets so no orphan thread or connection leaks.
        # Errors here are collected rather than raised immediately, so the server
        # is always stopped first (see the outer finally) and nothing leaks when
        # cleanup itself raises.
        flood_done.set()
        if flood_thread is not None:
            flood_thread.join(timeout=2)
        if healthy_socket is not None:
            healthy_socket.close()
    # Server shutdown runs on every path, including when flood cleanup surfaced
    # a failure, so the scenario never orphans a server process.
    try:
        if flood_error:
            # A flood-thread failure supersedes the healthy-path result, but the
            # healthy failure is folded into the message so it is not silently
            # masked by the noise of the flood.
            msg = "flood thread failed: %r" % (flood_error[0],)
            if body_error is not None:
                msg += " (scenario also failed: %r)" % (body_error,)
            raise RuntimeError(msg)
        # Cleanup itself failed: the flood did not terminate after we signaled
        # it. Report that rather than masking it behind the scenario error.
        if flood_thread is not None:
            assert not flood_thread.is_alive(), "flood thread did not terminate"
        if body_error is not None:
            raise body_error
    finally:
        srv.stop()


def scenario_slow_reader(server_bin, inspect_bin):
    """A client that does not read ACKs for a while must get a reply for
    every event, in order, without truncation or duplication.

    Every backend admits through a bounded queue, so under send-pressure
    bursts the server may reject surplus events with an in-band "ERR queue"
    reply instead of losing the event. The contract is: each event is
    answered exactly once, replies arrive in order, and the durable log
    holds exactly the accepted events. The client shrinks its receive window
    so the server's ACK path must experience real send-buffer pressure.

    The event count is sized so the scenario also terminates quickly under
    ASan, where flush-per-event persistence is several times slower.
    """
    srv = Server(server_bin, inspect_bin,
                 extra_env={"WEIR_TEST_SNDBUF": "8192"})
    count = 4000
    ack_timeout = float(os.environ.get("WEIR_ACK_TIMEOUT", "60"))
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
        deadline = time.monotonic() + ack_timeout
        data = b""
        while data.count(b"\n") < count and time.monotonic() < deadline:
            data += c.recv(1 << 16)
        c.close()
        lines = data.splitlines()
        ok = [int(line.split()[1]) for line in lines if line.startswith(b"OK ")]
        err = lines.count(b"ERR queue")
        if len(ok) + err != count:
            raise AssertionError(
                "expected %d replies, got %d lines (OK=%d ERR=%d), "
                "rejected=%s overload=%s"
                % (count, len(lines), len(ok), err,
                   metric(srv, "weir_rejected_total"),
                   metric(srv, "weir_overload_total")))
        # Replies must arrive in order: the server assigns ids 1..count and
        # every event (accepted or rejected) advances the sequence, so each
        # OK id must equal its 1-based position among all reply lines.
        expected_id = 1
        for line in lines:
            if line.startswith(b"OK "):
                assert line == (b"OK " + str(expected_id).encode()), line
            else:
                assert line == b"ERR queue", line
            expected_id += 1
        assert len(ok) > 0, "%s backend admitted no events" % backend()
        assert "records=%d" % len(ok) in srv.inspect_log(), srv.inspect_log()
    finally:
        srv.stop()


def scenario_overload_admission(server_bin, inspect_bin):
    """A full admission queue rejects without blocking the epoll thread."""
    srv = Server(server_bin, inspect_bin,
                 extra_env={"WEIR_TEST_ACK_DELAY_MS": "20"})
    count = 320
    try:
        c = connect(srv.port, timeout=30)
        c.sendall(b"".join(make_frame(30000 + i, b"overload")
                           for i in range(count)))
        c.settimeout(30)
        data = b""
        while data.count(b"\n") < count:
            chunk = c.recv(1 << 16)
            if not chunk:
                raise RuntimeError("server closed during overload")
            data += chunk
        lines = data.splitlines()
        assert any(line == b"ERR queue" for line in lines), lines[:5]
        accepted = sum(line.startswith(b"OK ") for line in lines)
        assert accepted > 0, lines[:5]
        expected_id = 1
        for line in lines:
            if line.startswith(b"OK "):
                assert line == (b"OK " + str(expected_id).encode()), line
            expected_id += 1
        metrics = srv.fetch_metrics().split(b"\r\n\r\n", 1)[1].decode()
        overload = int(next(line.split()[1] for line in metrics.splitlines()
                            if line.startswith("weir_overload_total ")))
        rejected = int(next(line.split()[1] for line in metrics.splitlines()
                            if line.startswith("weir_rejected_total ")))
        assert overload == rejected == lines.count(b"ERR queue"), metrics
        assert "records=%d" % accepted in srv.inspect_log(), srv.inspect_log()
        c.sendall(make_frame(40000, b"recovered"))
        assert recv_ack_ids(c, 1, timeout=10) == [count + 1]
        c.close()
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
    "parser_flood_isolation": scenario_parser_flood_isolation,
    "slow_reader": scenario_slow_reader,
    "overload_admission": scenario_overload_admission,
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
