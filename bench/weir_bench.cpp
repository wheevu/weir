// weir_bench: benchmark client for the Weir server.
//
// Spawns a real weir-server process (epoll, coroutine, or io_uring backend),
// runs a pipelined load of N concurrent connections on a single-threaded
// coroutine client (the same transport layer the server uses), records
// per-reply latency into an HDR histogram, and prints one JSON line.
//
// Usage:
//   weir_bench --server PATH --backend NAME [--connections N] [--duration S]
//              [--warmup S] [--window K] [--payload N] [--out FILE]
//
// The JSON output is the only machine-readable output; diagnostics go to
// stderr.

#include "transport/epoll_transport.hpp"
#include "transport/task.hpp"

#include <hdr/hdr_histogram.h>

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <iostream>
#include <signal.h>
#include <string>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
using weir::transport::EpollTransport;
using weir::transport::Error;
using weir::transport::Handle;
using weir::transport::PeerClosed;
using weir::transport::Read;
using weir::transport::Readable;
using weir::transport::Writable;
using weir::transport::Write;

struct Options {
  std::string server_bin;
  std::string backend = "epoll";
  std::string out_path;
  unsigned connections = 100;
  unsigned duration_s = 15;
  unsigned warmup_s = 2;
  unsigned window = 32;
  unsigned payload = 16;
  unsigned port = 0;
  unsigned metrics_port = 0;
};

struct Stats {
  hdr_histogram* hist = nullptr;
  std::uint64_t ops = 0;
  std::uint64_t err_replies = 0;
  std::uint64_t failed_conns = 0;
  std::int64_t warmup_ns = 0;
};

std::uint32_t checksum(const char* data, std::size_t size) {
  std::uint32_t c = 2166136261U;
  for (std::size_t i = 0; i < size; ++i) {
    c ^= static_cast<std::uint8_t>(data[i]);
    c *= 16777619U;
  }
  return c;
}

std::string make_frame(std::uint64_t id, unsigned payload_size) {
  std::string frame;
  frame.reserve(16 + payload_size + 4);
  frame += "WR01";
  const std::uint64_t be_id = __builtin_bswap64(id);
  frame.append(reinterpret_cast<const char*>(&be_id), sizeof(be_id));
  const std::uint32_t be_len = __builtin_bswap32(payload_size);
  frame.append(reinterpret_cast<const char*>(&be_len), sizeof(be_len));
  frame.append(payload_size, 'x');
  const std::uint32_t crc =
      checksum(frame.data() + 16, static_cast<std::size_t>(payload_size));
  const std::uint32_t be_crc = __builtin_bswap32(crc);
  frame.append(reinterpret_cast<const char*>(&be_crc), sizeof(be_crc));
  return frame;
}

unsigned free_port() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  socklen_t len = sizeof(addr);
  getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
  const unsigned port = ntohs(addr.sin_port);
  close(fd);
  return port;
}

pid_t spawn_server(const Options& options, int& status_pipe_out) {
  int pipe_fds[2];
  if (pipe(pipe_fds) != 0) return -1;
  const pid_t pid = fork();
  if (pid < 0) return -1;
  if (pid == 0) {
    close(pipe_fds[0]);
    dup2(pipe_fds[1], STDERR_FILENO);
    close(pipe_fds[1]);
    std::string port = std::to_string(options.port);
    std::string metrics = std::to_string(options.metrics_port);
    // Disk-backed directory: /tmp is tmpfs on modern Ubuntu and the
    // persistence log grows ~660 bytes per accepted event, so a campaign
    // run can write gigabytes. The log is scratch state for the bench run
    // and is unlinked once the server exits.
    std::string log = "/var/tmp/weir-bench-" + std::to_string(getpid()) + ".log";
    execl(options.server_bin.c_str(), options.server_bin.c_str(), "--port",
          port.c_str(), "--metrics-port", metrics.c_str(), "--log",
          log.c_str(), "--backend", options.backend.c_str(), nullptr);
    _exit(127);
  }
  close(pipe_fds[1]);
  status_pipe_out = pipe_fds[0];
  return pid;
}

bool wait_ready(unsigned port, int status_pipe, unsigned timeout_s) {
  const auto deadline = Clock::now() + std::chrono::seconds(timeout_s);
  while (Clock::now() < deadline) {
    int status = 0;
    const pid_t r = waitpid(status_pipe < 0 ? -1 : 0, &status, WNOHANG);
    (void)r;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_port = htons(static_cast<std::uint16_t>(port));
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
        close(fd);
        return true;
      }
      close(fd);
    }
    usleep(50'000);
  }
  (void)status_pipe;
  return false;
}

std::uint64_t rss_kb(pid_t pid) {
  char path[64];
  snprintf(path, sizeof(path), "/proc/%d/status", static_cast<int>(pid));
  std::FILE* f = std::fopen(path, "r");
  if (!f) return 0;
  char line[256];
  unsigned long long kb = 0;
  while (std::fgets(line, sizeof(line), f)) {
    if (std::sscanf(line, "VmRSS: %llu kB", &kb) == 1) break;
  }
  std::fclose(f);
  return static_cast<std::uint64_t>(kb);
}

struct ConnState {
  Handle handle = 0;
  int fd = -1;
  bool established = false;
  bool failed = false;
  bool counted = false;
  bool finishing = false;
  std::string outbox;
  std::string partial_line;
  std::deque<std::int64_t> sent_at;
  std::uint64_t sent = 0;
  std::uint64_t received = 0;
};

struct ClientContext {
  EpollTransport& io;
  const Options& options;
  Stats& stats;
  const std::int64_t deadline_ns;
  const std::int64_t warmup_end_ns;
  std::vector<ConnState> conns;
};

// A connection is counted as failed at most once, on the alive-to-failed
// transition. Dead connections are closed through the transport immediately:
// level-triggered error conditions (EPOLLERR/EPOLLHUP) keep re-reporting for
// as long as the fd stays registered, which would inflate the failure count
// and busy-loop the event loop.
static void fail_conn(ClientContext& ctx, ConnState& c, const char* why,
                      int err = 0) {
  if (c.counted) return;
  c.counted = true;
  c.failed = true;
  ++ctx.stats.failed_conns;
  fprintf(stderr, "FAIL idx=%td sent=%llu recv=%llu why=%s errno=%d\n",
          &c - ctx.conns.data(),
          static_cast<unsigned long long>(c.sent),
          static_cast<unsigned long long>(c.received), why, err);
  if (c.handle != 0) {
    ctx.io.close(c.handle);
    c.handle = 0;
    c.fd = -1;
  }
}

std::int64_t now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             Clock::now().time_since_epoch())
      .count();
}

void update_interest(ClientContext& ctx, ConnState& c) {
  std::uint32_t interest = 0;
  if (!c.established) {
    interest = Write;
  } else if (!c.failed) {
    interest = Read;
    if (!c.outbox.empty()) interest |= Write;
  }
  // Always push the interest through, including zero: a failed or finished
  // connection must stop being polled or the transport keeps reporting its
  // level-triggered error condition and the loop spins on it.
  if (c.handle != 0) ctx.io.set_interest(c.handle, interest);
}

void try_send(ClientContext& ctx, ConnState& c) {
  if (c.failed || c.finishing) return;
  // Top the pipeline up to `window` frames in flight; whatever remains
  // unsent stays in the outbox for the next writable pass.
  while (c.sent - c.received < ctx.options.window &&
         now_ns() < ctx.deadline_ns) {
    c.outbox += make_frame(c.sent + 1, ctx.options.payload);
    c.sent_at.push_back(now_ns());
    ++c.sent;
  }
  while (!c.outbox.empty()) {
    const auto r =
        ctx.io.write(c.handle, c.outbox.data(), c.outbox.size());
    if (r > 0) {
      c.outbox.erase(0, static_cast<std::size_t>(r));
      continue;
    }
    if (r < 0 && errno == EINTR) continue;
    if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
    fail_conn(ctx, c, "write", errno);
    return;
  }
}

void recv_replies(ClientContext& ctx, ConnState& c) {
  char buffer[1 << 16];
  while (true) {
    const auto r = ctx.io.read(c.handle, buffer, sizeof(buffer));
    if (r > 0) {
      c.partial_line.append(buffer, static_cast<std::size_t>(r));
      std::size_t start = 0;
      while (true) {
        const std::size_t nl = c.partial_line.find('\n', start);
        if (nl == std::string::npos) break;
        const std::string line = c.partial_line.substr(0, nl);
        c.partial_line.erase(0, nl + 1);
        const bool ok = line.rfind("OK ", 0) == 0;
        const bool err = line.rfind("ERR ", 0) == 0;
        if (ok || err) {
          // Replies arrive one per sent frame (OK or in-band rejection), so
          // each reply retires exactly one sent timestamp.
          const std::int64_t sent = c.sent_at.empty() ? 0 : c.sent_at.front();
          if (!c.sent_at.empty()) c.sent_at.pop_front();
          if (sent >= ctx.warmup_end_ns) {
            if (ok) {
              hdr_record_value(ctx.stats.hist, now_ns() - sent);
              ++ctx.stats.ops;
            } else {
              ++ctx.stats.err_replies;
            }
          }
          ++c.received;
        } else {
          // Anything else is a protocol anomaly; count it as a failed reply
          // so malformed output cannot silently inflate throughput.
          fail_conn(ctx, c, "anomaly");
        }
        start = 0;
      }
      continue;
    }
    if (r < 0 && errno == EINTR) continue;
    if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
    fail_conn(ctx, c, "read", errno);
    return;
  }
}

weir::transport::RootTask run_load(ClientContext& ctx) {
  for (auto& c : ctx.conns) {
#ifdef __linux__
    c.fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
#else
    c.fd = socket(AF_INET, SOCK_STREAM, 0);
#endif
    if (c.fd < 0) {
      fail_conn(ctx, c, "socket", errno);
      continue;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<std::uint16_t>(ctx.options.port));
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    const int r = connect(c.fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (r != 0 && errno != EINPROGRESS) {
      fail_conn(ctx, c, "connect", errno);
      continue;
    }
    c.handle = ctx.io.listen(c.fd, Write);
    if (c.handle == 0) {
      fail_conn(ctx, c, "listen");
    }
  }
  while (true) {
    bool any_alive = false;
    for (const auto& c : ctx.conns) {
      if (!c.failed && !c.finishing) {
        any_alive = true;
        break;
      }
    }
    if (!any_alive) break;
    auto events = co_await ctx.io.next_events();
    for (const auto& event : events) {
      for (auto& c : ctx.conns) {
        if (c.handle != event.handle) continue;
        if ((event.events & Error) != 0U) {
          int error = 0;
          socklen_t elen = sizeof(error);
          if (getsockopt(c.fd, SOL_SOCKET, SO_ERROR, &error, &elen) != 0) {
            fail_conn(ctx, c, "error_event_no_soerror", errno);
          } else {
            fail_conn(ctx, c, "error_event", error);
          }
        }
        if ((event.events & PeerClosed) != 0U) {
          if (c.received < c.sent) {
            fail_conn(ctx, c, "peer_closed");
          }
        }
        if ((event.events & Writable) != 0U) {
          if (!c.established) {
            int error = 0;
            socklen_t len = sizeof(error);
            if (getsockopt(c.fd, SOL_SOCKET, SO_ERROR, &error, &len) != 0 ||
                error != 0) {
              fail_conn(ctx, c, "connect_soerror", error);
            } else {
              c.established = true;
              try_send(ctx, c);
            }
          } else {
            try_send(ctx, c);
          }
        }
        if ((event.events & Readable) != 0U) {
          recv_replies(ctx, c);
          // Replies free pipeline slots: send more in the same pass.
          try_send(ctx, c);
        }
        update_interest(ctx, c);
      }
    }
    if (now_ns() >= ctx.deadline_ns) {
      for (auto& c : ctx.conns) c.finishing = true;
    }
  }
  for (auto& c : ctx.conns) {
    // The transport owns the fd once listen() succeeded: close it through the
    // transport and drop the raw fd to avoid a double close. Connections that
    // never registered keep their raw fd and are closed directly.
    if (c.handle != 0) {
      ctx.io.close(c.handle);
      c.fd = -1;
    }
    if (c.fd >= 0) ::close(c.fd);
  }
}

bool parse_args(int argc, char** argv, Options& options) {
  auto require = [&](int& i) -> const char* {
    if (i + 1 >= argc) return nullptr;
    return argv[++i];
  };
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--help") {
      std::cout
          << "usage: weir_bench --server PATH --backend NAME\n"
          << "  [--connections N] [--duration S] [--warmup S] [--window K]\n"
          << "  [--payload N] [--out FILE]\n";
      return false;
    }
    const char* value = require(i);
    if (!value) return false;
    if (a == "--server") {
      options.server_bin = value;
    } else if (a == "--backend") {
      options.backend = value;
    } else if (a == "--out") {
      options.out_path = value;
    } else if (a == "--connections" || a == "--duration" ||
               a == "--warmup" || a == "--window" || a == "--payload") {
      char* end = nullptr;
      const unsigned long v = std::strtoul(value, &end, 10);
      if (end == value || *end != '\0' || v > 1'000'000U) return false;
      if (a == "--connections")
        options.connections = static_cast<unsigned>(v);
      else if (a == "--duration")
        options.duration_s = static_cast<unsigned>(v);
      else if (a == "--warmup")
        options.warmup_s = static_cast<unsigned>(v);
      else if (a == "--window")
        options.window = static_cast<unsigned>(v);
      else
        options.payload = static_cast<unsigned>(v);
    } else {
      return false;
    }
  }
  return !options.server_bin.empty() && options.connections > 0 &&
         options.duration_s > 0 && options.window > 0;
}

std::string json_escape(const std::string& s) {
  std::string out;
  for (char c : s) {
    if (c == '"' || c == '\\') out += '\\';
    out += c;
  }
  return out;
}
}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse_args(argc, argv, options)) return 2;
  if (options.backend != "epoll" && options.backend != "coroutine" &&
      options.backend != "io_uring") {
    std::cerr << "unknown backend: " << options.backend << '\n';
    return 2;
  }
  rlimit nofile{65535, 65535};
  setrlimit(RLIMIT_NOFILE, &nofile);

  options.port = free_port();
  options.metrics_port = free_port();
  int status_pipe = -1;
  const pid_t server_pid = spawn_server(options, status_pipe);
  if (server_pid < 0) {
    std::cerr << "failed to spawn server\n";
    return 1;
  }
  if (!wait_ready(options.port, status_pipe, 15)) {
    std::cerr << "server did not become ready\n";
    kill(server_pid, SIGKILL);
    waitpid(server_pid, nullptr, 0);
    return 1;
  }

  hdr_histogram* hist = nullptr;
  hdr_init(1, 60LL * 1000 * 1000 * 1000, 3, &hist);
  Stats stats;
  stats.hist = hist;
  stats.warmup_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::seconds(options.warmup_s))
                        .count();

  EpollTransport io;
  ClientContext ctx{io,
                    options,
                    stats,
                    now_ns() +
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::seconds(options.duration_s))
                            .count(),
                    now_ns() + stats.warmup_ns,
                    std::vector<ConnState>(options.connections)};
  const auto start = Clock::now();
  io.run(run_load(ctx));
  const double wall_s =
      std::chrono::duration<double>(Clock::now() - start).count();

  const std::uint64_t rss = rss_kb(server_pid);
  kill(server_pid, SIGTERM);
  int status = 0;
  waitpid(server_pid, &status, 0);
  const int server_exit = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
  if (status_pipe >= 0) close(status_pipe);
  std::string log = "/var/tmp/weir-bench-" + std::to_string(server_pid) + ".log";
  unlink(log.c_str());

  const double measured_s =
      std::max(wall_s - static_cast<double>(options.warmup_s), 0.001);
  const double throughput =
      measured_s > 0 ? static_cast<double>(stats.ops) / measured_s : 0.0;
  const double p50 =
      static_cast<double>(hdr_value_at_percentile(hist, 50.0)) / 1000.0;
  const double p90 =
      static_cast<double>(hdr_value_at_percentile(hist, 90.0)) / 1000.0;
  const double p99 =
      static_cast<double>(hdr_value_at_percentile(hist, 99.0)) / 1000.0;
  const double p999 =
      static_cast<double>(hdr_value_at_percentile(hist, 99.9)) / 1000.0;
  const double max_us =
      static_cast<double>(hdr_max(hist)) / 1000.0;
  hdr_close(hist);

  char result[4096];
  snprintf(result, sizeof(result),
           "{\"backend\":\"%s\",\"connections\":%u,\"payload\":%u,"
           "\"window\":%u,\"duration_s\":%.3f,\"warmup_s\":%u,"
           "\"ops\":%llu,\"err_replies\":%llu,"
           "\"throughput_ops_s\":%.1f,\"p50_us\":%.3f,\"p90_us\":%.3f,"
           "\"p99_us\":%.3f,\"p999_us\":%.3f,\"max_us\":%.3f,"
           "\"rss_kb\":%llu,\"failed_conns\":%llu,\"server_exit\":%d}\n",
           json_escape(options.backend).c_str(), options.connections,
           options.payload, options.window, wall_s, options.warmup_s,
           static_cast<unsigned long long>(stats.ops),
           static_cast<unsigned long long>(stats.err_replies), throughput,
           p50, p90, p99, p999, max_us,
           static_cast<unsigned long long>(rss),
           static_cast<unsigned long long>(stats.failed_conns), server_exit);
  if (!options.out_path.empty()) {
    std::FILE* f = std::fopen(options.out_path.c_str(), "w");
    if (f) {
      std::fputs(result, f);
      std::fclose(f);
    } else {
      std::cerr << "cannot write " << options.out_path << '\n';
      return 1;
    }
  } else {
    std::fputs(result, stdout);
  }
  return 0;
}