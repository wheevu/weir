#include "weir/coroutine_server.hpp"
#include "transport/epoll_transport.hpp"
#include "transport/uring_transport.hpp"

#include <atomic>
#include <cerrno>
#include <map>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace weir {
namespace {
using transport::EpollTransport;
using transport::Handle;
using transport::Readable;
using transport::Writable;
using transport::PeerClosed;
using transport::Error;

struct Connection {
  Parser parser;
  std::string out;
  std::uint64_t generation{};
  std::uint64_t pending{};
  std::uint64_t next_reply{};
  std::uint64_t next_emit{};
  bool read_open{true};
  std::map<std::uint64_t, std::optional<std::string>> replies;
};

template<class Transport>
struct CoroutineServer {
  Transport& io;
  Log& log;
  Metrics& metrics;
  std::atomic<bool>& stop;
  Pipeline pipeline;
  Handle listener{};
  std::map<Handle, Connection> connections;
  std::uint64_t next_generation{0};
  std::uint64_t next_id{};
  CoroutineServer(Transport& transport, Log& log_file, Metrics& metric_store,
                  std::atomic<bool>& stopping, unsigned workers)
      : io(transport), log(log_file), metrics(metric_store), stop(stopping),
        pipeline(log_file, metric_store, workers) {}

  void update_interest(Handle handle) {
    auto it = connections.find(handle);
    if (it == connections.end()) return;
    auto& c = it->second;
    std::uint32_t interest = c.read_open ? transport::Read : 0U;
    if (!c.out.empty()) interest |= transport::Write;
    io.set_interest(handle, interest);
  }

  void drop(Handle handle) {
    io.close(handle);
    connections.erase(handle);
  }

  void emit_ready(Handle handle) {
    auto it = connections.find(handle);
    if (it == connections.end()) return;
    auto& c = it->second;
    while (true) {
      auto reply = c.replies.find(c.next_emit);
      if (reply == c.replies.end() || !reply->second) break;
      if (c.out.size() + reply->second->size() > 1024U * 1024U) {
        drop(handle);
        return;
      }
      c.out += *reply->second;
      c.replies.erase(reply);
      ++c.next_emit;
    }
    update_interest(handle);
  }

  void complete(Handle handle, std::uint64_t generation, std::uint64_t sequence,
                std::uint64_t id, bool ok) {
    auto it = connections.find(handle);
    if (it == connections.end() || it->second.generation != generation) return;
    auto& c = it->second;
    if (c.pending > 0) --c.pending;
    c.replies[sequence] = ok ? "OK " + std::to_string(id) + "\n"
                              : "ERR persistence\n";
    emit_ready(handle);
  }

  void accept_ready() {
    while (true) {
      Handle handle = io.accept(listener);
      if (handle == 0) break;
      Connection connection;
      connection.generation = ++next_generation;
      connections.emplace(handle, std::move(connection));
      metrics.inc("connections_total");
    }
    io.set_interest(listener, transport::Read);
  }

  void read_ready(Handle handle) {
    auto it = connections.find(handle);
    if (it == connections.end()) return;
    auto& c = it->second;
    std::uint8_t buffer[4096];
    while (true) {
      auto result = io.read(handle, buffer, sizeof(buffer));
      if (result == 0) { c.read_open = false; break; }
      if (result < 0) {
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        c.read_open = false; break;
      }
      for (auto& event : c.parser.feed(buffer, static_cast<std::size_t>(result))) {
        if (c.replies.size() >= 65536U) {
          drop(handle);
          return;
        }
        event.id = next_id++;
        const auto sequence = c.next_reply++;
        const auto generation = c.generation;
        ++c.pending;
        c.replies.emplace(sequence, std::nullopt);
        event.durable_completion = [this, handle, generation, sequence,
                                    id = event.id](bool ok) {
          io.post([this, handle, generation, sequence, id, ok] {
            complete(handle, generation, sequence, id, ok);
          });
        };
        if (!pipeline.try_submit(std::move(event))) {
          --c.pending;
          metrics.inc("overload_total");
          c.replies[sequence] = "ERR queue\n";
        }
      }
      if (c.parser.bad()) {
        // Malformed traffic poisons the parser: drop the connection, matching
        // the legacy backend, so the peer cannot hold a dead connection open.
        drop(handle);
        return;
      }
    }
    emit_ready(handle);
  }

  void write_ready(Handle handle) {
    auto it = connections.find(handle);
    if (it == connections.end()) return;
    auto& c = it->second;
    while (!c.out.empty()) {
      auto result = io.write(handle, c.out.data(), c.out.size());
      if (result > 0) { c.out.erase(0, static_cast<std::size_t>(result)); continue; }
      if (result < 0 && (errno == EINTR)) continue;
      if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
      drop(handle); return;
    }
    update_interest(handle);
  }

  void remove_closed() {
    for (auto it = connections.begin(); it != connections.end();) {
      auto& c = it->second;
      if (!c.read_open && c.pending == 0 && c.out.empty()) {
        io.close(it->first);
        it = connections.erase(it);
      } else ++it;
    }
  }

  transport::RootTask serve() {
    while (!stop) {
      auto events = co_await io.next_events();
      for (const auto& event : events) {
        if (event.handle == listener) { accept_ready(); continue; }
        if ((event.events & Error) != 0U) {
          auto it = connections.find(event.handle);
          if (it != connections.end()) it->second.read_open = false;
          update_interest(event.handle);
        }
        if ((event.events & Readable) != 0U) read_ready(event.handle);
        if ((event.events & PeerClosed) != 0U) {
          auto it = connections.find(event.handle);
          if (it != connections.end()) it->second.read_open = false;
          // Re-arm with the remaining interest (write-only while output is
          // pending) so pending replies are never stranded by a close.
          update_interest(event.handle);
        }
        if ((event.events & Writable) != 0U) write_ready(event.handle);
      }
      remove_closed();
    }
  }
};
}

template<class Transport>
int run_server_with_transport(unsigned port, Log& log, Metrics& metrics,
                         std::atomic<bool>& stop, unsigned workers) {
#ifdef __linux__
  if (port == 0 || port > 65535 || workers == 0) return 2;
  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (fd < 0) return 1;
  int yes = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<std::uint16_t>(port));
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      listen(fd, 256) != 0) { close(fd); return 1; }
  Transport io;
  CoroutineServer<Transport> server{io, log, metrics, stop, workers};
  server.listener = io.listen(fd);
  if (server.listener == 0) { close(fd); return 1; }
  server.next_id = log.recover();
  io.run(server.serve());
  return 0;
#else
  (void)port; (void)log; (void)metrics; (void)stop; (void)workers;
  return 2;
#endif
}

int run_coroutine_server(unsigned port, Log& log, Metrics& metrics,
                         std::atomic<bool>& stop, unsigned workers) {
  return run_server_with_transport<EpollTransport>(port, log, metrics, stop, workers);
}

int run_uring_server(unsigned port, Log& log, Metrics& metrics,
                     std::atomic<bool>& stop, unsigned workers) {
  return run_server_with_transport<transport::UringTransport>(port, log, metrics, stop, workers);
}


}
