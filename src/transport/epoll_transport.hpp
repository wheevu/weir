#pragma once

#include "task.hpp"

#include <cstddef>
#include <cstdint>
#include <coroutine>
#include <functional>
#include <system_error>
#include <vector>

namespace weir::transport {

using Handle = std::uint64_t;
enum Interest : std::uint32_t { Read = 1U << 0, Write = 1U << 1 };
enum EventFlag : std::uint32_t { Readable = 1U << 0, Writable = 1U << 1, PeerClosed = 1U << 2, Error = 1U << 3 };
struct Event { Handle handle{}; std::uint32_t events{}; };
using EventBatch = std::vector<Event>;

class EpollTransport {
 public:
  using Post = std::function<void()>;
  explicit EpollTransport(std::size_t max_events = 64);
  ~EpollTransport();
  EpollTransport(const EpollTransport&) = delete;
  EpollTransport& operator=(const EpollTransport&) = delete;
  bool valid() const noexcept;
  std::error_code error() const noexcept;
  Handle listen(int fd, std::uint32_t interest = Read);
  Handle accept(Handle listener);
  std::ptrdiff_t read(Handle, void*, std::size_t);
  std::ptrdiff_t write(Handle, const void*, std::size_t);
  bool set_interest(Handle, std::uint32_t);
  bool close(Handle) noexcept;
  bool post(Post);

  struct NextEvents {
    EpollTransport* transport;
    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<> continuation) noexcept;
    EventBatch await_resume();
  };
  NextEvents next_events() noexcept { return {this}; }
  void run(RootTask task);

 private:
  friend struct NextEvents;
  struct Impl;
  Impl* impl_{};
};
}
