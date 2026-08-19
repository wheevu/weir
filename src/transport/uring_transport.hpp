#pragma once

#include "task.hpp"
#include "epoll_transport.hpp"
#include "uring_ring.hpp"

namespace weir::transport {

class UringTransport {
 public:
  using Post = EpollTransport::Post;
  explicit UringTransport(std::size_t max_events = 64);
  ~UringTransport();
  UringTransport(const UringTransport&) = delete;
  UringTransport& operator=(const UringTransport&) = delete;
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
    UringTransport* transport;
    bool await_ready() const noexcept;
    void await_suspend(std::coroutine_handle<> continuation) noexcept;
    EventBatch await_resume();
  };
  NextEvents next_events() noexcept { return {this}; }
  void run(RootTask task);

 private:
 public:
  struct Impl;
 private:
  Impl* impl_{};
};

}  // namespace weir::transport
