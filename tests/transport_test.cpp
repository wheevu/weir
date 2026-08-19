#include "../src/transport/task.hpp"
#include "../src/transport/uring_ring.hpp"
#include "../src/transport/epoll_transport.hpp"
#include "../src/transport/uring_transport.hpp"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#ifdef __linux__
#include <linux/io_uring.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace {
struct FrameGuard {
  std::atomic<int>* destroyed;
  ~FrameGuard() { ++*destroyed; }
};

void check(bool value, const char* message) {
  if (!value) {
    std::cerr << "transport test failure: " << message << '\n';
    std::exit(1);
  }
}

weir::transport::RootTask root_task(int& state) {
  state = 1;
  co_await std::suspend_always{};
  state = 2;
}

weir::transport::RootTask throwing_task() {
  throw std::runtime_error("expected coroutine error");
  co_return;
}

weir::transport::RootTask suspended_task(int& state) {
  state = 1;
  co_await std::suspend_always{};
  state = 2;
}

weir::transport::RootTask guarded_task(std::atomic<int>& destroyed) {
  FrameGuard guard{&destroyed};
  co_await std::suspend_always{};
}

#ifdef __linux__
weir::transport::RootTask wait_for_read(weir::transport::EpollTransport& transport,
                                        weir::transport::Handle handle,
                                        bool& observed) {
  auto events = co_await transport.next_events();
  for (const auto& event : events) {
    if (event.handle == handle && (event.events & 1U) != 0U) observed = true;
  }
}
weir::transport::RootTask wait_for_uring_read(weir::transport::UringTransport& transport,
                                              weir::transport::Handle handle,
                                              bool& observed) {
  auto events = co_await transport.next_events();
  for (const auto& event : events) {
    if (event.handle == handle && (event.events & weir::transport::Readable) != 0U) observed = true;
  }
}
#endif
}  // namespace

int main() {
  int state = 0;
  auto task = root_task(state);
  check(state == 0, "root task must start suspended");
  task.resume();
  check(state == 1 && !task.done(), "root task first resume");
  task.resume();
  check(state == 2 && task.done(), "root task second resume");
  auto moved = std::move(task);
  check(!task.valid() && moved.done(), "root task move ownership");
  bool caught = false;
  try {
    auto failing = throwing_task();
    failing.resume();
  } catch (const std::runtime_error&) {
    caught = true;
  }
  check(caught, "root task propagates exceptions");
  int suspended_state = 0;
  auto suspended = suspended_task(suspended_state);
  suspended.resume();
  check(suspended_state == 1 && !suspended.done(), "suspended task setup");
  auto replacement = root_task(state);
  replacement = std::move(suspended);
  check(!suspended.valid() && replacement.valid(), "root task move assignment");
  replacement.destroy();
  check(!replacement.valid(), "root task explicit destroy");
  std::atomic<int> destroyed{0};
  auto guarded = guarded_task(destroyed);
  guarded.resume();
  guarded.destroy();
  check(destroyed == 1, "suspended coroutine frame destruction");

  weir::transport::UringRing ring(8);
#ifdef __linux__
  if (ring.valid()) {
    check(ring.error().value() == 0, "valid ring has no error");
    auto* sqe = ring.acquire(7);
    check(sqe != nullptr, "acquire SQE");
    sqe->opcode = IORING_OP_NOP;
    check(ring.submit(1), "submit NOP");
    bool saw_completion = false;
    check(ring.drain([&](const weir::transport::Completion& completion) {
      saw_completion = completion.user_data == 7 && completion.result == 0;
    }) == 1, "drain NOP completion");
    check(saw_completion, "NOP completion contents");
    int pair[2]{};
    check(socketpair(AF_UNIX, SOCK_STREAM, 0, pair) == 0, "io_uring socketpair");
    auto* poll_sqe = ring.acquire(8);
    check(poll_sqe != nullptr, "acquire poll SQE");
    poll_sqe->opcode = IORING_OP_POLL_ADD;
    poll_sqe->fd = pair[0];
    poll_sqe->poll_events = POLLIN;
    check(ring.submit(), "submit poll");
    const char byte = 'p';
    check(write(pair[1], &byte, 1) == 1, "io_uring socketpair write");
    check(ring.submit(1), "wait for poll");
    bool saw_poll = false;
    check(ring.drain([&](const weir::transport::Completion& completion) {
      saw_poll = completion.user_data == 8 && (completion.result & POLLIN) != 0;
    }) == 1, "drain poll completion");
    check(saw_poll, "poll completion contents");
    close(pair[0]);
    close(pair[1]);
  } else {
    check(static_cast<bool>(ring.error()), "invalid ring reports an error");
  }
#else
  check(!ring.valid(), "io_uring is unavailable on non-Linux");
#endif

#ifdef __linux__
  int sockets[2]{};
  check(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0, "socketpair");
  weir::transport::EpollTransport transport;
  auto handle = transport.listen(sockets[0]);
  check(handle != 0, "epoll transport registration");
  const char byte = 'x';
  check(write(sockets[1], &byte, 1) == 1, "socketpair write");
  bool observed = false;
  transport.run(wait_for_read(transport, handle, observed));
  check(observed, "epoll transport event delivery");
  close(sockets[1]);

  weir::transport::EpollTransport callback_transport;
  bool posted = false;
  auto callback_task = [&]() -> weir::transport::RootTask {
    (void)co_await callback_transport.next_events();
    posted = true;
  };
  check(callback_transport.post([&] { posted = true; }), "post callback");
  callback_transport.run(callback_task());
  check(posted, "post callback execution");
  check(!callback_transport.post([] {}), "post rejected after run");

  int uring_sockets[2]{};
  weir::transport::UringTransport uring_transport;
  if (uring_transport.valid()) {
    check(socketpair(AF_UNIX, SOCK_STREAM, 0, uring_sockets) == 0, "uring transport socketpair");
    auto uring_handle = uring_transport.listen(uring_sockets[0]);
    check(uring_handle != 0, "uring transport registration");
    check(write(uring_sockets[1], &byte, 1) == 1, "uring transport socketpair write");
    bool uring_observed = false;
    uring_transport.run(wait_for_uring_read(uring_transport, uring_handle, uring_observed));
    check(uring_observed, "uring transport event delivery");
    close(uring_sockets[1]);

    // A suspended task must still let run() return when no fd ever fires:
    // the transport wakes the waiter periodically (100ms timeout SQE), the
    // task observes nothing and completes, and run() exits instead of
    // blocking forever. Regression test for clean SIGTERM shutdown.
    weir::transport::UringTransport idle_transport;
    if (idle_transport.valid()) {
      bool waited = false;
      auto one_wait = [&]() -> weir::transport::RootTask {
        (void)co_await idle_transport.next_events();
        waited = true;
      };
      idle_transport.run(one_wait());
      check(waited, "uring run returns after a single await with no events");
    }
  }
#endif
  std::cout << "transport tests passed\n";
}
