#include "epoll_transport.hpp"

#include <algorithm>
#include <cerrno>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <utility>

#ifdef __linux__
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace weir::transport {
struct EpollTransport::Impl {
  std::error_code error;
  EventBatch batch;
#ifdef __linux__
  struct Entry { int fd; };
  int epoll{-1};
  int wake{-1};
  std::size_t max_events;
  Handle next_handle{1};
  std::unordered_map<Handle, Entry> entries;
  std::mutex mutex;
  std::deque<Post> posts;
  bool stopped{false};
  std::coroutine_handle<> waiter{};
  explicit Impl(std::size_t n) : max_events(n == 0 ? 1 : n) {
    epoll = epoll_create1(EPOLL_CLOEXEC);
    wake = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (epoll < 0 || wake < 0) { error = {errno, std::generic_category()}; return; }
    epoll_event event{}; event.events = EPOLLIN; event.data.u64 = 0;
    if (epoll_ctl(epoll, EPOLL_CTL_ADD, wake, &event) < 0)
      error = {errno, std::generic_category()};
  }
  ~Impl() {
    for (const auto& [_, entry] : entries) ::close(entry.fd);
    if (wake >= 0) ::close(wake);
    if (epoll >= 0) ::close(epoll);
  }
  Handle add(int fd, std::uint32_t interest) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return 0;
    Handle handle = next_handle++;
    epoll_event event{};
    event.events = (interest & Read ? EPOLLIN | EPOLLRDHUP : 0U) |
                   (interest & Write ? EPOLLOUT : 0U);
    event.data.u64 = handle;
    if (epoll_ctl(epoll, EPOLL_CTL_ADD, fd, &event) < 0) return 0;
    entries.emplace(handle, Entry{fd});
    return handle;
  }
#else
  explicit Impl(std::size_t) : error(std::make_error_code(std::errc::operation_not_supported)) {}
  ~Impl() = default;
#endif
};

EpollTransport::EpollTransport(std::size_t n) : impl_(new Impl(n)) {}
EpollTransport::~EpollTransport() { delete impl_; }
bool EpollTransport::valid() const noexcept { return impl_ != nullptr && !impl_->error; }
std::error_code EpollTransport::error() const noexcept { return impl_->error; }

#ifdef __linux__
static std::uint32_t epoll_mask(std::uint32_t interest) {
  return (interest & Read ? EPOLLIN | EPOLLRDHUP : 0U) |
         (interest & Write ? EPOLLOUT : 0U);
}
#endif

Handle EpollTransport::listen(int fd, std::uint32_t interest) {
#ifdef __linux__
  return valid() ? impl_->add(fd, interest) : 0;
#else
  (void)fd; (void)interest; return 0;
#endif
}
Handle EpollTransport::accept(Handle listener) {
#ifdef __linux__
  auto it = impl_->entries.find(listener);
  if (it == impl_->entries.end()) return 0;
  int fd = accept4(it->second.fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (fd < 0) return 0;
  Handle handle = impl_->add(fd, Read);
  if (handle == 0) ::close(fd);
  return handle;
#else
  (void)listener; return 0;
#endif
}
std::ptrdiff_t EpollTransport::read(Handle h, void* b, std::size_t n) {
#ifdef __linux__
  auto it = impl_->entries.find(h);
  return it == impl_->entries.end() ? -1 : recv(it->second.fd, b, n, 0);
#else
  (void)h; (void)b; (void)n; return -1;
#endif
}
std::ptrdiff_t EpollTransport::write(Handle h, const void* b, std::size_t n) {
#ifdef __linux__
  auto it = impl_->entries.find(h);
  return it == impl_->entries.end() ? -1 : send(it->second.fd, b, n, MSG_NOSIGNAL);
#else
  (void)h; (void)b; (void)n; return -1;
#endif
}
bool EpollTransport::set_interest(Handle h, std::uint32_t interest) {
#ifdef __linux__
  auto it = impl_->entries.find(h); if (it == impl_->entries.end()) return false;
  epoll_event event{}; event.events = epoll_mask(interest); event.data.u64 = h;
  return epoll_ctl(impl_->epoll, EPOLL_CTL_MOD, it->second.fd, &event) == 0;
#else
  (void)h; (void)interest; return false;
#endif
}
bool EpollTransport::close(Handle h) noexcept {
#ifdef __linux__
  auto it = impl_->entries.find(h); if (it == impl_->entries.end()) return false;
  epoll_ctl(impl_->epoll, EPOLL_CTL_DEL, it->second.fd, nullptr);
  ::close(it->second.fd); impl_->entries.erase(it); return true;
#else
  (void)h; return false;
#endif
}
bool EpollTransport::post(Post operation) {
#ifdef __linux__
  if (!operation) return false;
  std::uint64_t one = 1;
  std::lock_guard lock(impl_->mutex);
  if (impl_->stopped || impl_->wake < 0) return false;
  impl_->posts.push_back(std::move(operation));
  const ssize_t result = ::write(impl_->wake, &one, sizeof(one));
  if (result == static_cast<ssize_t>(sizeof(one)) || (result < 0 && errno == EAGAIN)) return true;
  impl_->posts.pop_back();
  return false;
#else
  (void)operation; return false;
#endif
}
bool EpollTransport::NextEvents::await_ready() const noexcept {
#ifdef __linux__
  return !transport->impl_->batch.empty();
#else
  return true;
#endif
}
void EpollTransport::NextEvents::await_suspend(std::coroutine_handle<> continuation) noexcept {
#ifdef __linux__
  transport->impl_->waiter = continuation;
#else
  (void)continuation;
#endif
}
EventBatch EpollTransport::NextEvents::await_resume() {
  return std::exchange(transport->impl_->batch, {});
}
void EpollTransport::run(RootTask task) {
#ifdef __linux__
  std::vector<epoll_event> events(impl_->max_events);
  auto resume = [&]() {
    try {
      task.resume();
      return true;
    } catch (...) {
      std::lock_guard lock(impl_->mutex);
      impl_->stopped = true;
      impl_->waiter = {};
      impl_->error = std::make_error_code(std::errc::operation_canceled);
      return false;
    }
  };
  while (valid() && !task.done()) {
    if (!impl_->waiter) { if (!resume()) break; continue; }
    int count;
    do { count = epoll_wait(impl_->epoll, events.data(), static_cast<int>(events.size()), 100); }
    while (count < 0 && errno == EINTR);
    if (count < 0) { impl_->error = {errno, std::generic_category()}; break; }
    for (int i = 0; i < count; ++i) {
      auto& event = events[static_cast<std::size_t>(i)];
      if (event.data.u64 == 0) {
        std::uint64_t value;
        while (::read(impl_->wake, &value, sizeof(value)) == sizeof(value)) {}
        std::deque<Post> posts;
        { std::lock_guard lock(impl_->mutex); posts.swap(impl_->posts); }
        try {
          while (!posts.empty()) { posts.front()(); posts.pop_front(); }
        } catch (...) {
          std::lock_guard lock(impl_->mutex);
          impl_->waiter = {};
          impl_->stopped = true;
          impl_->error = std::make_error_code(std::errc::operation_canceled);
          break;
        }
      } else {
        if (impl_->entries.find(event.data.u64) == impl_->entries.end()) continue;
        std::uint32_t flags = 0;
        if ((event.events & EPOLLIN) != 0) flags |= Readable;
        if ((event.events & EPOLLOUT) != 0) flags |= Writable;
        if ((event.events & (EPOLLRDHUP | EPOLLHUP)) != 0) flags |= PeerClosed;
        if ((event.events & EPOLLERR) != 0) flags |= Error;
        impl_->batch.push_back({event.data.u64, flags});
      }
    }
    {
      std::lock_guard lock(impl_->mutex);
      impl_->batch.erase(std::remove_if(impl_->batch.begin(), impl_->batch.end(),
                                        [&](const Event& event) {
                                          return impl_->entries.find(event.handle) == impl_->entries.end();
                                        }), impl_->batch.end());
    }
    auto continuation = std::exchange(impl_->waiter, {});
    if (continuation && !resume()) break;
  }
  {
    std::lock_guard lock(impl_->mutex);
    impl_->stopped = true;
  }
#else
  (void)task;
#endif
}
}  // namespace weir::transport
