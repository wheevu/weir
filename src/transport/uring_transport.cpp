#include "uring_transport.hpp"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef __linux__
#include <fcntl.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace weir::transport {

#ifdef __linux__
namespace {
constexpr std::uint64_t kCancelBit = 1ULL << 63;
constexpr std::size_t kTraceEntries = 1024;

// Trace codes (test-only diagnostics; zero I/O in the hot path).
enum : std::uint32_t {
  kTraceArm = 1,
  kTraceRemove = 2,
  kTraceCancelCqe = 3,
  kTraceCancelCqeStale = 4,
  kTracePollCqe = 5,
  kTracePollCqeStale = 6,
  kTracePollCqeNegative = 7,
  kTraceSetInterest = 8,
  kTraceClose = 9,
  kTraceReconcilePending = 10,
  kTraceSweep = 11,
  kTraceWake = 12,
  kTraceDeferSubmit = 13,
  kTraceSubmitFail = 14,
};

struct TraceRing {
  struct Entry {
    std::uint64_t iteration;
    std::uint64_t token;
    std::uint32_t code;
    std::uint32_t from_state;
    std::uint32_t to_state;
    std::uint32_t result;
  };
  Entry entries[kTraceEntries]{};
  std::size_t next = 0;

  void record(std::uint64_t iteration, std::uint64_t token, std::uint32_t code,
              std::uint32_t from, std::uint32_t to, std::uint32_t result) {
    entries[next % kTraceEntries] = {iteration, token, code, from, to, result};
    ++next;
  }
  void dump(const char* path) const {
    std::FILE* file = std::fopen(path, "w");
    if (!file) return;
    const std::size_t count = next < kTraceEntries ? next : kTraceEntries;
    std::fprintf(file, "# ring=%zu total=%zu\n", kTraceEntries, next);
    for (std::size_t i = 0; i < count; ++i) {
      const Entry& e = entries[(next - count + i) % kTraceEntries];
      std::fprintf(file, "%llu %llu %u %u %u %u\n",
                   static_cast<unsigned long long>(e.iteration),
                   static_cast<unsigned long long>(e.token), e.code,
                   e.from_state, e.to_state, e.result);
    }
    std::fclose(file);
  }
};

std::uint32_t poll_mask(std::uint32_t interest) {
  return static_cast<std::uint32_t>((interest & Read ? POLLIN | POLLPRI | POLLRDHUP : 0) |
                                    (interest & Write ? POLLOUT : 0));
}
// Token layout: handle in bits 0-31, poll generation in bits 32-62.
// Bit 63 is reserved for cancel tokens, so a poll token can never be
// classified as a cancel even after generation wraps (31-bit generation).
// Handles are capped at 32 bits by listen(); poll generations are masked to
// 31 bits on increment. Cancel tokens are kCancelBit | poll_token.
std::uint64_t token(Handle h, std::uint32_t generation) {
  return (static_cast<std::uint64_t>(generation & 0x7fffffffU) << 32) | h;
}
Handle token_handle(std::uint64_t value) { return value & 0xffffffffU; }
std::uint32_t token_generation(std::uint64_t value) {
  return static_cast<std::uint32_t>(value >> 32) & 0x7fffffffU;
}
}  // namespace
#endif

struct UringTransport::Impl {
  std::error_code error;
  EventBatch batch;
  std::coroutine_handle<> waiter{};
#ifdef __linux__
  enum class PollState : std::uint32_t { Idle, Armed, Cancelling };
  struct Entry {
    int fd;
    std::uint32_t interest;
    std::uint32_t poll_generation;
    std::uint64_t armed_token;
    std::uint32_t armed_mask;
    std::uint64_t cancel_token;
    PollState state{PollState::Idle};
    bool reconcile_pending{false};
  };
  UringRing ring;
  int wake{-1};
  std::size_t max_events;
  Handle next_handle{1};
  std::unordered_map<Handle, Entry> entries;
  std::mutex mutex;
  std::deque<Post> posts;
  bool stopped{false};
  bool wake_armed{false};
  bool timeout_armed{false};
  std::size_t pending_reconciles{0};
  std::uint64_t iterations{0};
  TraceRing trace;
  explicit Impl(std::size_t n) : ring(256), max_events(n == 0 ? 1 : n) {
    if (!ring.valid()) {
      error = ring.error();
      return;
    }
    wake = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake < 0) error = {errno, std::generic_category()};
  }
  ~Impl() {
    for (const auto& [_, entry] : entries) ::close(entry.fd);
    if (wake >= 0) ::close(wake);
  }
#else
  explicit Impl(std::size_t)
      : error(std::make_error_code(std::errc::operation_not_supported)) {}
  ~Impl() = default;
#endif
};

#ifdef __linux__
namespace {
// Single-owner poll state machine. Invariants:
// - At most one poll (or one remove targeting it) is in flight per entry.
// - State transitions happen only on the run-loop thread, inside reconcile()
//   or the completion handler; no direct arm/cancel anywhere else.
// - After every poll completion the poll is re-armed while interest is
//   non-zero (level-triggered emulation, matching epoll semantics), so an
//   event is never lost even if the application does not call set_interest.
// - A remove completion always reconciles, so the entry is re-armed with the
//   latest interest after the old poll is fully retired.
bool reconcile(UringTransport::Impl& impl, UringTransport::Impl::Entry& entry,
               Handle h) {
  if (entry.state == UringTransport::Impl::PollState::Cancelling) return true;
  const std::uint32_t mask = poll_mask(entry.interest);
  if (entry.state == UringTransport::Impl::PollState::Armed) {
    if (mask == entry.armed_mask) return true;
    auto* sqe = impl.ring.acquire(kCancelBit | entry.armed_token);
    if (!sqe) return false;
    sqe->opcode = IORING_OP_POLL_REMOVE;
    sqe->addr = entry.armed_token;
    entry.cancel_token = kCancelBit | entry.armed_token;
    entry.state = UringTransport::Impl::PollState::Cancelling;
    impl.trace.record(
        impl.iterations, entry.cancel_token, kTraceRemove,
        static_cast<std::uint32_t>(UringTransport::Impl::PollState::Armed),
        static_cast<std::uint32_t>(UringTransport::Impl::PollState::Cancelling),
        mask);
    return true;
  }
  if (mask != 0) {
    entry.poll_generation = (entry.poll_generation + 1) & 0x7fffffffU;
    const std::uint64_t poll_token = token(h, entry.poll_generation);
    auto* sqe = impl.ring.acquire(poll_token);
    if (!sqe) return false;
    sqe->opcode = IORING_OP_POLL_ADD;
    sqe->fd = entry.fd;
    sqe->poll32_events = mask;
    entry.armed_token = poll_token;
    entry.armed_mask = mask;
    entry.state = UringTransport::Impl::PollState::Armed;
    impl.trace.record(
        impl.iterations, poll_token, kTraceArm,
        static_cast<std::uint32_t>(UringTransport::Impl::PollState::Idle),
        static_cast<std::uint32_t>(UringTransport::Impl::PollState::Armed),
        mask);
    return true;
  }
  return true;
}

void mark_reconcile_pending(UringTransport::Impl& impl,
                            UringTransport::Impl::Entry& entry,
                            std::uint64_t token_value) {
  if (!entry.reconcile_pending) {
    entry.reconcile_pending = true;
    ++impl.pending_reconciles;
    impl.trace.record(impl.iterations, token_value, kTraceReconcilePending, 0,
                      0, 0);
  }
}

struct CompletionHandler {
  UringTransport::Impl& impl;

  void wake() {
    std::uint64_t value;
    while (::read(impl.wake, &value, sizeof(value)) == sizeof(value)) {
    }
    std::deque<UringTransport::Post> posts;
    {
      std::lock_guard lock(impl.mutex);
      posts.swap(impl.posts);
    }
    impl.trace.record(impl.iterations, 0, kTraceWake, 0, 0,
                      static_cast<std::uint32_t>(posts.size()));
    while (!posts.empty()) {
      posts.front()();
      posts.pop_front();
    }
  }

  void completion(const Completion& c) {
    if (c.user_data == kCancelBit) {
      // Periodic wake (IORING_OP_TIMEOUT): exactly kCancelBit, never a real
      // cancel (those carry a poll token in the low bits). One timeout is
      // in flight at a time; completion retires it so the next iteration can
      // re-arm.
      impl.timeout_armed = false;
      return;
    }
    if (c.user_data == 0) {
      impl.wake_armed = false;
      wake();
      return;
    }
    const bool is_cancel = (c.user_data & kCancelBit) != 0;
    const std::uint64_t poll_token = c.user_data & ~kCancelBit;
    const Handle h = token_handle(poll_token);
    auto it = impl.entries.find(h);
    // A completion for an unknown handle is either a stale completion from a
    // closed entry or a bogus token; both are safe to ignore because handles
    // are never reused.
    if (it == impl.entries.end()) return;
    auto& entry = it->second;
    if (is_cancel) {
      if (c.user_data != entry.cancel_token ||
          entry.state != UringTransport::Impl::PollState::Cancelling) {
        impl.trace.record(impl.iterations, c.user_data, kTraceCancelCqeStale,
                          static_cast<std::uint32_t>(entry.state),
                          static_cast<std::uint32_t>(entry.state), c.result);
        return;
      }
      const auto from = entry.state;
      entry.state = UringTransport::Impl::PollState::Idle;
      impl.trace.record(impl.iterations, c.user_data, kTraceCancelCqe,
                        static_cast<std::uint32_t>(from),
                        static_cast<std::uint32_t>(entry.state), c.result);
      if (!reconcile(impl, entry, h))
        mark_reconcile_pending(impl, entry, c.user_data);
      return;
    }
    if (c.user_data != entry.armed_token ||
        token_generation(poll_token) != entry.poll_generation) {
      impl.trace.record(impl.iterations, c.user_data, kTracePollCqeStale,
                        static_cast<std::uint32_t>(entry.state),
                        static_cast<std::uint32_t>(entry.state), c.result);
      return;
    }
    if (c.result < 0) {
      // The poll was canceled or failed (e.g. -ECANCELED after a remove won,
      // or the fd was closed underneath us). No event flags: negative results
      // must never be translated into garbage bits.
      impl.trace.record(impl.iterations, c.user_data, kTracePollCqeNegative,
                        static_cast<std::uint32_t>(entry.state),
                        static_cast<std::uint32_t>(entry.state), c.result);
      if (entry.state == UringTransport::Impl::PollState::Armed) {
        entry.state = UringTransport::Impl::PollState::Idle;
        if (!reconcile(impl, entry, h))
          mark_reconcile_pending(impl, entry, c.user_data);
      }
      return;
    }
    std::uint32_t flags = 0;
    if (c.result & (POLLIN | POLLPRI)) flags |= Readable;
    if (c.result & POLLOUT) flags |= Writable;
    if (c.result & (POLLHUP | POLLRDHUP)) flags |= PeerClosed;
    if (c.result & POLLERR) flags |= Error;
    if (flags != 0 && impl.batch.size() < impl.max_events)
      impl.batch.push_back({h, flags});
    const auto from = entry.state;
    if (entry.state == UringTransport::Impl::PollState::Armed) {
      entry.state = UringTransport::Impl::PollState::Idle;
      // Level-triggered emulation: re-arm with the current interest so a
      // persistent condition keeps delivering events regardless of what the
      // application does after this batch.
      if (!reconcile(impl, entry, h))
        mark_reconcile_pending(impl, entry, c.user_data);
    }
    impl.trace.record(impl.iterations, c.user_data, kTracePollCqe,
                      static_cast<std::uint32_t>(from),
                      static_cast<std::uint32_t>(entry.state), flags);
  }
};
}  // namespace
#endif

UringTransport::UringTransport(std::size_t n) : impl_(new Impl(n)) {}
UringTransport::~UringTransport() { delete impl_; }
bool UringTransport::valid() const noexcept { return impl_ && !impl_->error; }
std::error_code UringTransport::error() const noexcept { return impl_->error; }

Handle UringTransport::listen(int fd, std::uint32_t interest) {
#ifdef __linux__
  if (!valid()) return 0;
  if (impl_->next_handle > 0xffffffffU) return 0;  // 32-bit token handle space
  const int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return 0;
  const Handle h = impl_->next_handle++;
  auto [it, inserted] = impl_->entries.emplace(
      h, Impl::Entry{fd, interest, 0, 0, 0, 0, Impl::PollState::Idle, false});
  if (!reconcile(*impl_, it->second, h)) {
    impl_->entries.erase(h);
    return 0;
  }
  // A transient submit failure (EAGAIN or partial submission) leaves the
  // SQE queued; the run loop retries it, so the entry must stay registered.
  if (!impl_->ring.submit() && impl_->ring.error()) {
    impl_->entries.erase(h);
    return 0;
  }
  return h;
#else
  (void)fd;
  (void)interest;
  return 0;
#endif
}
Handle UringTransport::accept(Handle listener) {
#ifdef __linux__
  auto it = impl_->entries.find(listener);
  if (it == impl_->entries.end()) return 0;
  const int fd =
      accept4(it->second.fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
  if (fd < 0) return 0;
  if (const char* value = std::getenv("WEIR_TEST_SNDBUF")) {
    char* end = nullptr;
    const long parsed = std::strtol(value, &end, 10);
    if (end != value && *end == '\0' && parsed > 0 && parsed <= 1'048'576) {
      const int size = static_cast<int>(parsed);
      setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
    }
  }
  const Handle h = listen(fd, Read);
  if (!h) ::close(fd);
  return h;
#else
  (void)listener;
  return 0;
#endif
}
std::ptrdiff_t UringTransport::read(Handle h, void* b, std::size_t n) {
#ifdef __linux__
  auto it = impl_->entries.find(h);
  return it == impl_->entries.end() ? -1 : recv(it->second.fd, b, n, 0);
#else
  (void)h;
  (void)b;
  (void)n;
  return -1;
#endif
}
std::ptrdiff_t UringTransport::write(Handle h, const void* b, std::size_t n) {
#ifdef __linux__
  auto it = impl_->entries.find(h);
  return it == impl_->entries.end()
             ? -1
             : send(it->second.fd, b, n, MSG_NOSIGNAL);
#else
  (void)h;
  (void)b;
  (void)n;
  return -1;
#endif
}
bool UringTransport::set_interest(Handle h, std::uint32_t interest) {
#ifdef __linux__
  auto it = impl_->entries.find(h);
  if (it == impl_->entries.end()) return false;
  auto& entry = it->second;
  entry.interest = interest;
  impl_->trace.record(impl_->iterations, h, kTraceSetInterest,
                      static_cast<std::uint32_t>(entry.state),
                      static_cast<std::uint32_t>(entry.state), interest);
  if (!reconcile(*impl_, entry, h)) mark_reconcile_pending(*impl_, entry, h);
  return impl_->ring.submit();
#else
  (void)h;
  (void)interest;
  return false;
#endif
}
bool UringTransport::close(Handle h) noexcept {
#ifdef __linux__
  auto it = impl_->entries.find(h);
  if (it == impl_->entries.end()) return false;
  impl_->trace.record(impl_->iterations, h, kTraceClose,
                      static_cast<std::uint32_t>(it->second.state),
                      static_cast<std::uint32_t>(it->second.state), 0);
  // Closing the fd cancels any kernel-side poll; completions for this entry
  // are ignored afterwards because the handle is never reused.
  ::close(it->second.fd);
  impl_->entries.erase(it);
  return true;
#else
  (void)h;
  return false;
#endif
}
bool UringTransport::post(Post operation) {
#ifdef __linux__
  if (!operation) return false;
  std::uint64_t one = 1;
  std::lock_guard lock(impl_->mutex);
  if (impl_->stopped) return false;
  impl_->posts.push_back(std::move(operation));
  if (::write(impl_->wake, &one, sizeof(one)) == sizeof(one) || errno == EAGAIN)
    return true;
  impl_->posts.pop_back();
  return false;
#else
  (void)operation;
  return false;
#endif
}
bool UringTransport::NextEvents::await_ready() const noexcept {
  return !transport->impl_->batch.empty();
}
void UringTransport::NextEvents::await_suspend(
    std::coroutine_handle<> continuation) noexcept {
  transport->impl_->waiter = continuation;
}
EventBatch UringTransport::NextEvents::await_resume() {
  return std::exchange(transport->impl_->batch, {});
}

void UringTransport::run(RootTask task) {
#ifdef __linux__
  auto resume = [&] {
    try {
      task.resume();
      return true;
    } catch (...) {
      impl_->error = std::make_error_code(std::errc::operation_canceled);
      return false;
    }
  };
  // Test-only failpoints (WEIR_URING_SEED): with a seed set, completions are
  // processed in a seed-chosen order (cancels first or polls first) and
  // submissions are occasionally deferred by one iteration, sweeping both
  // kernel interleavings deterministically. Unset: zero effect, no overhead.
  bool failpoints_enabled = false;
  std::uint64_t rng_state = 0;
  if (const char* seed = std::getenv("WEIR_URING_SEED")) {
    char* end = nullptr;
    const auto value = std::strtoull(seed, &end, 10);
    if (end != seed && *end == '\0' && value != 0) {
      failpoints_enabled = true;
      rng_state = value;
    }
  }
  auto rng = [&rng_state] {
    std::uint64_t x = rng_state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    rng_state = x;
    return x;
  };
  // Kernel-owned storage for the periodic wake: its address must stay valid
  // until the kernel consumes the SQE, which can happen in a later iteration
  // when a submit is retried after EAGAIN or deferred by the failpoints.
  timespec timeout{0, 100'000'000};  // 100ms periodic wake, mirrors epoll
  while (valid() && !task.done()) {
    ++impl_->iterations;
    if (!impl_->waiter) {
      if (!resume()) break;
      continue;
    }
    if (impl_->pending_reconciles > 0) {
      impl_->trace.record(impl_->iterations, 0, kTraceSweep, 0, 0,
                          static_cast<std::uint32_t>(impl_->pending_reconciles));
      for (auto& [h, entry] : impl_->entries) {
        if (entry.reconcile_pending && reconcile(*impl_, entry, h)) {
          entry.reconcile_pending = false;
          --impl_->pending_reconciles;
        }
      }
    }
    // At most one wake poll and one periodic timeout are in flight at a
    // time; both are re-armed after their completion retires them, so idle
    // periods never accumulate kernel requests.
    if (!impl_->wake_armed) {
      auto* wake_sqe = impl_->ring.acquire(0);
      if (!wake_sqe) {
        impl_->error = std::make_error_code(std::errc::no_buffer_space);
        break;
      }
      wake_sqe->opcode = IORING_OP_POLL_ADD;
      wake_sqe->fd = impl_->wake;
      wake_sqe->poll32_events = POLLIN;
      impl_->wake_armed = true;
    }
    if (impl_->waiter && !impl_->timeout_armed) {
      auto* timeout_sqe = impl_->ring.acquire(kCancelBit);
      if (!timeout_sqe) {
        impl_->error = std::make_error_code(std::errc::no_buffer_space);
        break;
      }
      timeout_sqe->opcode = IORING_OP_TIMEOUT;
      timeout_sqe->addr = reinterpret_cast<std::uint64_t>(&timeout);
      timeout_sqe->len = 1;
      impl_->timeout_armed = true;
    }
    if (!impl_->ring.submit(1)) {
      impl_->trace.record(impl_->iterations, 0, kTraceSubmitFail, 0, 0,
                          static_cast<std::uint32_t>(impl_->ring.error().value()));
      if (impl_->ring.error()) break;
      continue;
    }
    CompletionHandler handler{*impl_};
    try {
      if (failpoints_enabled) {
        std::vector<Completion> cancels, polls;
        impl_->ring.drain([&](const Completion& c) {
          if (c.user_data == 0) {
            impl_->wake_armed = false;
            handler.wake();
            return;
          }
          ((c.user_data & kCancelBit) != 0 ? cancels : polls).push_back(c);
        });
        if ((rng() & 1U) != 0) {
          for (const auto& c : cancels) handler.completion(c);
          for (const auto& c : polls) handler.completion(c);
        } else {
          for (const auto& c : polls) handler.completion(c);
          for (const auto& c : cancels) handler.completion(c);
        }
      } else {
        impl_->ring.drain([&](const Completion& c) { handler.completion(c); });
      }
    } catch (...) {
      // A throwing posted callback must not escape the run loop: record the
      // failure, retire the waiter, and stop like the epoll transport does.
      impl_->waiter = {};
      impl_->error = std::make_error_code(std::errc::operation_canceled);
      break;
    }
    bool defer_submit = false;
    if (failpoints_enabled) defer_submit = (rng() & 7U) == 0;
    if (defer_submit) {
      impl_->trace.record(impl_->iterations, 0, kTraceDeferSubmit, 0, 0, 0);
    } else if (!impl_->ring.submit()) {
      // EAGAIN or partial submission: SQEs stay queued and are retried by the
      // next iteration's submit.
    }
    auto continuation = std::exchange(impl_->waiter, {});
    if (continuation && !resume()) break;
  }
  { std::lock_guard lock(impl_->mutex); impl_->stopped = true; }
  if (const char* path = std::getenv("WEIR_URING_TRACE_FILE"))
    impl_->trace.dump(path);
#else
  (void)task;
#endif
}
}  // namespace weir::transport
