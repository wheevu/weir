#include "uring_ring.hpp"

#include <cerrno>
#include <cstring>
#include <memory>

#ifdef __linux__
#include <algorithm>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace weir::transport {

struct UringRing::Impl {
  std::error_code error;
#ifdef __linux__
  int fd = -1;
  io_uring_params params{};
  void* ring = MAP_FAILED;
  void* cq_ring = MAP_FAILED;
  io_uring_sqe* sqes = nullptr;
  std::size_t ring_size = 0;
  std::size_t cq_ring_size = 0;
  unsigned* sq_head = nullptr;
  unsigned* sq_tail = nullptr;
  unsigned* sq_mask = nullptr;
  unsigned* sq_array = nullptr;
  unsigned reserved_tail = 0;
  unsigned* cq_head = nullptr;
  unsigned* cq_tail = nullptr;
  unsigned* cq_mask = nullptr;
  io_uring_cqe* cqes = nullptr;

  explicit Impl(unsigned entries) {
    do {
      fd = static_cast<int>(syscall(__NR_io_uring_setup, entries, &params));
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) {
      error = std::error_code(errno, std::generic_category());
      return;
    }
    ring_size = params.sq_off.array + params.sq_entries * sizeof(unsigned);
    cq_ring_size = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);
    if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0) {
      ring_size = std::max(ring_size, cq_ring_size);
      cq_ring_size = ring_size;
    }
    ring = mmap(nullptr, ring_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                fd, IORING_OFF_SQ_RING);
    if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0) {
      cq_ring = ring;
    } else {
      cq_ring = mmap(nullptr, cq_ring_size, PROT_READ | PROT_WRITE, MAP_SHARED,
                     fd, IORING_OFF_CQ_RING);
    }
    sqes = static_cast<io_uring_sqe*>(mmap(
        nullptr, params.sq_entries * sizeof(io_uring_sqe), PROT_READ | PROT_WRITE,
        MAP_SHARED, fd, IORING_OFF_SQES));
    if (ring == MAP_FAILED || cq_ring == MAP_FAILED || sqes == MAP_FAILED) {
      error = std::error_code(errno, std::generic_category());
      return;
    }
    auto* sq = static_cast<char*>(ring);
    sq_head = reinterpret_cast<unsigned*>(sq + params.sq_off.head);
    sq_tail = reinterpret_cast<unsigned*>(sq + params.sq_off.tail);
    sq_mask = reinterpret_cast<unsigned*>(sq + params.sq_off.ring_mask);
    sq_array = reinterpret_cast<unsigned*>(sq + params.sq_off.array);
    reserved_tail = __atomic_load_n(sq_tail, __ATOMIC_RELAXED);
    auto* cq = static_cast<char*>(cq_ring);
    cq_head = reinterpret_cast<unsigned*>(cq + params.cq_off.head);
    cq_tail = reinterpret_cast<unsigned*>(cq + params.cq_off.tail);
    cq_mask = reinterpret_cast<unsigned*>(cq + params.cq_off.ring_mask);
    cqes = reinterpret_cast<io_uring_cqe*>(cq + params.cq_off.cqes);
  }

  ~Impl() {
    if (sqes != nullptr && sqes != MAP_FAILED)
      munmap(sqes, params.sq_entries * sizeof(io_uring_sqe));
    if (ring != MAP_FAILED) munmap(ring, ring_size);
    if ((params.features & IORING_FEAT_SINGLE_MMAP) == 0 && cq_ring != MAP_FAILED)
      munmap(cq_ring, cq_ring_size);
    if (fd >= 0) close(fd);
  }
#else
  explicit Impl(unsigned) : error(std::make_error_code(std::errc::operation_not_supported)) {}
  ~Impl() = default;
#endif
};

UringRing::UringRing(unsigned entries) : impl_(new Impl(entries)) {}
UringRing::~UringRing() { delete impl_; }
bool UringRing::valid() const noexcept {
#ifdef __linux__
  return impl_ != nullptr && impl_->fd >= 0 && !impl_->error;
#else
  return false;
#endif
}
std::error_code UringRing::error() const noexcept { return impl_->error; }

#ifdef __linux__
io_uring_sqe* UringRing::acquire(std::uint64_t user_data) {
  if (!valid()) return nullptr;
  const unsigned tail = impl_->reserved_tail;
  const unsigned head = __atomic_load_n(impl_->sq_head, __ATOMIC_ACQUIRE);
  if (tail - head >= *impl_->sq_mask + 1U) return nullptr;
  const unsigned index = tail & *impl_->sq_mask;
  auto* sqe = &impl_->sqes[index];
  std::memset(sqe, 0, sizeof(*sqe));
  sqe->user_data = user_data;
  impl_->sq_array[index] = index;
  impl_->reserved_tail = tail + 1U;
  return sqe;
}
#endif

bool UringRing::submit(unsigned minimum_completions) {
#ifdef __linux__
  if (!valid()) return false;
  const unsigned head = __atomic_load_n(impl_->sq_head, __ATOMIC_ACQUIRE);
  const unsigned tail = impl_->reserved_tail;
  const unsigned count = tail - head;
  __atomic_store_n(impl_->sq_tail, tail, __ATOMIC_RELEASE);
  const unsigned flags = minimum_completions == 0 ? 0U : IORING_ENTER_GETEVENTS;
  long result = 0;
  do {
    result = syscall(__NR_io_uring_enter, impl_->fd, count,
                     minimum_completions, flags, nullptr, 0);
  } while (result < 0 && errno == EINTR);
  if (result < 0 && errno != EAGAIN) {
    impl_->error = std::error_code(errno, std::generic_category());
    return false;
  }
  if (count != 0 && result >= 0 && static_cast<unsigned>(result) < count) return false;
  return result >= 0;
#else
  (void)minimum_completions;
  return false;
#endif
}

std::size_t UringRing::drain(
    const std::function<void(const Completion&)>& callback,
    std::size_t limit) {
#ifdef __linux__
  if (!valid()) return 0;
  unsigned head = __atomic_load_n(impl_->cq_head, __ATOMIC_RELAXED);
  const unsigned tail = __atomic_load_n(impl_->cq_tail, __ATOMIC_ACQUIRE);
  std::size_t count = 0;
  while (head != tail && (limit == 0 || count < limit)) {
    const auto& cqe = impl_->cqes[head & *impl_->cq_mask];
    callback(Completion{cqe.user_data, cqe.res, cqe.flags});
    ++head;
    ++count;
  }
  __atomic_store_n(impl_->cq_head, head, __ATOMIC_RELEASE);
  return count;
#else
  (void)callback;
  (void)limit;
  return 0;
#endif
}

}  // namespace weir::transport
