#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <system_error>

#ifdef __linux__
#include <linux/io_uring.h>
#endif

namespace weir::transport {

struct Completion {
  std::uint64_t user_data{};
  int result{};
  std::uint32_t flags{};
};

class UringRing {
 public:
  explicit UringRing(unsigned entries = 256);
  ~UringRing();
  UringRing(const UringRing&) = delete;
  UringRing& operator=(const UringRing&) = delete;
  UringRing(UringRing&&) = delete;
  UringRing& operator=(UringRing&&) = delete;

  bool valid() const noexcept;
  std::error_code error() const noexcept;
#ifdef __linux__
  io_uring_sqe* acquire(std::uint64_t user_data);
#endif
  bool submit(unsigned minimum_completions = 0);
  std::size_t drain(const std::function<void(const Completion&)>& callback);

 private:
  struct Impl;
  Impl* impl_{};
};

}  // namespace weir::transport
