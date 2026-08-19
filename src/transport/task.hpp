#pragma once

#include <coroutine>
#include <exception>
#include <utility>

namespace weir::transport {

class RootTask {
 public:
  struct promise_type;
  using Handle = std::coroutine_handle<promise_type>;

  RootTask() = default;
  explicit RootTask(Handle handle) : handle_(handle) {}
  RootTask(const RootTask&) = delete;
  RootTask& operator=(const RootTask&) = delete;
  RootTask(RootTask&& other) noexcept : handle_(std::exchange(other.handle_, {})) {}
  RootTask& operator=(RootTask&& other) noexcept {
    if (this != &other) {
      destroy();
      handle_ = std::exchange(other.handle_, {});
    }
    return *this;
  }
  ~RootTask() { destroy(); }

  bool valid() const noexcept { return static_cast<bool>(handle_); }
  bool done() const noexcept { return !handle_ || handle_.done(); }
  void resume() {
    if (handle_ && !handle_.done()) handle_.resume();
    if (handle_ && handle_.promise().error) std::rethrow_exception(handle_.promise().error);
  }
  void destroy() noexcept {
    if (handle_) handle_.destroy();
    handle_ = {};
  }

  struct promise_type {
    std::exception_ptr error;
    RootTask get_return_object() noexcept {
      return RootTask{Handle::from_promise(*this)};
    }
    std::suspend_always initial_suspend() const noexcept { return {}; }
    std::suspend_always final_suspend() const noexcept { return {}; }
    void return_void() const noexcept {}
    void unhandled_exception() noexcept { error = std::current_exception(); }
  };

 private:
  Handle handle_{};
};

}  // namespace weir::transport
