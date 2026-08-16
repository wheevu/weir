#pragma once
#include <atomic>
#include <array>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace weir {
struct Event { std::uint64_t id{}; std::string payload; std::shared_ptr<std::promise<bool>> durable_ack; std::function<void(bool)> durable_completion; };
std::uint32_t checksum(std::string_view s);
std::vector<std::uint8_t> encode(const Event& e);
class Parser { std::vector<std::uint8_t> b_; public: std::vector<Event> feed(const std::uint8_t*, std::size_t); };

template<class T> class BoundedQueue {
 std::mutex m_; std::condition_variable cv_, space_; std::deque<T> q_; std::size_t cap_; bool closed_=false;
public: explicit BoundedQueue(std::size_t c):cap_(c) {}
 bool push(T v) { std::unique_lock l(m_); space_.wait(l,[&]{return q_.size()<cap_||closed_;}); if(closed_) return false; q_.push_back(std::move(v)); cv_.notify_one(); return true; }
 std::optional<T> pop() { std::unique_lock l(m_); cv_.wait(l,[&]{return !q_.empty()||closed_;}); if(q_.empty()) return {}; T v=std::move(q_.front()); q_.pop_front(); space_.notify_one(); return v; }
 void close(){std::lock_guard l(m_);closed_=true;cv_.notify_all();space_.notify_all();}
};

class Log { std::filesystem::path path_; mutable std::mutex m_; std::ofstream out_; public: explicit Log(std::filesystem::path); bool append(const Event&); std::uint64_t recover(); std::vector<Event> replay() const; };
class Metrics { mutable std::mutex m_; std::map<std::string,std::uint64_t> v_; public: void inc(std::string); std::string prometheus() const; };
void log(std::string_view level,std::string_view message);
class Pipeline { Log& log_; Metrics& metrics_; BoundedQueue<Event> durable_{256}, process_{256}; std::vector<std::thread> workers_; std::thread persister_; public: Pipeline(Log&,Metrics&,unsigned workers=2); ~Pipeline(); bool submit(Event); };
 int run_server(unsigned port, Log&, Metrics&, std::atomic<bool>& stop, unsigned workers=2);
int run_metrics_http(unsigned port, Metrics&, std::atomic<bool>& stop);
}
