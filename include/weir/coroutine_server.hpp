#pragma once

#include "weir/weir.hpp"

namespace weir {
int run_coroutine_server(unsigned port, Log&, Metrics&, std::atomic<bool>& stop,
                         unsigned workers = 2);
int run_uring_server(unsigned port, Log&, Metrics&, std::atomic<bool>& stop,
                     unsigned workers = 2);
}
