#pragma once

#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>

namespace aggregator {

// Process-wide log line prefix: [HH:MM:SS] with a shared lock so concurrent
// threads don't interleave partial lines.
inline std::ostream& log_prefix(std::ostream& os, const char* tag) {
    static std::mutex m;
    std::lock_guard<std::mutex> lk(m);
    auto now = std::chrono::system_clock::now();
    auto t   = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    // std::cout is block-buffered when redirected to a file, which would lose
    // every log line if the process is killed before a clean exit. Flush each
    // line instead (log volume here is only ~10 lines/s).
    os << std::unitbuf;
    os << "[" << std::put_time(&tm, "%H:%M:%S") << "] " << tag << " ";
    return os;
}

#define LOG_INFO() (aggregator::log_prefix(std::cout, "INFO"))
#define LOG_WARN() (aggregator::log_prefix(std::cout, "WARN"))
#define LOG_ERR()  (aggregator::log_prefix(std::cerr, "ERR "))

}  // namespace aggregator
