#pragma once

#include "common/types.h"
#include <aggregator.pb.h>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>

namespace aggregator {

// Convert a repeated Level field into a local Book.
inline Book toBook(const google::protobuf::RepeatedPtrField<Level>& f) {
    Book b;
    b.reserve(f.size());
    for (const auto& l : f) b.push_back({l.price(), l.size()});
    return b;
}

// Render a producer timestamp (us since epoch) as "YYYY-MM-DD HH:MM:SS.mmm".
inline std::string formatTimestamp(int64_t us) {
    const auto secs = static_cast<std::time_t>(us / 1000000);
    const auto millis = static_cast<int>((us % 1000000) / 1000);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &secs);
#else
    localtime_r(&secs, &tm);
#endif
    std::ostringstream os;
    os << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setfill('0')
       << std::setw(3) << millis;
    return os.str();
}

// Simple display throttle so stdout isn't flooded at 100Hz.
struct DisplayThrottle {
    explicit DisplayThrottle(int64_t min_ms = 250) : min_ms_(min_ms) {}
    bool ready() {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                       .count();
        if (now - last_ >= min_ms_) {
            last_ = now;
            return true;
        }
        return false;
    }
    int64_t min_ms_;
    int64_t last_ = 0;
};

}  // namespace aggregator
