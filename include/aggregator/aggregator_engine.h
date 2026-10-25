#pragma once

#include "aggregator/consolidated_book.h"
#include "common/event_publisher.h"
#include "common/update_ptr.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <thread>

namespace aggregator {

// The single consumer of the venue ring — the Disruptor's event handler.
//
// It exclusively owns the ConsolidatedBook, which is why the book needs no
// locking at all (single-writer principle). Each wake-up drains *every*
// currently available event as one batch before publishing, so bursty inbound
// traffic does not turn into bursty outbound traffic.
class AggregatorEngine {
public:
    using UpdateSink = std::function<void(BookUpdatePtr)>;

    AggregatorEngine(VenueRing& ring, ConsolidatedBook& book, UpdateSink sink,
                     uint64_t min_publish_us     = 10'000,  // <=100 Hz out
                     size_t   consolidated_depth = 50,      // enough for bands
                     size_t   venue_depth        = 10);     // per-venue trim

    void start();
    void stop();

    // Diagnostics (relaxed loads; informational only).
    uint64_t eventsApplied() const { return events_.load(std::memory_order_relaxed); }
    uint64_t updatesPublished() const { return published_.load(std::memory_order_relaxed); }

private:
    void run();
    void publishOnce();

    VenueRing&        ring_;
    ConsolidatedBook& book_;
    UpdateSink        sink_;

    uint64_t min_publish_us_;
    size_t   consolidated_depth_;
    size_t   venue_depth_;

    // Reused across publishes => no allocation in the steady state.
    ConsolidatedBook::Snapshot snap_;
    int64_t last_publish_us_ = 0;

    std::atomic<bool>    running_{false};
    std::atomic<uint64_t> events_{0};
    std::atomic<uint64_t> published_{0};
    std::thread           th_;
};

}  // namespace aggregator
