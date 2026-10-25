#pragma once

#include "common/disruptor.h"
#include "common/types.h"
#include <cstddef>
#include <utility>

namespace aggregator {

// Ring capacity. At ~150 inbound events/s, 1024 slots is ample headroom; the
// only cost is ~2 MB of pre-allocated memory.
constexpr size_t kRingCapacity = 1024;

using VenueRing = RingBuffer<VenueEvent, kRingCapacity>;

// Producer-side handle. The producer fills the slot in place via a callback, so
// no intermediate snapshot object is ever built and nothing is allocated.
class VenueEventPublisher {
public:
    explicit VenueEventPublisher(VenueRing& ring) : ring_(ring) {}

    // 'fill' receives a writable, zero-initialised-count slot to populate.
    template <typename FillFn>
    void publish(FillFn&& fill) {
        ring_.produce([&](VenueEvent& e) {
            e.bid_count = 0;
            e.ask_count = 0;
            fill(e);
        });
    }

private:
    VenueRing& ring_;
};

}  // namespace aggregator
