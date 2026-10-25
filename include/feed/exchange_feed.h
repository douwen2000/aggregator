#pragma once

#include "common/types.h"
#include <memory>
#include <string>

namespace aggregator {

class VenueEventPublisher;  // defined in common/event_publisher.h

// Abstraction over a market-data source for a single symbol on a single venue.
// Implementations may be simulated (offline, deterministic) or real
// (WebSocket to a live CEX). The aggregator only depends on this interface.
//
// Producers write directly into a claimed ring-buffer slot (Disruptor):
//     VenueEvent& e = pub.claim();
//     ... fill e ...
//     pub.commit();
// This avoids building an intermediate snapshot and any per-event allocation.
class ExchangeFeed {
public:
    virtual ~ExchangeFeed() = default;

    // Human-readable venue name, e.g. "binance".
    virtual const std::string& name() const = 0;

    // Begin producing events into 'pub' from the feed's own thread.
    // Returns false on init failure.
    virtual bool start(VenueEventPublisher& pub) = 0;

    // Stop producing and join internal threads.
    virtual void stop() = 0;
};

using ExchangeFeedPtr = std::shared_ptr<ExchangeFeed>;

}  // namespace aggregator
