#pragma once

#include "common/types.h"
#include <map>

namespace aggregator {

// A single venue's order book. Maintains bids (descending) and asks (ascending)
// keyed by price. We deliberately avoid std::map ordering tricks: bids are kept
// in a map keyed ascending and iterated in reverse when needed.
class OrderBook {
public:
    // Replace the book sides wholesale from a snapshot (levels already sorted by
    // the producer). Using full snapshots keeps reconciliation trivially correct
    // and is what most exchange "snapshot" streams provide.
    void applySnapshot(const Book& bids, const Book& asks);

    // Top 'k' levels: bids descending, asks ascending.
    Book topBids(size_t k) const;
    Book topAsks(size_t k) const;

    PriceLevel bestBid() const;
    PriceLevel bestAsk() const;

    bool empty() const { return bids_.empty() && asks_.empty(); }

    const std::map<double, double>& bids() const { return bids_; }
    const std::map<double, double>& asks() const { return asks_; }

private:
    // price -> size, ascending key order
    std::map<double, double> bids_;
    std::map<double, double> asks_;
};

}  // namespace aggregator
