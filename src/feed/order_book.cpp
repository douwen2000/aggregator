#include "feed/order_book.h"

namespace aggregator {

void OrderBook::applySnapshot(const Book& bids, const Book& asks) {
    bids_.clear();
    asks_.clear();
    for (const auto& l : bids) bids_[l.price] = l.size;
    for (const auto& l : asks) asks_[l.price] = l.size;
}

Book OrderBook::topBids(size_t k) const {
    Book out;
    out.reserve(std::min(k, bids_.size()));
    size_t n = 0;
    // bids_ is ascending by price; best bid is the largest price -> reverse.
    for (auto it = bids_.rbegin(); it != bids_.rend() && n < k; ++it, ++n)
        out.push_back({it->first, it->second});
    return out;
}

Book OrderBook::topAsks(size_t k) const {
    Book out;
    out.reserve(std::min(k, asks_.size()));
    size_t n = 0;
    for (auto it = asks_.begin(); it != asks_.end() && n < k; ++it, ++n)
        out.push_back({it->first, it->second});
    return out;
}

PriceLevel OrderBook::bestBid() const {
    if (bids_.empty()) return {};
    auto it = bids_.rbegin();
    return {it->first, it->second};
}

PriceLevel OrderBook::bestAsk() const {
    if (asks_.empty()) return {};
    auto it = asks_.begin();
    return {it->first, it->second};
}

}  // namespace aggregator
