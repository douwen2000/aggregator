#include "aggregator/consolidated_book.h"

#include <algorithm>

namespace aggregator {

namespace {
constexpr size_t kNoCursor = static_cast<size_t>(-1);
}  // namespace

void ConsolidatedBook::applyVenue(const std::string& venue, const PriceLevel* bids,
                                  size_t nb, const PriceLevel* asks, size_t na,
                                  uint64_t /*venue_seq*/, int64_t ts_us) {
    auto& st = venues_[venue];
    st.bids.assign(bids, bids + nb);
    st.asks.assign(asks, asks + na);
    ++seq_;
    ts_us_ = ts_us;
}

// k-way merge. Each venue's book is already sorted, so this is a merge of K
// sorted runs rather than a sort: O(levels * K) with K small (number of
// venues), no allocation, and perfectly sequential memory access.
// (A priority queue would only pay off once K grows past ~8-16.)
void ConsolidatedBook::mergeSide(bool is_bid, Book& out, size_t max_levels) const {
    out.clear();
    if (max_levels == 0 || venues_.empty()) return;

    cursors_.clear();
    for (const auto& kv : venues_) {
        const Book& b = is_bid ? kv.second.bids : kv.second.asks;
        if (!b.empty()) cursors_.push_back(Cursor{&b, 0});
    }
    if (cursors_.empty()) return;

    if (max_levels != kUnbounded) out.reserve(max_levels);

    while (out.size() < max_levels) {
        // Pick the best head: highest price for bids, lowest for asks.
        size_t best = kNoCursor;
        for (size_t i = 0; i < cursors_.size(); ++i) {
            const Cursor& c = cursors_[i];
            if (c.i >= c.b->size()) continue;
            if (best == kNoCursor) { best = i; continue; }
            const double p  = (*c.b)[c.i].price;
            const double bp = (*cursors_[best].b)[cursors_[best].i].price;
            if (is_bid ? (p > bp) : (p < bp)) best = i;
        }
        if (best == kNoCursor) break;  // all venues exhausted

        const double price = (*cursors_[best].b)[cursors_[best].i].price;

        // Sum every venue quoting this exact price.
        double size = 0.0;
        for (auto& c : cursors_) {
            while (c.i < c.b->size() && (*c.b)[c.i].price == price) {
                size += (*c.b)[c.i].size;
                ++c.i;
            }
        }
        out.push_back(PriceLevel{price, size});
    }
}

Book ConsolidatedBook::topBids(size_t k) const {
    Book out;
    mergeSide(true, out, k);
    return out;
}

Book ConsolidatedBook::topAsks(size_t k) const {
    Book out;
    mergeSide(false, out, k);
    return out;
}

PriceLevel ConsolidatedBook::bestBid() const {
    mergeSide(true, scratch_bids_, 1);
    return scratch_bids_.empty() ? PriceLevel{} : scratch_bids_.front();
}

PriceLevel ConsolidatedBook::bestAsk() const {
    mergeSide(false, scratch_asks_, 1);
    return scratch_asks_.empty() ? PriceLevel{} : scratch_asks_.front();
}

double ConsolidatedBook::volumeBandPrice(bool is_bid, double target_notional) const {
    Book& side = is_bid ? scratch_bids_ : scratch_asks_;
    // Needs the full depth, not just top-K, so no truncation here.
    mergeSide(is_bid, side, kUnbounded);
    double cum = 0.0;
    for (const auto& l : side) {
        cum += l.price * l.size;
        if (cum >= target_notional) return l.price;
    }
    // Insufficient depth -> deepest available price (the "50M+" tail).
    return side.empty() ? 0.0 : side.back().price;
}

void ConsolidatedBook::priceBands(const std::vector<double>& bps,
                                  std::vector<double>& bid_bands,
                                  std::vector<double>& ask_bands) const {
    mergeSide(true, scratch_bids_, 1);
    mergeSide(false, scratch_asks_, 1);
    const double bb = scratch_bids_.empty() ? 0.0 : scratch_bids_.front().price;
    const double ba = scratch_asks_.empty() ? 0.0 : scratch_asks_.front().price;

    bid_bands.assign(bps.size(), 0.0);
    ask_bands.assign(bps.size(), 0.0);
    for (size_t i = 0; i < bps.size(); ++i) {
        const double f = bps[i] / 10000.0;
        bid_bands[i] = bb * (1.0 - f);
        ask_bands[i] = ba * (1.0 + f);
    }
}

void ConsolidatedBook::fillSnapshot(Snapshot& s, size_t top_k, size_t venue_depth) const {
    s.symbol       = symbol_cache_;
    s.sequence     = seq_;
    s.timestamp_us = ts_us_;

    mergeSide(true, s.bids, top_k);
    mergeSide(false, s.asks, top_k);

    // Views only: the consumer reads them immediately on this same thread.
    s.venues.clear();
    for (const auto& kv : venues_) {
        s.venues.push_back(
            VenueView{&kv.first, &kv.second.bids, &kv.second.asks, venue_depth});
    }
}

}  // namespace aggregator
