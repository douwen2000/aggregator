#pragma once

#include "common/types.h"
#include <cstddef>
#include <limits>
#include <map>
#include <string>
#include <vector>

namespace aggregator {

// Merges the order books of multiple venues into a single consolidated book.
// Sizes at identical prices across venues are summed.
//
// NOT thread-safe by design: under the Disruptor architecture this book is
// owned exclusively by the aggregator engine thread (single-writer principle),
// so no locking is required anywhere in the hot path.
//
// All merge work writes into caller-supplied or internally reused buffers, so
// the steady state performs zero heap allocation.
class ConsolidatedBook {
public:
    // Apply a new snapshot from one venue.
    void applyVenue(const std::string& venue, const PriceLevel* bids, size_t nb,
                    const PriceLevel* asks, size_t na, uint64_t venue_seq,
                    int64_t ts_us);

    // Convenience overload (used by tests and the real-feed path).
    void applyVenue(const std::string& venue, const Book& bids, const Book& asks,
                    uint64_t venue_seq, int64_t ts_us) {
        applyVenue(venue, bids.data(), bids.size(), asks.data(), asks.size(),
                   venue_seq, ts_us);
    }

    void setSymbol(std::string s) { symbol_cache_ = std::move(s); }

    // Consolidated top-K levels (bids descending, asks ascending).
    Book topBids(size_t k) const;
    Book topAsks(size_t k) const;
    PriceLevel bestBid() const;
    PriceLevel bestAsk() const;

    uint64_t sequence() const { return seq_; }
    int64_t timestamp() const { return ts_us_; }
    bool empty() const { return venues_.empty(); }

    // Volume-band price: walking from the top of book, the price at which
    // cumulative notional (sum of size*price) first reaches 'target_notional'.
    // If depth is insufficient, returns the deepest available price.
    double volumeBandPrice(bool is_bid, double target_notional) const;

    // Price bands around the BBO in basis points (1bp = 1e-4).
    void priceBands(const std::vector<double>& bps, std::vector<double>& bid_bands,
                    std::vector<double>& ask_bands) const;

    // Zero-copy view of a venue's book; valid until the next mutation.
    struct VenueView {
        const std::string* name;
        const Book*        bids;
        const Book*        asks;
        size_t             depth;  // levels the consumer should take
    };

    struct Snapshot {
        std::string              symbol;
        uint64_t                 sequence     = 0;
        int64_t                  timestamp_us = 0;
        Book                     bids;    // consolidated top-K
        Book                     asks;    // consolidated top-K
        std::vector<VenueView>   venues;  // per-venue views (no copies)
    };

    // Fills 's' in place. Its vectors are reused across calls, so after warm-up
    // this performs no allocation and no copying of per-venue books.
    void fillSnapshot(Snapshot& s, size_t top_k, size_t venue_depth) const;

private:
    struct VenueState {
        Book bids;   // bids descending, asks ascending
        Book asks;
    };
    struct Cursor {
        const Book* b;
        size_t      i;
    };

    // k-way merge of every venue into 'out' (bids descending / asks ascending),
    // summing sizes at equal prices. Stops after 'max_levels' (or when the
    // books are exhausted if max_levels == kUnbounded). Uses a reused cursor
    // buffer, so no allocation and no tree building.
    static constexpr size_t kUnbounded = std::numeric_limits<size_t>::max();
    void mergeSide(bool is_bid, Book& out, size_t max_levels) const;

    std::map<std::string, VenueState> venues_;
    uint64_t    seq_   = 0;
    int64_t     ts_us_ = 0;
    std::string symbol_cache_;

    mutable Book              scratch_bids_;
    mutable Book              scratch_asks_;
    mutable std::vector<Cursor> cursors_;
};

}  // namespace aggregator
