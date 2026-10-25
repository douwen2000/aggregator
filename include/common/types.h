#pragma once

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace aggregator {

// A price level in an order book.
struct PriceLevel {
    double price = 0.0;
    double size  = 0.0;
};

// A book side: bids are stored descending by price, asks ascending.
using Book = std::vector<PriceLevel>;

// A full snapshot from one exchange venue.
struct VenueSnapshot {
    std::string venue;
    std::string symbol;
    uint64_t    sequence     = 0;
    int64_t     timestamp_us = 0;
    Book        bids;   // top-N, descending price
    Book        asks;   // top-N, ascending price
};

// Maximum levels carried by a VenueEvent.
constexpr size_t MAX_DEPTH = 64;

// Fixed-capacity, allocation-free market-data event. Producers fill this *in
// place* inside a pre-allocated ring slot (Disruptor pattern), so the hot path
// performs no heap allocation and no string/vector construction.
struct VenueEvent {
    char     venue[16]       = {};
    char     symbol[16]      = {};
    uint64_t sequence        = 0;
    int64_t  timestamp_us    = 0;
    uint32_t bid_count       = 0;
    uint32_t ask_count       = 0;
    PriceLevel bids[MAX_DEPTH];   // descending price
    PriceLevel asks[MAX_DEPTH];   // ascending price

    void setVenue(const char* s) { std::snprintf(venue, sizeof venue, "%s", s); }
    void setSymbol(const char* s) { std::snprintf(symbol, sizeof symbol, "%s", s); }
};

}  // namespace aggregator
