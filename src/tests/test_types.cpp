// Tests for the wire-level event type (VenueEvent).
//
// Producers fill this IN PLACE inside a pre-allocated ring slot, so its layout
// is load-bearing: it has fixed-size char buffers for the venue/symbol names,
// and a name that overflowed its buffer would corrupt whatever follows it.
#include "test/test_harness.h"
#include "common/types.h"

#include <cstring>

using namespace aggregator;

TEST(venue_event_defaults_are_zeroed) {
    VenueEvent e;
    test::check(e.sequence == 0 && e.timestamp_us == 0,
                "a fresh event starts at sequence 0 / timestamp 0");
    test::check(e.bid_count == 0 && e.ask_count == 0,
                "a fresh event reports no levels");
    test::check(e.venue[0] == '\0' && e.symbol[0] == '\0',
                "the name buffers start empty");
    test::check(MAX_DEPTH >= 50,
                "MAX_DEPTH covers the feeds' 50-level snapshots");
    test::check(sizeof(e.bids) / sizeof(e.bids[0]) == MAX_DEPTH &&
                    sizeof(e.asks) / sizeof(e.asks[0]) == MAX_DEPTH,
                "each side holds exactly MAX_DEPTH levels");
}

TEST(venue_event_name_truncation) {
    // venue/symbol are fixed char[16]. An over-long name must be truncated and
    // stay NUL-terminated, never run into the following field.
    VenueEvent e;

    e.setVenue("binance");
    test::check(std::strcmp(e.venue, "binance") == 0, "a short venue name round-trips");
    e.setSymbol("BTCUSDT");
    test::check(std::strcmp(e.symbol, "BTCUSDT") == 0, "a short symbol round-trips");

    const char* kLong = "an-extremely-long-venue-name-that-does-not-fit";
    e.setVenue(kLong);
    test::check(std::strlen(e.venue) == sizeof(e.venue) - 1,
                "an over-long venue name is truncated to the buffer size");
    test::check(e.venue[sizeof(e.venue) - 1] == '\0',
                "the truncated venue name stays NUL-terminated");
    test::check(std::strncmp(e.venue, kLong, sizeof(e.venue) - 1) == 0,
                "truncation keeps the leading characters");

    // The symbol sits right after the venue, so a truncation bug shows up as a
    // mangled symbol.
    test::check(std::strcmp(e.symbol, "BTCUSDT") == 0,
                "the neighbouring symbol buffer is untouched");
}
