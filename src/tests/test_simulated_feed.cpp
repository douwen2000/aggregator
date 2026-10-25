// Integration tests for the simulated feed.
//
// Unlike the other suites these run the real thing: a feed thread writing real
// VenueEvents into a real ring, drained by a consumer here. They cover the
// producer half of the Disruptor end to end, plus the invariant that keeps the
// consolidated book uncrossed.
#include "test/test_harness.h"
#include "common/event_publisher.h"
#include "feed/simulated_exchange.h"

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <memory>
#include <thread>

using namespace aggregator;

namespace {

// Drain events from 'ring' until 'want' have been seen, invoking 'on_event'
// for each. Runs on the calling thread; the feed keeps producing meanwhile.
// Returns the number of events actually seen, so a stalled feed fails the test
// instead of hanging it (a thread test that hangs is worse than one that fails).
template <typename Fn>
int drain(VenueRing& ring, std::atomic<bool>& running, Fn&& on_event, int want) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    int64_t    next     = 0;
    int        seen     = 0;
    while (running && seen < want) {
        if (std::chrono::steady_clock::now() > deadline) break;
        const int64_t hi = ring.waitFor(next, &running);
        if (hi < 0) break;
        for (int64_t s = next; s <= hi; ++s) {
            on_event(ring.slot(s));
            ++seen;
        }
        ring.publishConsumed(hi);
        next = hi + 1;
    }
    return seen;
}

}  // namespace

TEST(simulated_feed_emits_well_formed_books) {
    auto market = std::make_shared<MarketState>(60000.0);
    auto ring   = std::make_unique<VenueRing>();
    VenueEventPublisher pub(*ring);

    SimulatedExchange feed("binance", "BTCUSDT", market, 60000.0, /*seed=*/1,
                           /*depth=*/20, /*interval_ms=*/5);
    test::check(feed.start(pub), "the feed starts");

    std::atomic<bool> running{true};
    bool     venue_ok = true, symbol_ok = true, sorted = true;
    bool     uncrossed = true, seq_ok = true;
    uint64_t last_seq  = 0;
    bool     have_seq  = false;

    const int seen = drain(*ring, running, [&](const VenueEvent& e) {
        if (std::strcmp(e.venue, "binance") != 0) venue_ok = false;
        if (std::strcmp(e.symbol, "BTCUSDT") != 0) symbol_ok = false;
        // start() seeds the ring with a sequence-0 book *before* the producer
        // thread runs, so 0 is a legitimate first sequence: only later
        // comparisons are meaningful.
        if (have_seq && e.sequence <= last_seq) seq_ok = false;
        last_seq = e.sequence;
        have_seq = true;
        if (e.bid_count > 0 && e.ask_count > 0) {
            if (!(e.bids[0].price < e.asks[0].price)) uncrossed = false;
            for (uint32_t i = 1; i < e.bid_count; ++i)
                if (e.bids[i].price > e.bids[i - 1].price) sorted = false;
            for (uint32_t i = 1; i < e.ask_count; ++i)
                if (e.asks[i].price < e.asks[i - 1].price) sorted = false;
        }
    }, 40);

    feed.stop();
    running = false;

    test::check(seen == 40, "the feed produced events into the ring");
    test::check(venue_ok && symbol_ok, "events carry the venue and symbol");
    test::check(seq_ok, "the feed's sequence increases monotonically");
    test::check(sorted, "bids descend and asks ascend within each event");
    test::check(uncrossed, "each venue's own book is uncrossed (best bid < best ask)");
}

TEST(simulated_feed_venues_track_one_mid) {
    // Regression guard for the crossed-book bug: the three venues used to walk
    // independently, so their mids drifted apart until the consolidated best bid
    // (the MAX across venues) rose above the best ask (the MIN).
    //
    // Zero volatility pins the shared mid, so each venue must quote at exactly
    // (shared mid + its own offset) on EVERY event and the gap between two
    // venues must stay exactly at their offset spread. Under the old
    // independent-walk code that gap drifted without bound.
    constexpr double kBase = 60000.0;
    constexpr double kMidA = 60002.0;
    constexpr double kMidB = 59998.0;

    auto market = std::make_shared<MarketState>(kBase, /*step_vol_frac=*/0.0);
    auto ring   = std::make_unique<VenueRing>();
    VenueEventPublisher pub(*ring);

    SimulatedExchange a("venueA", "BTCUSDT", market, kMidA, 1, 10, 5);
    SimulatedExchange b("venueB", "BTCUSDT", market, kMidB, 2, 10, 5);
    test::check(a.start(pub) && b.start(pub), "both feeds start");

    std::atomic<bool> running{true};
    double mid_a = 0.0, mid_b = 0.0;
    bool   at_offset = true, gap_ok = true, spread_ok = true;
    int    seen_a = 0, seen_b = 0;

    drain(*ring, running, [&](const VenueEvent& e) {
        if (e.bid_count == 0 || e.ask_count == 0) return;
        if (e.asks[0].price <= e.bids[0].price) spread_ok = false;
        const double mid = (e.bids[0].price + e.asks[0].price) / 2.0;
        if (std::strcmp(e.venue, "venueA") == 0) {
            mid_a = mid;
            ++seen_a;
            if (std::abs(mid - kMidA) > 1e-6) at_offset = false;
        } else if (std::strcmp(e.venue, "venueB") == 0) {
            mid_b = mid;
            ++seen_b;
            if (std::abs(mid - kMidB) > 1e-6) at_offset = false;
        }
        if (seen_a > 0 && seen_b > 0 && std::abs((mid_a - mid_b) - 4.0) > 1e-6)
            gap_ok = false;
    }, 60);

    a.stop();
    b.stop();
    running = false;

    test::check(seen_a > 0 && seen_b > 0, "both venues produced events");
    test::check(at_offset, "each venue quotes at the shared mid plus its own offset");
    test::check(gap_ok, "the two venues stay exactly 4 USD apart (one shared walk)");
    test::check(spread_ok, "both venues keep a positive spread");
}
