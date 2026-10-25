// Tests for the simulated market's shared mid (MarketState) and the invariant
// it exists to protect.
//
// The three simulated venues used to random-walk independently, so their mids
// diverged without bound: after ~30s the consolidated book went crossed (best
// bid above best ask), because the consolidated best bid is the max across
// venues and the best ask is the min. They now share one walk and differ only
// by a small fixed offset. These tests lock that.
#include "test/test_harness.h"
#include "aggregator/consolidated_book.h"
#include "feed/simulated_exchange.h"

#include <algorithm>
#include <cmath>
#include <random>
#include <string>

using namespace aggregator;

TEST(market_state_is_single_shared_walk) {
    // Every venue advances the SAME walk, so two venues sampling it back to
    // back stay one step apart forever. Independent walks would drift apart
    // without bound (their gap grows like sqrt(2N) * sigma).
    constexpr double kStart = 60000.0;
    constexpr double kVol   = 0.000015;  // the server default
    MarketState market(kStart, kVol);
    std::mt19937 rng_a(1), rng_b(2);

    double max_gap = 0.0;
    for (int i = 0; i < 4000; ++i) {
        const double a = market.advance(rng_a);
        const double b = market.advance(rng_b);
        max_gap = std::max(max_gap, std::abs(a - b));
    }

    // |a - b| is one N(0, sigma) draw. Over 4000 samples the max is ~3.5 sigma,
    // so 10 sigma is a comfortable bound that independent walks would blow past.
    const double sigma = kStart * kVol;
    test::check(max_gap < 10.0 * sigma, "venues ride one walk and never drift apart");
    test::check(std::abs(market.advance(rng_a) - kStart) > 0.0,
                "advance() actually moves the shared mid");
}

TEST(market_state_stays_clamped) {
    // A runaway walk must be pinned to [0.5x, 1.5x] of the start, otherwise a
    // long-running process could quote absurd prices.
    constexpr double kStart = 100.0;
    MarketState market(kStart, 0.05);  // deliberately huge volatility
    std::mt19937 rng(3);

    bool in_range = true;
    for (int i = 0; i < 20000; ++i) {
        const double mid = market.advance(rng);
        if (mid < kStart * 0.5 - 1e-9 || mid > kStart * 1.5 + 1e-9) {
            in_range = false;
            break;
        }
    }
    test::check(in_range, "mid stays clamped to [0.5x, 1.5x] of the start");
}

TEST(market_state_keeps_book_uncrossed) {
    // The regression itself. With one shared mid the consolidated spread is
    // constant: 2*half_spread - (max offset - min offset), independent of where
    // the walk happens to be. With independent walks it shrinks over time and
    // eventually goes negative -- that was the crossed-book bug.
    constexpr double kStart      = 60000.0;
    constexpr double kHalfSpread = kStart * 0.0001;   // same as the simulator
    const double     offsets[3]  = {0.0, 2.0, -2.0};  // same as server_main

    MarketState market(kStart);  // default volatility
    std::mt19937 rng(7);
    ConsolidatedBook book;
    book.setSymbol("BTCUSDT");

    bool crossed = false;
    double min_spread = 1e18;
    for (int step = 0; step < 1000; ++step) {
        const double shared = market.advance(rng);
        for (int v = 0; v < 3; ++v) {
            const double mid = shared + offsets[v];
            book.applyVenue("v" + std::to_string(v), {{mid - kHalfSpread, 40.0}},
                            {{mid + kHalfSpread, 40.0}}, 1, 0);
        }
        const double bb = book.bestBid().price;
        const double ba = book.bestAsk().price;
        if (!(bb < ba)) {
            crossed = true;
            break;
        }
        min_spread = std::min(min_spread, ba - bb);
    }

    const double kExpected = 2.0 * kHalfSpread - 4.0;  // 2*6 - (offset range 4)
    test::check(!crossed, "consolidated book never goes crossed over 1000 steps");
    test::check(std::abs(min_spread - kExpected) < 1e-6,
                "consolidated spread stays constant at 2*half-spread - offset range");
}
