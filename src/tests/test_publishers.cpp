#include "test/test_harness.h"
#include "common/bands.h"

using namespace aggregator;

TEST(band_volume_price) {
    // Descending bids; cumulative notional from top.
    Book bids = {{100.0, 10.0}, {99.0, 10.0}, {98.0, 10.0}};
    // 10*100=1000, +10*99=990 -> 1990, +10*98=980 -> 2970
    test::check(std::abs(volumeBandPrice(bids, 1000.0) - 100.0) < 1e-9,
                "1k notional at price 100");
    test::check(std::abs(volumeBandPrice(bids, 1990.0) - 99.0) < 1e-9,
                "1990 notional at price 99");
    test::check(std::abs(volumeBandPrice(bids, 1e9) - 98.0) < 1e-9,
                "overshoot returns deepest price 98");
}

TEST(band_price_bps) {
    std::vector<double> bps = {50, 100, 200};
    std::vector<double> bid_b, ask_b;
    priceBands(100.0, 200.0, bps, bid_b, ask_b);
    // bid side: 100*(1 - bps/1e4)
    test::check(std::abs(bid_b[0] - 99.5) < 1e-9, "50bps bid = 99.5");
    test::check(std::abs(bid_b[2] - 98.0) < 1e-9, "200bps bid = 98.0");
    // ask side: 200*(1 + bps/1e4)
    test::check(std::abs(ask_b[0] - 201.0) < 1e-9, "50bps ask = 201.0");
    test::check(std::abs(ask_b[2] - 204.0) < 1e-9, "200bps ask = 204.0");
}

TEST(band_cumulative_size) {
    // Ask book, ascending (SELL = 卖盘): take everything at or below the limit.
    Book asks = {{101.0, 1.0}, {102.0, 2.0}, {103.0, 4.0}};
    test::check(std::abs(cumulativeSizeTo(asks, BandSide::Sell, 101.0) - 1.0) < 1e-9,
                "ask book to 101 collects only the top level (1)");
    test::check(std::abs(cumulativeSizeTo(asks, BandSide::Sell, 102.0) - 3.0) < 1e-9,
                "ask book to 102 collects 1+2");
    test::check(std::abs(cumulativeSizeTo(asks, BandSide::Sell, 103.0) - 7.0) < 1e-9,
                "ask book to 103 collects 1+2+4");
    test::check(std::abs(cumulativeSizeTo(asks, BandSide::Sell, 100.0) - 0.0) < 1e-9,
                "limit below the top of the ask book collects nothing");

    // Bid book, descending (BUY = 买盘): take everything at or above the limit.
    Book bids = {{99.0, 1.0}, {98.0, 2.0}, {97.0, 4.0}};
    test::check(std::abs(cumulativeSizeTo(bids, BandSide::Buy, 99.0) - 1.0) < 1e-9,
                "bid book to 99 collects only the top level (1)");
    test::check(std::abs(cumulativeSizeTo(bids, BandSide::Buy, 97.0) - 7.0) < 1e-9,
                "bid book to 97 collects 1+2+4");
}

TEST(band_side_names_the_order_book) {
    // Lock down the convention: 'buy' means 买盘 (bid book) and 'sell' means
    // 卖盘 (ask book). The two books get wildly different level sizes, so a
    // swapped mapping shows up as a wrong number rather than passing silently.
    Book bids = {{100.0, 1.0}, {99.5, 1.0}, {99.0, 1.0}};   // descending, size 1
    Book asks = {{101.0, 7.0}, {101.5, 7.0}, {102.0, 7.0}}; // ascending, size 7

    const std::vector<double> bps = {50};  // f = 0.005
    std::vector<double> buy_v, sell_v;
    priceBandVolumes(bids, asks, 100.0, 101.0, bps, buy_v, sell_v);

    // buy: bid book down to 100*(1-0.005) = 99.5 -> levels {100, 99.5} = 2.0
    test::check(std::abs(buy_v[0] - 2.0) < 1e-9, "BUY measures the bid book (size 1 each)");
    // sell: ask book up to 101*(1+0.005) = 101.505 -> levels {101, 101.5} = 14.0
    test::check(std::abs(sell_v[0] - 14.0) < 1e-9, "SELL measures the ask book (size 7 each)");
}

TEST(band_volume_monotonic_in_bps) {
    // The property the PriceBands output relies on: a wider band strictly
    // contains a narrower one, so Vol must never decrease with bps.
    Book bids = {{100.0, 5.0}, {99.9, 5.0}, {99.5, 5.0}, {99.0, 5.0}, {98.0, 5.0}};
    Book asks = {{100.1, 5.0}, {100.2, 5.0}, {100.5, 5.0}, {101.0, 5.0}, {102.0, 5.0}};

    const std::vector<double>& bps = priceBandBps();
    std::vector<double> buy_v, sell_v;
    priceBandVolumes(bids, asks, 100.0, 100.1, bps, buy_v, sell_v);

    test::check(buy_v.size() == bps.size() && sell_v.size() == bps.size(),
                "one volume per band");
    bool monotonic = true;
    for (size_t i = 1; i < bps.size(); ++i) {
        if (buy_v[i] < buy_v[i - 1] - 1e-9) monotonic = false;
        if (sell_v[i] < sell_v[i - 1] - 1e-9) monotonic = false;
    }
    test::check(monotonic, "cumulative volume never decreases as bps widens");
    test::check(buy_v[0] > 0.0 && sell_v[0] > 0.0, "narrowest band already has volume");
}

TEST(band_canonical_definitions) {
    // Clients render whatever labels the server ships, so the canonical band
    // set is part of the contract.
    const auto& n = notionalBands();
    test::check(n.size() == 5, "five notional bands");
    if (n.size() == 5) {
        test::check(n[0].first == "1M" && n[0].second == 1e6, "1M band = 1e6 USD");
        test::check(n[1].first == "5M" && n[1].second == 5e6, "5M band = 5e6 USD");
        test::check(n[3].first == "25M" && n[3].second == 25e6, "25M band = 25e6 USD");
        test::check(n[4].first == "50M+" && n[4].second == 50e6, "50M+ band = 50e6 USD");
        bool ascending = true;
        for (size_t i = 1; i < n.size(); ++i)
            if (n[i].second <= n[i - 1].second) ascending = false;
        test::check(ascending, "notional targets strictly increase");
    }
    const auto& b = priceBandBps();
    test::check(b.size() == 5, "five price bands");
    if (b.size() == 5) {
        test::check(b[0] == 50 && b[1] == 100 && b[2] == 200 && b[3] == 500 &&
                        b[4] == 1000,
                    "bands are 50/100/200/500/1000 bps");
    }
}

TEST(band_empty_book_edges) {
    const Book none;
    test::check(volumeBandPrice(none, 1e6) == 0.0,
                "volume band with no depth returns 0, not a garbage price");
    test::check(cumulativeSizeTo(none, BandSide::Buy, 100.0) == 0.0,
                "empty bid book yields 0 size");
    test::check(cumulativeSizeTo(none, BandSide::Sell, 100.0) == 0.0,
                "empty ask book yields 0 size");

    std::vector<double> buy_v, sell_v, no_bps;
    priceBandVolumes(none, none, 0.0, 0.0, no_bps, buy_v, sell_v);
    test::check(buy_v.empty() && sell_v.empty(), "no bands requested -> no volumes");
}

TEST(band_notional_boundary_exact) {
    // cum >= target must trigger exactly at the crossing level: not one level
    // early, not one late. An off-by-one here silently misprices every band.
    Book asks = {{100.0, 1.0}, {101.0, 1.0}};  // cumulative 100, then 201
    test::check(std::abs(volumeBandPrice(asks, 100.0) - 100.0) < 1e-9,
                "target equal to level 1's notional hits level 1");
    test::check(std::abs(volumeBandPrice(asks, 100.5) - 101.0) < 1e-9,
                "one unit more spills into level 2");
    test::check(std::abs(volumeBandPrice(asks, 201.0) - 101.0) < 1e-9,
                "target equal to the total hits the last level");
    test::check(std::abs(volumeBandPrice(asks, 1e9) - 101.0) < 1e-9,
                "far beyond available depth returns the deepest price");
    test::check(volumeBandPrice(asks, 0.0) == 100.0,
                "a zero target is satisfied by the very first level");
}
