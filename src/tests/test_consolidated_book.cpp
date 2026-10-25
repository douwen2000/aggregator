#include "test/test_harness.h"
#include "aggregator/consolidated_book.h"

using namespace aggregator;

TEST(consolidated_merge_two_venues) {
    ConsolidatedBook book;
    book.setSymbol("BTCUSDT");

    // binance: best bid 100, okx: best bid 101 -> consolidated best bid = 101
    book.applyVenue("binance", {{100.0, 2.0}, {99.0, 2.0}},
                    {{101.0, 2.0}}, 1, 0);
    book.applyVenue("okx", {{101.0, 3.0}, {100.0, 1.0}},
                    {{102.0, 3.0}}, 1, 0);

    auto bb = book.bestBid();
    auto ba = book.bestAsk();
    test::check(bb.price == 101.0, "consolidated best bid = 101 (max across venues)");
    test::check(ba.price == 101.0, "consolidated best ask = 101 (min across venues)");

    // At price 100, binance has 2.0 and okx has 1.0 -> summed 3.0 in consolidated.
    auto top = book.topBids(5);
    bool found100 = false;
    for (auto& l : top)
        if (l.price == 100.0) { found100 = (l.size == 3.0); break; }
    test::check(found100, "size at price 100 summed across venues = 3.0");

    test::check(book.sequence() == 2, "consolidated sequence incremented per venue update");
}

TEST(consolidated_volume_band) {
    ConsolidatedBook book;
    book.setSymbol("BTCUSDT");
    // Single level: price 100, size 1000 -> notional 100,000.
    book.applyVenue("v", {{100.0, 1000.0}}, {{101.0, 1000.0}}, 1, 0);

    // Target 50,000 notional reached at price 100.0 (1000 * 100 >= 50000).
    double pb = book.volumeBandPrice(true, 50000.0);
    test::check(pb == 100.0, "volume band 50k at price 100");
    // Target far above available -> deepest available price returned.
    double pb2 = book.volumeBandPrice(true, 1e12);
    test::check(pb2 == 100.0, "volume band overshoot returns deepest price");
}

TEST(consolidated_price_bands) {
    ConsolidatedBook book;
    book.setSymbol("BTCUSDT");
    book.applyVenue("v", {{100.0, 1.0}}, {{102.0, 1.0}}, 1, 0);

    std::vector<double> bps = {50, 100};
    std::vector<double> bid_b, ask_b;
    book.priceBands(bps, bid_b, ask_b);
    // best bid 100 -> 100*(1-0.005)=99.5 ; best ask 102 -> 102*(1+0.005)=102.51
    test::check(std::abs(bid_b[0] - 99.5) < 1e-9, "50bps bid band = 99.5");
    test::check(std::abs(ask_b[0] - 102.51) < 1e-9, "50bps ask band = 102.51");
}

TEST(consolidated_empty_book) {
    ConsolidatedBook book;
    test::check(book.empty(), "a book with no venues is empty");
    test::check(book.bestBid().price == 0.0 && book.bestBid().size == 0.0,
                "best bid on an empty book is zero");
    test::check(book.bestAsk().price == 0.0, "best ask on an empty book is zero");
    test::check(book.volumeBandPrice(true, 1e6) == 0.0,
                "volume band with no depth returns 0, not a garbage price");
    test::check(book.topBids(10).empty() && book.topAsks(10).empty(), "no levels");

    // A venue that reports empty sides must contribute nothing either.
    book.applyVenue("v", {}, {}, 1, 0);
    test::check(book.bestBid().price == 0.0, "a venue with no levels adds no bid");
    test::check(book.bestAsk().price == 0.0, "a venue with no levels adds no ask");
}

TEST(consolidated_three_venues_sum_at_same_price) {
    ConsolidatedBook book;
    book.applyVenue("a", {{100.0, 1.0}, {99.0, 5.0}}, {{101.0, 1.0}}, 1, 0);
    book.applyVenue("b", {{100.0, 2.0}}, {{101.0, 2.0}, {102.0, 9.0}}, 1, 0);
    book.applyVenue("c", {{100.0, 3.0}, {98.0, 7.0}}, {{101.0, 3.0}}, 1, 0);

    const Book bids = book.topBids(10);
    test::check(bids.size() == 3, "three distinct bid prices after merge");
    test::check(bids[0].price == 100.0 && bids[0].size == 6.0,
                "size at 100 summed across all three venues (1+2+3)");
    test::check(bids[1].price == 99.0 && bids[2].price == 98.0,
                "bids strictly descending after merge");

    const Book asks = book.topAsks(10);
    test::check(asks[0].price == 101.0 && asks[0].size == 6.0,
                "size at 101 summed across all three venues (1+2+3)");
    test::check(asks[1].price == 102.0 && asks[1].size == 9.0,
                "asks strictly ascending after merge");
}

TEST(consolidated_venue_replaced_not_appended) {
    ConsolidatedBook book;
    book.applyVenue("a", {{100.0, 1.0}, {99.0, 1.0}}, {{101.0, 1.0}}, 1, 0);
    book.applyVenue("a", {{200.0, 1.0}}, {{201.0, 1.0}}, 2, 0);  // same venue again

    ConsolidatedBook::Snapshot s;
    book.fillSnapshot(s, 10, 10);
    test::check(s.venues.size() == 1,
                "re-applying a venue updates it instead of adding a duplicate");
    test::check(book.bestBid().price == 200.0, "stale levels from the old snapshot are gone");
    test::check(book.topBids(10).size() == 1, "only the new level remains");
    test::check(book.sequence() == 2, "each applyVenue counts as one update");
}

TEST(consolidated_fill_snapshot_views_are_zero_copy) {
    ConsolidatedBook book;
    book.setSymbol("BTCUSDT");
    book.applyVenue("binance",
                    {{100.0, 1.0}, {99.0, 1.0}, {98.0, 1.0}, {97.0, 1.0}},
                    {{101.0, 1.0}, {102.0, 1.0}, {103.0, 1.0}, {104.0, 1.0}},
                    7, 12345);

    ConsolidatedBook::Snapshot s;
    book.fillSnapshot(s, /*top_k=*/2, /*venue_depth=*/3);

    test::check(s.symbol == "BTCUSDT", "snapshot carries the symbol");
    test::check(s.sequence == 1 && s.timestamp_us == 12345,
                "snapshot carries the sequence and timestamp");
    test::check(s.bids.size() == 2 && s.asks.size() == 2,
                "top_k truncates the consolidated book");
    test::check(s.venues.size() == 1, "one venue view");
    test::check(s.venues[0].name && *s.venues[0].name == "binance",
                "venue view names the venue");
    test::check(s.venues[0].depth == 3, "venue view carries the trim depth");
    test::check(s.venues[0].bids && s.venues[0].bids->size() == 4,
                "venue view still sees the full book (trimming happens downstream)");

    // The whole point of VenueView: it aliases the live book rather than
    // copying it, so no per-update allocation happens on the hot path.
    const PriceLevel* before = s.venues[0].bids->data();
    book.fillSnapshot(s, 2, 3);
    test::check(s.venues[0].bids->data() == before,
                "venue view aliases the live book across refills (zero copy)");
}

TEST(consolidated_sequence_and_timestamp) {
    ConsolidatedBook book;
    test::check(book.sequence() == 0 && book.timestamp() == 0, "fresh book starts at 0");
    book.applyVenue("a", {{100.0, 1.0}}, {{101.0, 1.0}}, 1, 111);
    test::check(!book.empty() && book.sequence() == 1, "first update bumps the sequence");
    test::check(book.timestamp() == 111, "timestamp comes from the update");
    book.applyVenue("b", {{100.0, 1.0}}, {{101.0, 1.0}}, 2, 222);
    test::check(book.sequence() == 2, "sequence counts every venue update, not every venue");
    test::check(book.timestamp() == 222, "timestamp is the most recent venue's");
}

TEST(consolidated_one_sided_venues) {
    // A venue may report only one side (thin book, partial feed, or a venue
    // that simply has no resting offers). The consolidated BBO must still come
    // from whichever venues do have that side.
    ConsolidatedBook book;
    book.applyVenue("bids_only", {{100.0, 2.0}, {99.0, 2.0}}, {}, 1, 0);
    book.applyVenue("asks_only", {}, {{101.0, 3.0}, {102.0, 3.0}}, 1, 0);

    test::check(book.bestBid().price == 100.0 && book.bestBid().size == 2.0,
                "best bid comes from the venue that has bids");
    test::check(book.bestAsk().price == 101.0 && book.bestAsk().size == 3.0,
                "best ask comes from the venue that has asks");
    test::check(book.topBids(10).size() == 2, "both bid levels survive the merge");
    test::check(book.topAsks(10).size() == 2, "both ask levels survive the merge");
    test::check(book.bestBid().price < book.bestAsk().price,
                "the consolidated book is still uncrossed");
}

TEST(consolidated_volume_band_uses_full_depth) {
    // volumeBandPrice must walk the WHOLE book, not just the top-K that the
    // snapshot ships -- otherwise every deep band would report a shallow price.
    ConsolidatedBook book;
    Book bids, asks;
    for (int i = 0; i < 12; ++i) {
        bids.push_back({100.0 - i * 1.0, 10.0});  // 100 .. 89
        asks.push_back({101.0 + i * 1.0, 10.0});  // 101 .. 112
    }
    book.applyVenue("v", bids, asks, 1, 0);

    test::check(book.topBids(3).size() == 3, "top-K truncates for the snapshot");
    test::check(std::abs(book.volumeBandPrice(true, 1e9) - 89.0) < 1e-9,
                "the bid band walks past top-K to the deepest level (89)");
    test::check(std::abs(book.volumeBandPrice(false, 1e9) - 112.0) < 1e-9,
                "and does the same on the ask side (112)");
    // A target reachable near the top must not run to the bottom either.
    // Level 1 alone is 10*100 = 1000, so 1500 needs two levels -> price 99.
    test::check(std::abs(book.volumeBandPrice(true, 1500.0) - 99.0) < 1e-9,
                "a modest target stops early (1000 + 990 = 1990 >= 1500)");
}
