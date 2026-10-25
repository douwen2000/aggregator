#include "test/test_harness.h"
#include "feed/order_book.h"

using namespace aggregator;

TEST(order_book_basic) {
    OrderBook ob;
    Book bids = {{100.0, 1.0}, {99.5, 2.0}, {99.0, 3.0}};  // descending
    Book asks = {{100.5, 1.5}, {101.0, 2.5}, {101.5, 3.5}}; // ascending
    ob.applySnapshot(bids, asks);

    auto bb = ob.bestBid();
    auto ba = ob.bestAsk();
    test::check(bb.price == 100.0 && bb.size == 1.0, "best bid = 100.0/1.0");
    test::check(ba.price == 100.5 && ba.size == 1.5, "best ask = 100.5/1.5");

    auto tb = ob.topBids(2);
    test::check(tb.size() == 2 && tb[0].price == 100.0 && tb[1].price == 99.5,
                "topBids(2) descending");
    auto ta = ob.topAsks(2);
    test::check(ta.size() == 2 && ta[0].price == 100.5 && ta[1].price == 101.0,
                "topAsks(2) ascending");
}

TEST(order_book_replace) {
    OrderBook ob;
    ob.applySnapshot({{10.0, 5.0}}, {{11.0, 5.0}});
    // Replace with a new snapshot; old levels must be gone.
    ob.applySnapshot({{20.0, 1.0}}, {{21.0, 1.0}});
    test::check(ob.bestBid().price == 20.0, "replaced best bid");
    test::check(ob.bestAsk().price == 21.0, "replaced best ask");
    test::check(ob.topBids(10).size() == 1, "only one bid level remains");
}

TEST(order_book_empty) {
    OrderBook ob;
    test::check(ob.empty(), "a fresh book is empty");
    test::check(ob.bestBid().price == 0.0 && ob.bestBid().size == 0.0,
                "best bid on an empty book is zero");
    test::check(ob.bestAsk().price == 0.0 && ob.bestAsk().size == 0.0,
                "best ask on an empty book is zero");
    test::check(ob.topBids(5).empty() && ob.topAsks(5).empty(),
                "no levels on either side");
    test::check(ob.bids().empty() && ob.asks().empty(), "no stored levels");
}

TEST(order_book_top_k_clamped) {
    OrderBook ob;
    ob.applySnapshot({{100.0, 1.0}, {99.0, 1.0}}, {{101.0, 1.0}});
    test::check(ob.topBids(10).size() == 2, "k above the bid depth is clamped");
    test::check(ob.topAsks(10).size() == 1, "k above the ask depth is clamped");
    test::check(ob.topBids(0).empty(), "k = 0 yields nothing");
    test::check(ob.topBids(1).size() == 1 && ob.topBids(1)[0].price == 100.0,
                "k = 1 yields just the best level");
}

TEST(order_book_sorts_unsorted_snapshot) {
    // Producers are meant to send sorted levels, but the book is keyed by price,
    // so even a scrambled snapshot must come out ordered -- otherwise a badly
    // behaved venue could corrupt the merge downstream.
    OrderBook ob;
    ob.applySnapshot({{98.0, 3.0}, {100.0, 1.0}, {99.0, 2.0}},    // scrambled
                     {{103.0, 3.0}, {101.0, 1.0}, {102.0, 2.0}}); // scrambled

    const Book tb = ob.topBids(10);
    const Book ta = ob.topAsks(10);
    test::check(tb[0].price == 100.0 && tb[1].price == 99.0 && tb[2].price == 98.0,
                "bids come out descending regardless of input order");
    test::check(ta[0].price == 101.0 && ta[1].price == 102.0 && ta[2].price == 103.0,
                "asks come out ascending regardless of input order");
    test::check(tb[2].size == 3.0 && ta[2].size == 3.0,
                "each size travels with its own price");
}

TEST(order_book_duplicate_price_last_wins) {
    // A repeated price is one level, not two; a stale duplicate must not inflate
    // the consolidated size downstream.
    OrderBook ob;
    ob.applySnapshot({{100.0, 1.0}, {100.0, 9.0}}, {{101.0, 1.0}, {101.0, 9.0}});
    test::check(ob.topBids(10).size() == 1, "duplicate bid price collapses to one level");
    test::check(ob.topAsks(10).size() == 1, "duplicate ask price collapses to one level");
    test::check(ob.bestBid().size == 9.0, "last size wins for a repeated bid price");
    test::check(ob.bestAsk().size == 9.0, "last size wins for a repeated ask price");
}
