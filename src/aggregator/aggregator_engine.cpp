#include "aggregator/aggregator_engine.h"
#include "common/log.h"

#include <algorithm>
#include <chrono>
#include <utility>

namespace aggregator {

namespace {
int64_t now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
}  // namespace

AggregatorEngine::AggregatorEngine(VenueRing& ring, ConsolidatedBook& book,
                                   UpdateSink sink, uint64_t min_publish_us,
                                   size_t consolidated_depth, size_t venue_depth)
    : ring_(ring),
      book_(book),
      sink_(std::move(sink)),
      min_publish_us_(min_publish_us),
      consolidated_depth_(consolidated_depth),
      venue_depth_(venue_depth) {}

void AggregatorEngine::start() {
    if (running_.exchange(true)) return;
    th_ = std::thread(&AggregatorEngine::run, this);
    LOG_INFO() << "[engine] started (out<=" << 1000000 / min_publish_us_
               << "Hz, depth=" << consolidated_depth_
               << ", venue_depth=" << venue_depth_ << ")";
}

void AggregatorEngine::stop() {
    if (!running_.exchange(false)) return;
    if (th_.joinable()) th_.join();
    LOG_INFO() << "[engine] stopped (applied=" << eventsApplied()
               << " published=" << updatesPublished() << ")";
}

void AggregatorEngine::run() {
    int64_t next = 0;
    while (running_.load(std::memory_order_acquire)) {
        // Blocks until at least 'next' is published; returns the highest
        // contiguous published sequence so we can process the whole batch.
        const int64_t hi = ring_.waitFor(next, &running_);
        if (hi < 0) break;  // stop requested

        const uint64_t n = static_cast<uint64_t>(hi - next + 1);
        for (int64_t s = next; s <= hi; ++s) {
            const VenueEvent& e = ring_.slot(s);
            book_.applyVenue(e.venue, e.bids, e.bid_count, e.asks, e.ask_count,
                             e.sequence, e.timestamp_us);
        }
        ring_.publishConsumed(hi);  // release slots back to the producers
        next = hi + 1;
        events_.fetch_add(n, std::memory_order_relaxed);

        // Throttle outbound publication; the book itself is always current.
        const int64_t now = now_us();
        if (now - last_publish_us_ < static_cast<int64_t>(min_publish_us_)) continue;
        last_publish_us_ = now;
        publishOnce();
    }
}

void AggregatorEngine::publishOnce() {
    // Zero-copy: snap_ holds views into the book, and its vectors are reused.
    book_.fillSnapshot(snap_, consolidated_depth_, venue_depth_);

    auto u = std::make_shared<BookUpdate>();
    u->set_symbol(snap_.symbol);
    u->set_sequence(snap_.sequence);
    u->set_timestamp_us(snap_.timestamp_us);

    for (const auto& l : snap_.bids) {
        auto* p = u->add_bids();
        p->set_price(l.price);
        p->set_size(l.size);
    }
    for (const auto& l : snap_.asks) {
        auto* p = u->add_asks();
        p->set_price(l.price);
        p->set_size(l.size);
    }

    // Per-venue detail is reference data, not what publishers consume, so it
    // carries a shallow depth to keep the payload small.
    for (const auto& v : snap_.venues) {
        auto* vb = u->add_venues();
        vb->set_venue(*v.name);

        const size_t nb = std::min(v.depth, v.bids->size());
        for (size_t i = 0; i < nb; ++i) {
            auto* p = vb->add_bids();
            p->set_price((*v.bids)[i].price);
            p->set_size((*v.bids)[i].size);
        }
        const size_t na = std::min(v.depth, v.asks->size());
        for (size_t i = 0; i < na; ++i) {
            auto* p = vb->add_asks();
            p->set_price((*v.asks)[i].price);
            p->set_size((*v.asks)[i].size);
        }
    }

    published_.fetch_add(1, std::memory_order_relaxed);
    // Shared, not copied: every subscriber reads this same message.
    sink_(std::move(u));
}

}  // namespace aggregator
