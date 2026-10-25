#include "feed/simulated_exchange.h"
#include "common/log.h"

#include <algorithm>
#include <chrono>
#include <cmath>

namespace aggregator {

namespace {
int64_t now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
}  // namespace

SimulatedExchange::SimulatedExchange(std::string venue, std::string symbol,
                                     std::shared_ptr<MarketState> market,
                                     double start_mid, unsigned seed,
                                     unsigned depth, unsigned interval_ms)
    : venue_(std::move(venue)),
      symbol_(std::move(symbol)),
      start_mid_(start_mid),
      depth_(depth),
      interval_ms_(interval_ms),
      rng_(seed),
      market_(std::move(market)),
      // Keep the venue's offset well inside the half-spread, so the
      // consolidated book (max bid / min ask across venues) cannot cross.
      offset_(start_mid - market_->startMid()),
      mid_(start_mid),
      spread_(start_mid * 0.0001)  // ~1bp half-spread
{
}

bool SimulatedExchange::start(VenueEventPublisher& pub) {
    if (running_.exchange(true)) return false;
    pub_ = &pub;

    // Emit a coherent initial book from this thread *before* starting the
    // producer thread, so the ring is already seeded and rng_ is never shared.
    emit(0);

    th_ = std::thread(&SimulatedExchange::run, this);
    LOG_INFO() << "[sim:" << venue_ << "] started (depth=" << depth_
               << ", interval=" << interval_ms_ << "ms)";
    return true;
}

void SimulatedExchange::stop() {
    if (!running_.exchange(false)) return;
    if (th_.joinable()) th_.join();
    LOG_INFO() << "[sim:" << venue_ << "] stopped";
}

void SimulatedExchange::fill(VenueEvent& e, uint64_t seq) {
    e.setVenue(venue_.c_str());
    e.setSymbol(symbol_.c_str());
    e.sequence     = seq;
    e.timestamp_us = now_us();

    // Vary this venue's half-spread a little so the consolidated BBO breathes.
    // Bounded to [0.9, 1.1]x: together with the venue offsets that keeps
    // best-bid < best-ask, i.e. the consolidated book stays uncrossed.
    std::uniform_real_distribution<double> spread_jitter(0.9, 1.1);
    spread_ = start_mid_ * 0.0001 * spread_jitter(rng_);

    const double   tick = mid_ * 0.00001;  // price grid
    const uint32_t n    = static_cast<uint32_t>(std::min<size_t>(depth_, MAX_DEPTH));

    for (uint32_t i = 0; i < n; ++i) {
        const double decay = std::pow(0.97, static_cast<double>(i));
        const double sz_b  = 40.0 * decay + (rng_() % 20);
        const double sz_a  = 40.0 * decay + (rng_() % 20);
        e.bids[i] = PriceLevel{mid_ - spread_ - i * tick, sz_b};
        e.asks[i] = PriceLevel{mid_ + spread_ + i * tick, sz_a};
    }
    e.bid_count = n;
    e.ask_count = n;
}

void SimulatedExchange::emit(uint64_t seq) {
    pub_->publish([&](VenueEvent& e) { fill(e, seq); });
}

void SimulatedExchange::run() {
    while (running_) {
        // Follow the shared mid plus this venue's fixed offset. Venues track
        // each other the way arbitrage keeps real venues aligned.
        mid_ = market_->advance(rng_) + offset_;

        emit(++seq_);

        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
    }
}

}  // namespace aggregator
