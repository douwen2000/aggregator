#pragma once

#include "common/event_publisher.h"
#include "feed/exchange_feed.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>

namespace aggregator {

// The one random-walking mid that every simulated venue tracks.
//
// If each venue walked on its own, the mids would diverge without bound and the
// consolidated book would go crossed (best bid above best ask) after ~30s,
// because the consolidated best bid is the MAX across venues and the best ask
// is the MIN. Real venues never drift apart like that: arbitrage keeps them
// within a spread of each other. So they share this mid and differ only by a
// small fixed offset.
class MarketState {
public:
    // 'step_vol_frac' is the per-step move as a fraction of the current mid.
    //
    // It must stay well below the half-spread (~6 USD). The consolidated book
    // mixes snapshots taken a few ms apart, so the venues effectively observe
    // the shared mid at slightly different times; that timing skew is ~2 steps
    // and eats directly into the consolidated spread. At 0.0002 per step the
    // skew alone was ~15 USD and the book crossed again.
    explicit MarketState(double start_mid, double step_vol_frac = 0.000015)
        : start_mid_(start_mid),
          mid_(start_mid),
          vol_(step_vol_frac),
          lo_(start_mid * 0.5),
          hi_(start_mid * 1.5) {}

    double startMid() const { return start_mid_; }

    // Take one random step and return the new shared mid. Called from every
    // venue thread, hence the lock (150 calls/s total: negligible).
    double advance(std::mt19937& rng) {
        std::lock_guard<std::mutex> lk(mtx_);
        std::normal_distribution<double> noise(0.0, mid_ * vol_);
        mid_ += noise(rng);
        if (mid_ < lo_) mid_ = lo_;
        if (mid_ > hi_) mid_ = hi_;
        return mid_;
    }

private:
    std::mutex mtx_;
    double     start_mid_;
    double     mid_;
    double     vol_;
    double     lo_, hi_;
};

// Offline, deterministic-ish market-data simulator that emits full order-book
// snapshots at a fixed cadence. Three of these (binance/okx/bybit) feed the
// aggregator so the whole system runs without internet access and is testable.
//
// It writes straight into a claimed ring-buffer slot: no intermediate snapshot
// object, no per-event allocation.
class SimulatedExchange : public ExchangeFeed {
public:
    // 'start_mid' fixes this venue's offset from the shared mid: the venue's
    // quotes sit at (shared mid + start_mid - market->startMid()).
    SimulatedExchange(std::string venue, std::string symbol,
                      std::shared_ptr<MarketState> market, double start_mid,
                      unsigned seed, unsigned depth = 50, unsigned interval_ms = 20);

    const std::string& name() const override { return venue_; }
    bool start(VenueEventPublisher& pub) override;
    void stop() override;

private:
    void run();
    void emit(uint64_t seq);
    void fill(VenueEvent& e, uint64_t seq);

    std::string venue_;
    std::string symbol_;
    double      start_mid_;
    unsigned    depth_;
    unsigned    interval_ms_;
    std::mt19937 rng_;

    std::shared_ptr<MarketState> market_;
    double offset_ = 0.0;  // this venue's fixed offset from the shared mid

    double   mid_    = 0.0;
    double   spread_ = 0.0;  // half-spread in quote terms
    uint64_t seq_    = 0;

    VenueEventPublisher* pub_ = nullptr;
    std::thread          th_;
    std::atomic<bool>    running_{false};
};

}  // namespace aggregator
