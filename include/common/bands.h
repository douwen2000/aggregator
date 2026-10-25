#pragma once

// Band math shared by whoever renders the bands. Lives in common/ (not under
// clients/) because the server computes the bands and only pushes the result:
// clients just print what they receive.

#include "common/types.h"

#include <string>
#include <utility>
#include <vector>

namespace aggregator {

// Which ORDER BOOK a band is measured on. 'Side' names the book, not the
// direction you'd trade:
//   BUY  = 买盘 / bid book: descending prices, keep levels at or above a limit
//   SELL = 卖盘 / ask book: ascending prices,  keep levels at or below a limit
enum class BandSide { Buy, Sell };

// Canonical band definitions. The server uses these to build each payload and
// ships the labels along, so clients never re-derive them.
inline const std::vector<std::pair<std::string, double>>& notionalBands() {
    static const std::vector<std::pair<std::string, double>> v = {
        {"1M", 1e6}, {"5M", 5e6}, {"10M", 10e6}, {"25M", 25e6}, {"50M+", 50e6}};
    return v;
}

inline const std::vector<double>& priceBandBps() {
    static const std::vector<double> v = {50, 100, 200, 500, 1000};
    return v;
}

// Price at which cumulative notional (size*price) from the top of book first
// reaches 'target'. 'side' must be ordered from the top: bids descending,
// asks ascending. If depth is insufficient, returns the deepest available price.
inline double volumeBandPrice(const Book& side, double target_notional) {
    double cum = 0.0, last_price = 0.0;
    for (const auto& l : side) {
        cum += l.size * l.price;
        last_price = l.price;
        if (cum >= target_notional) return l.price;
    }
    return last_price;
}

// Price bands around the BBO expressed in basis points (1bp = 1e-4).
inline void priceBands(double best_bid, double best_ask,
                       const std::vector<double>& bps,
                       std::vector<double>& bid_bands,
                       std::vector<double>& ask_bands) {
    bid_bands.resize(bps.size());
    ask_bands.resize(bps.size());
    for (size_t i = 0; i < bps.size(); ++i) {
        const double f = bps[i] / 10000.0;
        bid_bands[i]   = best_bid * (1.0 - f);
        ask_bands[i]   = best_ask * (1.0 + f);
    }
}

// Cumulative size (base units) available from the top of 'side' out to
// 'limit_price' inclusive. Grows monotonically as the band widens, because each
// wider band strictly contains the narrower one.
//
//   BUY  (bid book, descending): every level with price >= limit
//   SELL (ask book, ascending):  every level with price <= limit
inline double cumulativeSizeTo(const Book& side, BandSide s, double limit_price) {
    double total = 0.0;
    for (const auto& l : side) {
        const bool within =
            (s == BandSide::Buy) ? (l.price >= limit_price) : (l.price <= limit_price);
        if (!within) break;  // levels are ordered from the top, so we're done
        total += l.size;
    }
    return total;
}

// Volume at each bps band: cumulative size from the BBO out to that band.
//   buy_vols  = 买盘, from best_bid DOWN  to best_bid*(1 - f)
//   sell_vols = 卖盘, from best_ask UP    to best_ask*(1 + f)
inline void priceBandVolumes(const Book& bids, const Book& asks, double best_bid,
                             double best_ask, const std::vector<double>& bps,
                             std::vector<double>& buy_vols,
                             std::vector<double>& sell_vols) {
    buy_vols.resize(bps.size());
    sell_vols.resize(bps.size());
    for (size_t i = 0; i < bps.size(); ++i) {
        const double f = bps[i] / 10000.0;
        buy_vols[i]    = cumulativeSizeTo(bids, BandSide::Buy, best_bid * (1.0 - f));
        sell_vols[i]   = cumulativeSizeTo(asks, BandSide::Sell, best_ask * (1.0 + f));
    }
}

}  // namespace aggregator
