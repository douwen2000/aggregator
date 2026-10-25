#pragma once

#include "feed/exchange_feed.h"
#include <string>
#include <vector>

namespace aggregator {

class VenueEventPublisher;

// Build real CEX connectors (Binance, OKX, Bybit) that stream BTCUSDT depth and
// feed the aggregator. Requires Boost.Beast + Boost.Json + OpenSSL; compiled only
// when the build defines AGG_REAL_FEEDS=ON.
std::vector<ExchangeFeedPtr> createRealFeeds(const std::string& symbol,
                                            VenueEventPublisher& pub);

}  // namespace aggregator
