// Publisher for Price Bands (subscription mode PRICE_BANDS): BBO +/- 50/100/200/
// 500/1000 bps, with the cumulative size available from the BBO out to each band.
//
//   [PriceBands] Timestamp | Side: SELL | +50bps: 80400 (Vol: 500) | +100bps: ...
//
// Side names the book: SELL = 卖盘 (ask book), the band walks UP from best_ask;
// BUY = 买盘 (bid book), the band walks DOWN from best_bid. 1bp = 1e-4, so the
// price is BBO * (1 +/- bps/1e4).
// Vol is cumulative base units from the BBO out to the band, so it grows with it.
#include "clients/aggregator_client.h"
#include "clients/publish_common.h"
#include "common/log.h"

#include <iomanip>
#include <iostream>
#include <string>

using namespace aggregator;

int main(int argc, char** argv) {
    std::string target = "localhost:50051";
    std::string symbol = "BTCUSDT";
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--target" && i + 1 < argc) target = argv[++i];
        else if (a == "--symbol" && i + 1 < argc) symbol = argv[++i];
    }

    AggregatorClient client(target);
    DisplayThrottle  throttle(250);

    std::cout << "Price-Bands Publisher -> " << target << " symbol=" << symbol
              << "\n";
    client.subscribe(symbol, PRICE_BANDS, [&](const MarketUpdate& mu) {
        if (!throttle.ready()) return;
        if (!mu.has_price_bands()) return;
        const PriceBandsUpdate& p = mu.price_bands();

        const std::string ts = formatTimestamp(p.timestamp_us());
        for (int side = 0; side < 2; ++side) {
            const auto& bands = (side == 0) ? p.buy() : p.sell();
            std::cout << "[PriceBands] " << ts << " | Side: "
                      << (side == 0 ? "BUY " : "SELL") << " |";
            for (const auto& b : bands) {
                std::cout << " +" << std::fixed << std::setprecision(0) << b.bps()
                          << "bps: " << std::setprecision(2) << b.price()
                          << " (Vol: " << std::setprecision(2) << b.volume() << ")";
            }
            std::cout << "\n";
        }
    });
    return 0;
}
