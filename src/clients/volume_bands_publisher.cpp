// Publisher for Volume Bands (subscription mode VOLUME_BANDS): for notional
// bands 1M/5M/10M/25M/50M+, print the price at which cumulative traded notional
// from the top of book is reached, on both sides.
//
//   [VolumeBands] Timestamp | Side: BUY  | 1M: 79997.0 | 5M: 79992.4 | ...
//   [VolumeBands] Timestamp | Side: SELL | 1M: 80002.1 | 5M: 80006.5 | ...
//
// Side names the book: BUY = 买盘 (bid book, prices descend),
// SELL = 卖盘 (ask book, prices ascend).
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

    std::cout << "Volume-Bands Publisher -> " << target << " symbol=" << symbol
              << "\n";
    client.subscribe(symbol, VOLUME_BANDS, [&](const MarketUpdate& mu) {
        if (!throttle.ready()) return;
        if (!mu.has_volume_bands()) return;
        const VolumeBandsUpdate& v = mu.volume_bands();

        const std::string ts = formatTimestamp(v.timestamp_us());
        for (int side = 0; side < 2; ++side) {
            const auto& bands = (side == 0) ? v.buy() : v.sell();
            std::cout << "[VolumeBands] " << ts << " | Side: "
                      << (side == 0 ? "BUY " : "SELL") << " |";
            for (const auto& b : bands) {
                std::cout << " " << b.label() << ": " << std::fixed
                          << std::setprecision(2) << b.price();
            }
            std::cout << "\n";
        }
    });
    return 0;
}
