// Publisher for Best Bid-Offer (subscription mode BBO). Subscribes in BBO mode
// and prints the consolidated best bid/offer. The server sends just the top of
// book, so this client does no book math at all.
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
    DisplayThrottle  throttle(250);  // ~4 Hz display

    std::cout << "BBO Publisher -> " << target << " symbol=" << symbol << "\n";
    client.subscribe(symbol, BBO, [&](const MarketUpdate& mu) {
        if (!throttle.ready()) return;
        if (!mu.has_bbo()) return;
        const BboUpdate& b = mu.bbo();

        const double mid    = (b.best_bid() + b.best_ask()) / 2.0;
        const double spread = b.best_ask() - b.best_bid();
        const double bps    = mid > 0.0 ? (spread / mid) * 10000.0 : 0.0;

        std::cout << "[BBO] " << formatTimestamp(b.timestamp_us()) << " | seq="
                  << b.sequence() << " | bid=" << std::fixed << std::setprecision(2)
                  << b.best_bid() << " (" << std::setprecision(4) << b.best_bid_size()
                  << ") | ask=" << std::setprecision(2) << b.best_ask() << " ("
                  << std::setprecision(4) << b.best_ask_size()
                  << ") | mid=" << std::setprecision(2) << mid
                  << " | spread=" << spread << " (" << std::setprecision(2) << bps
                  << " bps)\n";
    });
    return 0;
}
