#include "aggregator/aggregator_engine.h"
#include "aggregator/aggregator_service.h"
#include "common/event_publisher.h"
#include "common/log.h"
#include "feed/exchange_feed.h"
#include "feed/simulated_exchange.h"

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "aggregator.grpc.pb.h"

#ifdef AGG_REAL_FEEDS
#include "feed/real_exchanges.h"
#endif

using namespace aggregator;

namespace {

volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

// Build the set of exchange feeds for the requested mode. Every feed writes
// into the same ring through its own producer handle.
std::vector<ExchangeFeedPtr> createFeeds(const std::string& mode,
                                         const std::string& symbol,
                                         VenueEventPublisher& pub) {
    std::vector<ExchangeFeedPtr> feeds;
    if (mode == "real") {
#ifdef AGG_REAL_FEEDS
        feeds = createRealFeeds(symbol, pub);
#else
        LOG_ERR() << "--feeds real requested but built without AGG_REAL_FEEDS; "
                  << "rebuild with -DAGG_REAL_FEEDS=ON";
        return {};
#endif
    } else {
        // Three simulated venues sharing one random-walking mid, each offset by
        // +/-2 USD. The offset spread (4) must stay well inside twice the
        // half-spread (~2 x 6 x 0.85 = 10.2), otherwise the consolidated book
        // goes crossed: its best bid is the max across venues and its best ask
        // the min. With these numbers the consolidated spread stays ~6 USD.
        auto market = std::make_shared<MarketState>(60000.0);
        feeds.push_back(std::make_shared<SimulatedExchange>("binance", symbol, market, 60000.0, 1));
        feeds.push_back(std::make_shared<SimulatedExchange>("okx",     symbol, market, 60002.0, 2));
        feeds.push_back(std::make_shared<SimulatedExchange>("bybit",   symbol, market, 59998.0, 3));
        for (auto& f : feeds) f->start(pub);
    }
    return feeds;
}

}  // namespace

int main(int argc, char** argv) {
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    int         port   = 50051;
    std::string symbol = "BTCUSDT";
    std::string mode   = "sim";

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--port" && i + 1 < argc) port = std::stoi(argv[++i]);
        else if (a == "--symbol" && i + 1 < argc) symbol = argv[++i];
        else if (a == "--feeds" && i + 1 < argc) mode = argv[++i];
        else if (a == "--help") {
            std::cout << "Usage: aggregator_server [--port N] [--symbol S] "
                         "[--feeds sim|real]\n";
            return 0;
        }
    }

    // The ring is ~2 MB of pre-allocated slots, so it lives on the heap.
    auto ring = std::make_unique<VenueRing>();
    VenueEventPublisher publisher(*ring);

    // Owned exclusively by the engine thread; set before the engine starts.
    ConsolidatedBook book;
    book.setSymbol(symbol);
    AggregatorServiceImpl service(symbol);

    AggregatorEngine engine(*ring, book,
                            [&](BookUpdatePtr u) {
                                service.setLatest(u);        // seed new clients
                                service.onUpdate(std::move(u));
                            },
                            /*min_publish_us=*/10'000,
                            /*consolidated_depth=*/50,
                            /*venue_depth=*/10);

    auto feeds = createFeeds(mode, symbol, publisher);
    if (feeds.empty() && mode == "real") return 1;

    engine.start();

    std::string addr = "0.0.0.0:" + std::to_string(port);
    grpc::ServerBuilder builder;
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    if (!server) {
        LOG_ERR() << "failed to start gRPC server on " << addr;
        engine.stop();
        return 1;
    }
    LOG_INFO() << "aggregator gRPC server listening on " << addr
               << " (symbol=" << symbol << ", feeds=" << mode << ")";

    while (!g_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    LOG_INFO() << "shutting down...";
    // Stop producers first, then let the consumer drain and exit.
    for (auto& f : feeds) f->stop();
    engine.stop();
    // Wake any RPC thread still blocked in Subscription::wait(), otherwise
    // Shutdown() would hang waiting for handlers that can never observe a
    // client disconnect on their own.
    service.closeAll();
    server->Shutdown();
    return 0;
}
