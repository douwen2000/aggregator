#include "feed/real_exchanges.h"
#include "common/log.h"
#include "common/types.h"

#include <boost/asio/connect.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/json.hpp>

#include "common/event_publisher.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;
namespace json = boost::json;

namespace aggregator {
namespace {

Book parseLevels(const json::value& arr) {
    Book out;
    if (!arr.is_array()) return out;
    for (auto& e : arr.as_array()) {
        if (!e.is_array() || e.as_array().size() < 2) continue;
        double p = std::stod(std::string(e.as_array()[0].as_string()));
        double q = std::stod(std::string(e.as_array()[1].as_string()));
        if (q == 0.0) continue;  // zero size => level removed
        out.push_back({p, q});
    }
    return out;
}

int64_t now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Generic snapshot parser for Binance / OKX (payload is a full book each message).
// Unwraps the exchange-specific envelope ({"data":...} or {"data":[...]}) and
// reads the given bids/asks keys.
bool parseSimple(const json::value& root, const std::string& venue,
                 const char* bids_key, const char* asks_key,
                 uint64_t seq, VenueSnapshot& out) {
    auto* obj = root.if_object();
    if (!obj) return false;
    const json::value* data = &root;
    if (auto* d = obj->if_contains("data")) {
        if (d->is_array() && !d->as_array().empty())
            data = &d->as_array().front();
        else
            data = d;
    }
    auto* dobj = data->if_object();
    if (!dobj) return false;
    auto* b = dobj->if_contains(bids_key);
    auto* a = dobj->if_contains(asks_key);
    if (!b || !a) return false;
    out.venue       = venue;
    out.symbol      = "BTCUSDT";
    out.sequence    = seq;
    out.timestamp_us = now_us();
    out.bids        = parseLevels(*b);
    out.asks        = parseLevels(*a);
    return true;
}

// Bybit v5 sends an initial "snapshot" then "delta" frames; we maintain a local
// book and emit a consolidated snapshot after applying each frame.
struct BybitState {
    std::map<double, double> bids, asks;
};

bool parseBybit(const json::value& root, const std::string& venue,
                const std::shared_ptr<BybitState>& st, uint64_t seq,
                VenueSnapshot& out) {
    auto* obj = root.if_object();
    if (!obj) return false;
    auto* d = obj->if_contains("data");
    if (!d || !d->is_array() || d->as_array().empty()) return false;
    auto* dobj = d->as_array().front().if_object();
    if (!dobj) return false;
    auto* b = dobj->if_contains("b");
    auto* a = dobj->if_contains("a");
    if (!b || !a) return false;

    auto apply = [&](const json::value& arr, std::map<double, double>& book) {
        for (auto& e : arr.as_array()) {
            if (!e.is_array() || e.as_array().size() < 2) continue;
            double p = std::stod(std::string(e.as_array()[0].as_string()));
            double q = std::stod(std::string(e.as_array()[1].as_string()));
            if (q == 0.0)
                book.erase(p);
            else
                book[p] = q;
        }
    };
    apply(*b, st->bids);
    apply(*a, st->asks);

    out.venue       = venue;
    out.symbol      = "BTCUSDT";
    out.sequence    = seq;
    out.timestamp_us = now_us();
    for (auto& kv : st->bids) out.bids.push_back({kv.first, kv.second});
    for (auto& kv : st->asks) out.asks.push_back({kv.first, kv.second});
    return true;
}

// Copy a parsed snapshot into a ring slot (fixed capacity, truncating beyond
// MAX_DEPTH so the event stays allocation-free).
void fill_(VenueEvent& e, const VenueSnapshot& snap) {
    e.setVenue(snap.venue.c_str());
    e.setSymbol(snap.symbol.c_str());
    e.sequence     = snap.sequence;
    e.timestamp_us = snap.timestamp_us;

    const uint32_t nb =
        static_cast<uint32_t>(std::min<size_t>(snap.bids.size(), MAX_DEPTH));
    for (uint32_t i = 0; i < nb; ++i) e.bids[i] = snap.bids[i];
    const uint32_t na =
        static_cast<uint32_t>(std::min<size_t>(snap.asks.size(), MAX_DEPTH));
    for (uint32_t i = 0; i < na; ++i) e.asks[i] = snap.asks[i];

    e.bid_count = nb;
    e.ask_count = na;
}

class WsFeed : public ExchangeFeed {
public:
    using Parser = std::function<bool(const std::string&, uint64_t, VenueSnapshot&)>;

    WsFeed(std::string venue, std::string host, std::string target,
           std::string subscribe, Parser parse)
        : venue_(std::move(venue)),
          host_(std::move(host)),
          target_(std::move(target)),
          subscribe_(std::move(subscribe)),
          parse_(std::move(parse)) {}

    const std::string& name() const override { return venue_; }

    bool start(VenueEventPublisher& pub) override {
        if (running_.exchange(true)) return false;
        pub_ = &pub;
        th_ = std::thread(&WsFeed::run, this);
        LOG_INFO() << "[real:" << venue_ << "] connecting to " << host_;
        return true;
    }

    void stop() override {
        if (!running_.exchange(false)) return;
        if (th_.joinable()) th_.join();
    }

private:
    void run() {
        while (running_) {
            try {
                net::io_context ioc;
                ssl::context ctx(ssl::context::tlsv12_client);
                ctx.set_default_verify_paths();

                beast::websocket::stream<ssl::stream<tcp::socket>> ws(ioc, ctx);
                auto results = tcp::resolver(ioc).resolve(host_, "443");
                net::connect(beast::get_lowest_layer(ws), results);
                ws.next_layer().handshake(ssl::stream_base::client);
                ws.handshake(host_, target_);
                if (!subscribe_.empty())
                    ws.write(net::buffer(subscribe_));

                beast::flat_buffer buf;
                while (running_) {
                    ws.read(buf);
                    std::string text(static_cast<const char*>(buf.data().data()),
                                     buf.data().size());
                    buf.consume(buf.size());
                    VenueSnapshot snap;
                    if (parse_(text, ++seq_, snap) && pub_)
                        pub_->publish([&](VenueEvent& e) { fill_(e, snap); });
                }
            } catch (const std::exception& e) {
                LOG_WARN() << "[real:" << venue_ << "] " << e.what();
                if (!running_) break;
                std::this_thread::sleep_for(std::chrono::seconds(1));  // backoff
            }
        }
    }

    std::string venue_, host_, target_, subscribe_;
    Parser     parse_;
    VenueEventPublisher* pub_ = nullptr;
    std::thread th_;
    std::atomic<bool> running_{false};
    uint64_t  seq_ = 0;
};

std::string toOkxInstId(const std::string& symbol) {
    if (symbol.size() > 4 && symbol.substr(symbol.size() - 4) == "USDT")
        return symbol.substr(0, symbol.size() - 4) + "-USDT";
    return symbol;
}

}  // namespace

std::vector<ExchangeFeedPtr> createRealFeeds(const std::string& symbol,
                                            VenueEventPublisher& pub) {
    std::vector<ExchangeFeedPtr> feeds;

    // Binance: top-20 depth snapshot every 100ms; subscription is via the URL.
    feeds.push_back(std::make_shared<WsFeed>(
        "binance",
        "stream.binance.com",
        "/ws/" + symbol + "@depth20@100ms",
        "",
        [symbol](const std::string& text, uint64_t seq, VenueSnapshot& out) -> bool {
            try {
                return parseSimple(json::parse(text), "binance", "b", "a", seq, out);
            } catch (...) { return false; }
        }));

    // OKX: books50 (50-level snapshot stream).
    feeds.push_back(std::make_shared<WsFeed>(
        "okx",
        "ws.okx.com",
        "/ws/v5/public",
        R"({"op":"subscribe","args":[{"channel":"books50","instId":")" +
            toOkxInstId(symbol) + R"("}]})",
        [symbol](const std::string& text, uint64_t seq, VenueSnapshot& out) -> bool {
            try {
                return parseSimple(json::parse(text), "okx", "bids", "asks", seq, out);
            } catch (...) { return false; }
        }));

    // Bybit: orderbook.50 (snapshot + delta) on the spot public stream.
    auto bybit_state = std::make_shared<BybitState>();
    feeds.push_back(std::make_shared<WsFeed>(
        "bybit",
        "stream.bybit.com",
        "/v5/public/spot",
        R"({"op":"subscribe","args":[{"channel":"orderbook.50","symbol":")" +
            symbol + R"("}]})",
        [symbol, bybit_state](const std::string& text, uint64_t seq,
                              VenueSnapshot& out) -> bool {
            try {
                return parseBybit(json::parse(text), "bybit", bybit_state, seq, out);
            } catch (...) { return false; }
        }));

    for (auto& f : feeds) f->start(pub);
    return feeds;
}

}  // namespace aggregator
