#include "aggregator/aggregator_service.h"
#include "common/bands.h"
#include "common/log.h"

#include <algorithm>
#include <utility>

namespace aggregator {

AggregatorServiceImpl::AggregatorServiceImpl(std::string default_symbol)
    : default_symbol_(std::move(default_symbol)) {}

// ---- Subscription ----------------------------------------------------------

void AggregatorServiceImpl::Subscription::push(BookUpdatePtr u) {
    std::lock_guard<std::mutex> lk(m);
    if (shutdown) return;
    if (count == kCap) {
        // Drop the oldest: a stale snapshot is worthless to a market-data
        // consumer, and this keeps both memory and latency bounded.
        buf[head] = nullptr;
        head      = (head + 1) % kCap;
        --count;
        ++dropped;
    }
    buf[(head + count) % kCap] = std::move(u);
    ++count;
    cv.notify_one();
}

bool AggregatorServiceImpl::Subscription::wait(BookUpdatePtr& out) {
    std::unique_lock<std::mutex> lk(m);
    cv.wait(lk, [this] { return shutdown || count > 0; });
    if (shutdown) return false;
    out = std::move(buf[head]);
    buf[head] = nullptr;
    head      = (head + 1) % kCap;
    --count;
    return true;
}

void AggregatorServiceImpl::Subscription::close() {
    {
        std::lock_guard<std::mutex> lk(m);
        shutdown = true;
    }
    cv.notify_all();
}

// ---- Fan-out ---------------------------------------------------------------

void AggregatorServiceImpl::onUpdate(BookUpdatePtr u) {
    std::lock_guard<std::mutex> lk(sub_mtx_);
    auto it = subs_.find(u->symbol());
    if (it == subs_.end()) return;
    // Shared message: a refcount bump per subscriber, no protobuf deep copy.
    for (auto& s : it->second) s->push(u);
}

void AggregatorServiceImpl::setLatest(BookUpdatePtr u) {
    std::lock_guard<std::mutex> lk(latest_mtx_);
    latest_ = std::move(u);
}

BookUpdatePtr AggregatorServiceImpl::latest() const {
    std::lock_guard<std::mutex> lk(latest_mtx_);
    return latest_;
}

void AggregatorServiceImpl::closeAll() {
    std::lock_guard<std::mutex> lk(sub_mtx_);
    for (auto& kv : subs_)
        for (auto& s : kv.second) s->close();
}

size_t AggregatorServiceImpl::subscriberCount(const std::string& symbol) const {
    std::lock_guard<std::mutex> lk(sub_mtx_);
    auto it = subs_.find(symbol);
    return it == subs_.end() ? 0 : it->second.size();
}

// ---- RPC -------------------------------------------------------------------

namespace {

Book toBook(const google::protobuf::RepeatedPtrField<Level>& f) {
    Book b;
    b.reserve(f.size());
    for (const auto& l : f) b.push_back({l.price(), l.size()});
    return b;
}

// Narrow one consolidated book into the payload for a subscriber's mode.
//
// Runs in that subscriber's own RPC thread on the shared (immutable)
// BookUpdate, so subscribers of different modes never contend with each other
// and the single-consumer engine is untouched.
void fillMarketUpdate(const BookUpdate& b, SubscribeMode mode, MarketUpdate& out) {
    switch (mode) {
        case BBO: {
            auto* m = out.mutable_bbo();
            m->set_symbol(b.symbol());
            m->set_sequence(b.sequence());
            m->set_timestamp_us(b.timestamp_us());
            if (b.bids_size() > 0 && b.asks_size() > 0) {
                m->set_best_bid(b.bids(0).price());
                m->set_best_bid_size(b.bids(0).size());
                m->set_best_ask(b.asks(0).price());
                m->set_best_ask_size(b.asks(0).size());
            }
            return;
        }

        case VOLUME_BANDS: {
            auto* m = out.mutable_volume_bands();
            m->set_symbol(b.symbol());
            m->set_sequence(b.sequence());
            m->set_timestamp_us(b.timestamp_us());
            const Book bids = toBook(b.bids());
            const Book asks = toBook(b.asks());
            // Side names the book, not the direction you'd trade:
            //   BUY  = 买盘 (bid book), prices walk DOWN from best_bid
            //   SELL = 卖盘 (ask book), prices walk UP   from best_ask
            for (const auto& [label, notional] : notionalBands()) {
                auto* buy = m->add_buy();
                buy->set_label(label);
                buy->set_notional(notional);
                buy->set_price(volumeBandPrice(bids, notional));

                auto* sell = m->add_sell();
                sell->set_label(label);
                sell->set_notional(notional);
                sell->set_price(volumeBandPrice(asks, notional));
            }
            return;
        }

        case PRICE_BANDS: {
            auto* m = out.mutable_price_bands();
            m->set_symbol(b.symbol());
            m->set_sequence(b.sequence());
            m->set_timestamp_us(b.timestamp_us());
            if (b.bids_size() == 0 || b.asks_size() == 0) return;
            const double best_bid = b.bids(0).price();
            const double best_ask = b.asks(0).price();
            m->set_best_bid(best_bid);
            m->set_best_ask(best_ask);

            const Book bids = toBook(b.bids());
            const Book asks = toBook(b.asks());
            const std::vector<double>& bps = priceBandBps();
            std::vector<double> buy_vols, sell_vols;
            priceBandVolumes(bids, asks, best_bid, best_ask, bps, buy_vols, sell_vols);

            for (size_t i = 0; i < bps.size(); ++i) {
                const double f = bps[i] / 10000.0;
                // SELL = 卖盘 (ask book): the band walks UP from best_ask.
                auto* sell = m->add_sell();
                sell->set_bps(bps[i]);
                sell->set_price(best_ask * (1.0 + f));
                sell->set_volume(sell_vols[i]);

                // BUY = 买盘 (bid book): the band walks DOWN from best_bid.
                auto* buy = m->add_buy();
                buy->set_bps(bps[i]);
                buy->set_price(best_bid * (1.0 - f));
                buy->set_volume(buy_vols[i]);
            }
            return;
        }

        default:
            // FULL_BOOK (and MODE_UNSPECIFIED): ship the whole book. The only
            // mode that copies, since a oneof cannot borrow the shared message
            // -- it is meant for inspection, not for production fan-out.
            *out.mutable_book() = b;
            return;
    }
}

}  // namespace

grpc::Status AggregatorServiceImpl::Subscribe(
    grpc::ServerContext* /*context*/, const SubscribeRequest* request,
    grpc::ServerWriter<MarketUpdate>* writer) {
    std::string symbol = request->symbol().empty() ? default_symbol_ : request->symbol();

    // Only the configured symbol is aggregated. Reject anything else instead of
    // silently serving the wrong book: such a subscriber would land in a map
    // entry that never receives updates (and its RPC thread could never observe
    // the disconnect, since disconnect is only detected via Write() failing).
    if (symbol != default_symbol_) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                            "unknown symbol '" + symbol +
                                "'; this aggregator serves '" + default_symbol_ + "'");
    }

    auto sub = std::make_shared<Subscription>();
    sub->mode   = request->mode();
    sub->writer = writer;

    size_t active = 0;
    {
        std::lock_guard<std::mutex> lk(sub_mtx_);
        subs_[symbol].push_back(sub);
        active = subs_[symbol].size();
    }
    LOG_INFO() << "client subscribed to " << symbol
               << " (mode=" << SubscribeMode_Name(sub->mode)
               << ", active=" << active << ")";

    // Seed with the latest snapshot so the client does not wait a full interval.
    BookUpdatePtr seed = latest();
    if (seed) sub->push(std::move(seed));

    BookUpdatePtr u;
    MarketUpdate  out;
    while (sub->wait(u)) {
        // Clear() releases the oneof but keeps the submessage buffers allocated
        // for reuse, so the steady state allocates nothing per update.
        out.Clear();
        fillMarketUpdate(*u, sub->mode, out);
        if (!writer->Write(out)) {
            break;  // client disconnected or stream broke
        }
    }

    {
        std::lock_guard<std::mutex> lk(sub_mtx_);
        auto& v = subs_[symbol];
        v.erase(std::remove(v.begin(), v.end(), sub), v.end());
    }

    std::lock_guard<std::mutex> lk(sub->m);
    LOG_INFO() << "client unsubscribed from " << symbol
               << " (dropped=" << sub->dropped << ")";
    return grpc::Status::OK;
}

}  // namespace aggregator
