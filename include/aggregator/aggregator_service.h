#pragma once

#include "common/types.h"
#include "common/update_ptr.h"

#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "aggregator.grpc.pb.h"

namespace aggregator {

// gRPC service implementing the SubscribeBook streaming API.
//
// It does NOT touch the order book: the aggregator engine (single consumer
// thread) owns that state and hands this service an already-built, shared
// BookUpdate. This service only fans that shared message out.
class AggregatorServiceImpl final : public Aggregator::Service {
public:
    explicit AggregatorServiceImpl(std::string default_symbol);

    // Called by the aggregator engine thread on every published update.
    void onUpdate(BookUpdatePtr u);

    // Most recent update, used to seed a brand-new subscriber immediately.
    void setLatest(BookUpdatePtr u);
    BookUpdatePtr latest() const;

    // Number of active subscribers for a symbol (diagnostics).
    size_t subscriberCount(const std::string& symbol) const;

    // Wake every subscriber's RPC thread so it can return from wait() and let
    // the RPC complete. Required for a clean server shutdown.
    void closeAll();

private:
    grpc::Status Subscribe(grpc::ServerContext* context,
                           const SubscribeRequest* request,
                           grpc::ServerWriter<MarketUpdate>* writer) override;

    // Per-subscriber hand-off between the engine thread (producer) and the
    // gRPC RPC thread (consumer).
    struct Subscription {
        // Every subscriber still receives the same shared BookUpdate; its own
        // RPC thread narrows it to the requested mode before writing, so the
        // band math runs per-subscriber with no shared state to contend on.
        SubscribeMode mode = MODE_UNSPECIFIED;
        grpc::ServerWriter<MarketUpdate>* writer = nullptr;
        std::mutex               m;
        std::condition_variable  cv;

        // Bounded queue. Market data is "latest value wins": a slow subscriber
        // drops stale snapshots instead of growing without bound (which would
        // otherwise be an unbounded memory leak plus ever-growing latency).
        static constexpr size_t kCap = 32;
        BookUpdatePtr buf[kCap];
        size_t   head    = 0;
        size_t   count   = 0;
        bool     shutdown = false;
        uint64_t dropped = 0;

        void push(BookUpdatePtr u);
        bool wait(BookUpdatePtr& out);  // false once shut down
        void close();
    };

    std::string default_symbol_;

    mutable std::mutex sub_mtx_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<Subscription>>> subs_;

    mutable std::mutex latest_mtx_;
    BookUpdatePtr latest_;
};

}  // namespace aggregator
