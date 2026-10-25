#pragma once

#include <aggregator.grpc.pb.h>
#include <grpcpp/grpcpp.h>

#include <functional>
#include <memory>
#include <string>

namespace aggregator {

// Synchronous gRPC client for the Aggregator streaming API.
class AggregatorClient {
public:
    explicit AggregatorClient(const std::string& target);

    // Subscribe to a symbol in 'mode' and block, invoking 'cb' for each
    // MarketUpdate until the stream ends or the process is terminated.
    // The server computes the bands, so the client only receives (and prints)
    // the payload for its own mode.
    void subscribe(const std::string& symbol, SubscribeMode mode,
                   std::function<void(const MarketUpdate&)> cb);

private:
    std::shared_ptr<grpc::Channel>            channel_;
    std::unique_ptr<Aggregator::Stub>         stub_;
};

}  // namespace aggregator
