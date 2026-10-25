#include "clients/aggregator_client.h"
#include "common/log.h"

namespace aggregator {

AggregatorClient::AggregatorClient(const std::string& target)
    : channel_(grpc::CreateChannel(target, grpc::InsecureChannelCredentials())),
      stub_(Aggregator::NewStub(channel_)) {}

void AggregatorClient::subscribe(const std::string& symbol, SubscribeMode mode,
                                 std::function<void(const MarketUpdate&)> cb) {
    SubscribeRequest req;
    req.set_symbol(symbol);
    req.set_mode(mode);

    grpc::ClientContext context;
    auto reader = stub_->Subscribe(&context, req);  // returns unique_ptr

    MarketUpdate update;
    while (reader->Read(&update)) {
        cb(update);
    }
    grpc::Status status = reader->Finish();
    if (!status.ok()) {
        LOG_ERR() << "subscribe stream ended: " << status.error_message();
    }
}

}  // namespace aggregator
