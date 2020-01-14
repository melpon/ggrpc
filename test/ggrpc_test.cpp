#include <functional>
#include <iostream>
#include <memory>
#include <thread>

// ggrpc
#include <ggrpc/client.h>

// spdlog
#include <spdlog/spdlog.h>

#include "ggrpc.grpc.pb.h"
#include "ggrpc.pb.h"

class ClientManager {
  std::unique_ptr<ggrpc::ClientManager> gcm_;
  std::unique_ptr<ggrpctest::Test::Stub> stub_;

 public:
  ClientManager(std::shared_ptr<grpc::Channel> channel, int threads)
      : stub_(ggrpctest::Test::NewStub(channel)),
        gcm_(new ggrpc::ClientManager(threads)) {}

  void Shutdown() { gcm_->Shutdown(); }

  std::unique_ptr<ggrpc::WritableClient<ggrpctest::BidiRequest>> TestBidi(
      std::function<void(ggrpctest::BidiResponse)> read,
      std::function<void(grpc::Status)> done,
      std::function<void(ggrpc::ClientReaderWriterError)> on_error) {
    auto async_connect = [stub = stub_.get()](grpc::ClientContext* context,
                                              grpc::CompletionQueue* cq,
                                              void* tag) {
      return stub->AsyncBidi(context, cq, tag);
    };
    return gcm_->CreateReaderWriterClient<ggrpctest::BidiRequest,
                                          ggrpctest::BidiResponse>(
        std::move(async_connect), std::move(read), std::move(done),
        std::move(on_error));
  }
};

int main() {
  spdlog::set_level(spdlog::level::trace);

  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  std::unique_ptr<ClientManager> cm(new ClientManager(channel, 10));
  auto bidi = cm->TestBidi([](ggrpctest::BidiResponse resp) {},
                           [](grpc::Status status) {},
                           [](ggrpc::ClientReaderWriterError error) {
                           });
  std::this_thread::sleep_for(std::chrono::seconds(1));
}
