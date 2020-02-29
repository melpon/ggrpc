#include <functional>
#include <iostream>
#include <memory>
#include <thread>

// ggrpc
#include <ggrpc/client.h>
#include <ggrpc/server.h>

// spdlog
#include <spdlog/spdlog.h>

#include "ggrpc.grpc.pb.h"
#include "ggrpc.pb.h"

class TestUnaryHandler
    : public ggrpc::ServerResponseWriterHandler<ggrpctest::UnaryResponse,
                                                ggrpctest::UnaryRequest> {
  ggrpctest::Test::AsyncService* service_;

 public:
  TestUnaryHandler(ggrpctest::Test::AsyncService* service)
      : service_(service) {}
  void Request(grpc::ServerContext* context, ggrpctest::UnaryRequest* request,
               grpc::ServerAsyncResponseWriter<ggrpctest::UnaryResponse>*
                   response_writer,
               grpc::ServerCompletionQueue* cq, void* tag) override {
    service_->RequestUnary(context, request, response_writer, cq, cq, tag);
  }
  void OnAccept(ggrpctest::UnaryRequest request) override {
    SPDLOG_TRACE("received UnaryRequest: {}", request.DebugString());
    ggrpctest::UnaryResponse resp;
    resp.set_value(request.value() * 100);
    Finish(resp, grpc::Status::OK);
  }
};

class TestBidiHandler
    : public ggrpc::ServerReaderWriterHandler<ggrpctest::BidiResponse,
                                              ggrpctest::BidiRequest> {
  ggrpctest::Test::AsyncService* service_;

 public:
  TestBidiHandler(ggrpctest::Test::AsyncService* service) : service_(service) {}
  void Request(grpc::ServerContext* context,
               grpc::ServerAsyncReaderWriter<ggrpctest::BidiResponse,
                                             ggrpctest::BidiRequest>* streamer,
               grpc::ServerCompletionQueue* cq, void* tag) override {
    service_->RequestBidi(context, streamer, cq, cq, tag);
  }
  void OnAccept() override {
    ggrpctest::BidiResponse resp;
    resp.set_value(1);
    Write(resp);
  }
  void OnRead(ggrpctest::BidiRequest req) override {
    SPDLOG_TRACE("received BidiRequest {}", req.DebugString());
    ggrpctest::BidiResponse resp;
    resp.set_value(2);
    Write(resp);
  }
  void OnReadDoneOrError() override {
    ggrpctest::BidiResponse resp;
    resp.set_value(3);
    Write(resp);
    Finish(grpc::Status::OK);
  }
};

class TestServer {
  ggrpctest::Test::AsyncService service_;
  ggrpc::Server server_;

 public:
  void Start(std::string address, int threads) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);

    SPDLOG_INFO("gRPC Server listening on {}", address);

    // ハンドラの登録
    server_.AddReaderWriterHandler<TestBidiHandler>(&service_);
    server_.AddResponseWriterHandler<TestUnaryHandler>(&service_);

    server_.Start(builder, threads);
  }
};

typedef ggrpc::ClientResponseReader<ggrpctest::UnaryRequest,
                                    ggrpctest::UnaryResponse>
    UnaryClient;
typedef ggrpc::ClientReaderWriter<ggrpctest::BidiRequest,
                                  ggrpctest::BidiResponse>
    BidiClient;

class TestClientManager {
  ggrpc::ClientManager cm_;
  std::unique_ptr<ggrpctest::Test::Stub> stub_;

 public:
  TestClientManager(std::shared_ptr<grpc::Channel> channel, int threads)
      : cm_(10), stub_(ggrpctest::Test::NewStub(channel)) {}
  void Start() { cm_.Start(); }

  std::shared_ptr<UnaryClient> CreateUnary() {
    return cm_.CreateResponseReader<ggrpctest::UnaryRequest,
                                    ggrpctest::UnaryResponse>(
        [stub = stub_.get()](grpc::ClientContext* context,
                             const ggrpctest::UnaryRequest& request,
                             grpc::CompletionQueue* cq) {
          return stub->AsyncUnary(context, request, cq);
        });
  }

  std::shared_ptr<BidiClient> CreateBidi() {
    return cm_
        .CreateReaderWriter<ggrpctest::BidiRequest, ggrpctest::BidiResponse>(
            [stub = stub_.get()](grpc::ClientContext* context,
                                 grpc::CompletionQueue* cq, void* tag) {
              return stub->AsyncBidi(context, cq, tag);
            });
  }
};

// Bidiクライアントの接続コールバック時にいろいろやってもちゃんと動くか
void test_client_bidi_connect_callback() {
  TestServer server;
  server.Start("0.0.0.0:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  {
    auto bidi = cm.CreateBidi();
    bidi->SetOnConnect([bidi]() { bidi->Shutdown(); });
    bidi->Connect();
    bidi.reset();
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  {
    auto bidi = cm.CreateBidi();
    bidi->SetOnConnect([bidi]() {});
    bidi->SetOnRead([bidi](const ggrpctest::BidiResponse& resp) {
      SPDLOG_INFO("response: {}", resp.DebugString());
      bidi->Shutdown();
    });
    bidi->Connect();
    ggrpctest::BidiRequest req;
    req.set_value(100);
    bidi->Write(req);
    bidi->WritesDone();
    bidi.reset();
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  {
    auto bidi = cm.CreateBidi();
    bidi->SetOnConnect([bidi]() {
      ggrpctest::BidiRequest req;
      req.set_value(100);
      bidi->Write(req);
      bidi->WritesDone();
    });
    bidi->SetOnRead([bidi](const ggrpctest::BidiResponse& resp) {
      SPDLOG_INFO("response: {}", resp.DebugString());
      bidi->Shutdown();
    });
    bidi->Connect();
    bidi.reset();
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}
int main() {
  spdlog::set_level(spdlog::level::trace);

  test_client_bidi_connect_callback();

  //std::unique_ptr<TestServer> server(new TestServer());
  //server->Start("0.0.0.0:50051", 10);
  //std::this_thread::sleep_for(std::chrono::seconds(1));

  //auto channel = grpc::CreateChannel("localhost:50051",
  //                                   grpc::InsecureChannelCredentials());
  //std::unique_ptr<ClientManager> cm(new ClientManager(channel, 10));
  //auto bidi = cm->TestBidi(
  //    []() { SPDLOG_TRACE("connected BidiTest"); },
  //    [](ggrpctest::BidiResponse resp) {
  //      SPDLOG_TRACE("received BidiResponse {}", resp.DebugString());
  //    },
  //    [](grpc::Status status) {
  //      SPDLOG_TRACE("gRPC Error: {}", status.error_message());
  //    },
  //    [](ggrpc::ClientReaderWriterError error) {
  //      SPDLOG_TRACE("ClientReaderWriterError: {}", (int)error);
  //    });
  //std::this_thread::sleep_for(std::chrono::seconds(1));
  //ggrpctest::BidiRequest req;
  //req.set_value(100);
  //bidi->Write(req);
  //bidi->WritesDone();
  //std::this_thread::sleep_for(std::chrono::seconds(1));

  //auto alarm = cm->CreateAlarm(std::chrono::milliseconds(100),
  //                             []() { SPDLOG_TRACE("Alarm"); });
  //std::this_thread::sleep_for(std::chrono::seconds(1));

  //alarm = cm->CreateAlarm(std::chrono::milliseconds(100), [&]() {
  //  SPDLOG_TRACE("Alarm 2");
  //  alarm = cm->CreateAlarm(std::chrono::milliseconds(100),
  //                          []() { SPDLOG_TRACE("Alarm 3"); });
  //});
  //std::this_thread::sleep_for(std::chrono::seconds(1));

  //// 即座にキャンセルする
  //alarm = cm->CreateAlarm(std::chrono::milliseconds(100),
  //                        [&]() { SPDLOG_ERROR("Alarm 4"); });
  //alarm.reset();
  //std::this_thread::sleep_for(std::chrono::seconds(1));

  //ggrpctest::UnaryRequest unary_req;
  //unary_req.set_value(1);
  //auto unary = cm->TestUnary(
  //    unary_req,
  //    [](ggrpctest::UnaryResponse resp, grpc::Status status) {
  //      SPDLOG_TRACE("received UnaryResponse {}", resp.DebugString());
  //    },
  //    [](ggrpc::ClientResponseReaderError error) {
  //      SPDLOG_TRACE("ClientResponseReaderError: {}", (int)error);
  //    });
  //std::this_thread::sleep_for(std::chrono::seconds(1));
}
