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
    : public ggrpc::ServerResponseWriterHandler<gg::UnaryResponse,
                                                gg::UnaryRequest> {
  gg::Test::AsyncService* service_;

 public:
  TestUnaryHandler(gg::Test::AsyncService* service) : service_(service) {}
  void Request(
      grpc::ServerContext* context, gg::UnaryRequest* request,
      grpc::ServerAsyncResponseWriter<gg::UnaryResponse>* response_writer,
      grpc::ServerCompletionQueue* cq, void* tag) override {
    service_->RequestUnary(context, request, response_writer, cq, cq, tag);
  }
  void OnAccept(gg::UnaryRequest request) override {
    SPDLOG_TRACE("received UnaryRequest: {}", request.DebugString());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    gg::UnaryResponse resp;
    resp.set_value(request.value() * 100);
    Context()->Finish(resp, grpc::Status::OK);
  }
  void OnError(ggrpc::ServerResponseWriterError error) {}
};

class TestBidiHandler
    : public ggrpc::ServerReaderWriterHandler<gg::BidiResponse,
                                              gg::BidiRequest> {
  gg::Test::AsyncService* service_;

 public:
  TestBidiHandler(gg::Test::AsyncService* service) : service_(service) {}
  void Request(grpc::ServerContext* context,
               grpc::ServerAsyncReaderWriter<gg::BidiResponse, gg::BidiRequest>*
                   streamer,
               grpc::ServerCompletionQueue* cq, void* tag) override {
    service_->RequestBidi(context, streamer, cq, cq, tag);
  }
  void OnAccept() override {
    gg::BidiResponse resp;
    resp.set_value(1);
    Context()->Write(resp);
  }
  void OnRead(gg::BidiRequest req) override {
    SPDLOG_TRACE("received BidiRequest {}", req.DebugString());
    gg::BidiResponse resp;
    resp.set_value(2);
    Context()->Write(resp);
  }
  void OnReadDoneOrError() override {
    gg::BidiResponse resp;
    resp.set_value(3);
    Context()->Write(resp);
    Context()->Finish(grpc::Status::OK);
  }
  void OnError(ggrpc::ServerReaderWriterError error) override {}
};

class TestServer {
  gg::Test::AsyncService service_;
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

typedef ggrpc::ClientResponseReader<gg::UnaryRequest, gg::UnaryResponse>
    UnaryClient;
typedef ggrpc::ClientReaderWriter<gg::BidiRequest, gg::BidiResponse> BidiClient;

class TestClientManager {
  ggrpc::ClientManager cm_;
  std::unique_ptr<gg::Test::Stub> stub_;

 public:
  TestClientManager(std::shared_ptr<grpc::Channel> channel, int threads)
      : cm_(10), stub_(gg::Test::NewStub(channel)) {}
  void Start() { cm_.Start(); }

  std::shared_ptr<UnaryClient> CreateUnary() {
    return cm_.CreateResponseReader<gg::UnaryRequest, gg::UnaryResponse>(
        [stub = stub_.get()](grpc::ClientContext* context,
                             const gg::UnaryRequest& request,
                             grpc::CompletionQueue* cq) {
          return stub->AsyncUnary(context, request, cq);
        });
  }

  std::shared_ptr<BidiClient> CreateBidi() {
    return cm_.CreateReaderWriter<gg::BidiRequest, gg::BidiResponse>(
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
    bidi->SetOnConnect([bidi]() { bidi->Close(); });
    bidi->Connect();
    bidi.reset();
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  {
    auto bidi = cm.CreateBidi();
    bidi->SetOnConnect([bidi]() {});
    bidi->SetOnRead([bidi](const gg::BidiResponse& resp) {
      SPDLOG_INFO("response: {}", resp.DebugString());
      bidi->Close();
    });
    bidi->Connect();
    gg::BidiRequest req;
    req.set_value(100);
    bidi->Write(req);
    bidi->WritesDone();
    bidi.reset();
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  {
    auto bidi = cm.CreateBidi();
    bidi->SetOnConnect([bidi]() {
      gg::BidiRequest req;
      req.set_value(100);
      bidi->Write(req);
      bidi->WritesDone();
    });
    bidi->SetOnRead([bidi](const gg::BidiResponse& resp) {
      SPDLOG_INFO("response: {}", resp.DebugString());
      bidi->Close();
    });
    bidi->Connect();
    bidi.reset();
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  {
    auto bidi = cm.CreateBidi();
    bidi->SetOnConnect([bidi]() {});
    bidi->Connect();
  }
}

void test_client_unary() {
  TestServer server;
  server.Start("0.0.0.0:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(1));

  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  gg::UnaryRequest req;

  {
    auto unary = cm.CreateUnary();
    req.set_value(100);
    unary->SetOnResponse(
        [unary](gg::UnaryResponse resp, grpc::Status) { unary->Close(); });
    unary->Request(req);
    unary.reset();
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  {
    auto unary = cm.CreateUnary();
    req.set_value(100);
    unary->SetOnResponse(
        [unary](gg::UnaryResponse resp, grpc::Status) { unary->Close(); });
    unary->Request(req);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}

void test_server() {
  TestServer server;
  server.Start("0.0.0.0:50051", 1);
  std::this_thread::sleep_for(std::chrono::seconds(1));
}

int main() {
  spdlog::set_level(spdlog::level::trace);

  test_client_bidi_connect_callback();
  test_client_unary();
  test_server();

  //std::unique_ptr<TestServer> server(new TestServer());
  //server->Start("0.0.0.0:50051", 10);
  //std::this_thread::sleep_for(std::chrono::seconds(1));

  //auto channel = grpc::CreateChannel("localhost:50051",
  //                                   grpc::InsecureChannelCredentials());
  //std::unique_ptr<ClientManager> cm(new ClientManager(channel, 10));
  //auto bidi = cm->TestBidi(
  //    []() { SPDLOG_TRACE("connected BidiTest"); },
  //    [](gg::BidiResponse resp) {
  //      SPDLOG_TRACE("received BidiResponse {}", resp.DebugString());
  //    },
  //    [](grpc::Status status) {
  //      SPDLOG_TRACE("gRPC Error: {}", status.error_message());
  //    },
  //    [](ggrpc::ClientReaderWriterError error) {
  //      SPDLOG_TRACE("ClientReaderWriterError: {}", (int)error);
  //    });
  //std::this_thread::sleep_for(std::chrono::seconds(1));
  //gg::BidiRequest req;
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

  //gg::UnaryRequest unary_req;
  //unary_req.set_value(1);
  //auto unary = cm->TestUnary(
  //    unary_req,
  //    [](gg::UnaryResponse resp, grpc::Status status) {
  //      SPDLOG_TRACE("received UnaryResponse {}", resp.DebugString());
  //    },
  //    [](ggrpc::ClientResponseReaderError error) {
  //      SPDLOG_TRACE("ClientResponseReaderError: {}", (int)error);
  //    });
  //std::this_thread::sleep_for(std::chrono::seconds(1));
}
