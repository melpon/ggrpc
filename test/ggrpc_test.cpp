#include <functional>
#include <iostream>
#include <memory>
#include <thread>

// ggrpc
#include <ggrpc/ggrpc.h>

// spdlog
#include <spdlog/spdlog.h>

#include "ggrpc.grpc.pb.h"
#include "ggrpc.pb.h"

#define ASSERT(x)                  \
  if (!(x)) {                      \
    SPDLOG_ERROR("assert {}", #x); \
    std::exit(1);                  \
  }

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
    for (const auto& pair : GetGrpcContext()->client_metadata()) {
      GetGrpcContext()->AddInitialMetadata(
          std::string(pair.first.begin(), pair.first.end()),
          std::string(pair.second.begin(), pair.second.end()));
    }
    gg::UnaryResponse resp;
    resp.set_value(request.value() * 100);
    Context()->Finish(resp, grpc::Status::OK);
  }
  void OnError(ggrpc::ServerResponseWriterError error) override {}
};

class TestSstreamHandler
    : public ggrpc::ServerWriterHandler<gg::SstreamResponse,
                                        gg::SstreamRequest> {
  gg::Test::AsyncService* service_;

 public:
  TestSstreamHandler(gg::Test::AsyncService* service) : service_(service) {}
  void Request(grpc::ServerContext* context, gg::SstreamRequest* request,
               grpc::ServerAsyncWriter<gg::SstreamResponse>* writer,
               grpc::ServerCompletionQueue* cq, void* tag) override {
    service_->RequestSstream(context, request, writer, cq, cq, tag);
  }
  void OnAccept(gg::SstreamRequest request) override {
    SPDLOG_TRACE("received SstreamRequest: {}", request.DebugString());
    std::this_thread::sleep_for(std::chrono::seconds(1));
    gg::SstreamResponse resp;
    resp.set_value(request.value() * 1);
    Context()->Write(resp);
    resp.set_value(request.value() * 2);
    Context()->Write(resp);
    Context()->Finish(grpc::Status::OK);
  }
  void OnError(ggrpc::ServerWriterError error) override {}
};

class TestCstreamHandler
    : public ggrpc::ServerReaderHandler<gg::CstreamResponse,
                                        gg::CstreamRequest> {
  gg::Test::AsyncService* service_;
  int sum_ = 0;

 public:
  TestCstreamHandler(gg::Test::AsyncService* service) : service_(service) {}
  void Request(
      grpc::ServerContext* context,
      grpc::ServerAsyncReader<gg::CstreamResponse, gg::CstreamRequest>* reader,
      grpc::ServerCompletionQueue* cq, void* tag) override {
    service_->RequestCstream(context, reader, cq, cq, tag);
  }
  void OnAccept() override { gg::CstreamResponse resp; }
  void OnRead(gg::CstreamRequest req) override {
    SPDLOG_TRACE("received CstreamRequest {}", req.DebugString());
    if (req.value() == 100) {
      gg::CstreamResponse resp;
      resp.set_value(sum_);
      Context()->Finish(resp, grpc::Status::OK);
    } else {
      sum_ += req.value();
    }
  }
  void OnReadDoneOrError() override {
    gg::CstreamResponse resp;
    resp.set_value(sum_);
    Context()->Finish(resp, grpc::Status::OK);
  }
  void OnError(ggrpc::ServerReaderError error) override {}
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
    resp.set_value(req.value() * 2);
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
  ggrpc::Server* Server() { return &server_; }

  void Start(std::string address, int threads) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);

    SPDLOG_INFO("gRPC Server listening on {}", address);

    // ハンドラの登録
    server_.AddResponseWriterHandler<TestUnaryHandler>(&service_);
    server_.AddWriterHandler<TestSstreamHandler>(&service_);
    server_.AddReaderHandler<TestCstreamHandler>(&service_);
    server_.AddReaderWriterHandler<TestBidiHandler>(&service_);

    server_.Start(builder, threads);
  }
};

typedef ggrpc::ClientResponseReader<gg::UnaryRequest, gg::UnaryResponse>
    UnaryClient;
typedef ggrpc::ClientReader<gg::SstreamRequest, gg::SstreamResponse>
    SstreamClient;
typedef ggrpc::ClientWriter<gg::CstreamRequest, gg::CstreamResponse>
    CstreamClient;
typedef ggrpc::ClientReaderWriter<gg::BidiRequest, gg::BidiResponse> BidiClient;

typedef ggrpc::ClientResponseReader<grpc::ByteBuffer, grpc::ByteBuffer>
    UnaryClientGeneric;
typedef ggrpc::ClientReader<grpc::ByteBuffer, grpc::ByteBuffer>
    SstreamClientGeneric;
typedef ggrpc::ClientWriter<grpc::ByteBuffer, grpc::ByteBuffer>
    CstreamClientGeneric;
typedef ggrpc::ClientReaderWriter<grpc::ByteBuffer, grpc::ByteBuffer>
    BidiClientGeneric;

class TestClientManager {
  ggrpc::ClientManager cm_;
  std::unique_ptr<gg::Test::Stub> stub_;
  std::shared_ptr<grpc::Channel> channel_;

 public:
  TestClientManager(std::shared_ptr<grpc::Channel> channel, int threads)
      : cm_(threads), stub_(gg::Test::NewStub(channel)), channel_(channel) {}
  void Start() { cm_.Start(); }

  ggrpc::ClientManager* ClientManager() { return &cm_; }

  std::shared_ptr<UnaryClient> CreateUnary(
      int milliseconds = 0, std::map<std::string, std::string> meta = {}) {
    return cm_.CreateResponseReader<gg::UnaryRequest, gg::UnaryResponse>(
        [stub = stub_.get(), milliseconds, meta = std::move(meta)](
            grpc::ClientContext* context, const gg::UnaryRequest& request,
            grpc::CompletionQueue* cq) {
          for (const auto& p : meta) {
            context->AddMetadata(p.first, p.second);
          }
          if (milliseconds != 0) {
            context->set_deadline(std::chrono::system_clock::now() +
                                  std::chrono::milliseconds(milliseconds));
          }
          return stub->AsyncUnary(context, request, cq);
        });
  }
  std::shared_ptr<UnaryClientGeneric> CreateUnaryGeneric() {
    return cm_.CreateResponseReader<grpc::ByteBuffer, grpc::ByteBuffer>(
        [channel = channel_](grpc::ClientContext* context,
                             const grpc::ByteBuffer& request,
                             grpc::CompletionQueue* cq) {
          grpc::internal::RpcMethod method(
              "/gg.Test/Unary", grpc::internal::RpcMethod::NORMAL_RPC, channel);
          return std::unique_ptr<
              grpc::ClientAsyncResponseReader<grpc::ByteBuffer>>(
              grpc::internal::ClientAsyncResponseReaderFactory<
                  grpc::ByteBuffer>::Create(channel.get(), cq, method, context,
                                            request, true));
        });
  }

  std::shared_ptr<SstreamClient> CreateSstream() {
    return cm_.CreateReader<gg::SstreamRequest, gg::SstreamResponse>(
        [stub = stub_.get()](grpc::ClientContext* context,
                             const gg::SstreamRequest& request,
                             grpc::CompletionQueue* cq, void* tag) {
          return stub->AsyncSstream(context, request, cq, tag);
        });
  }

  std::shared_ptr<CstreamClient> CreateCstream() {
    return cm_.CreateWriter<gg::CstreamRequest, gg::CstreamResponse>(
        [stub = stub_.get()](grpc::ClientContext* context,
                             gg::CstreamResponse* response,
                             grpc::CompletionQueue* cq, void* tag) {
          return stub->AsyncCstream(context, response, cq, tag);
        });
  }

  std::shared_ptr<BidiClient> CreateBidi() {
    return cm_.CreateReaderWriter<gg::BidiRequest, gg::BidiResponse>(
        [stub = stub_.get()](grpc::ClientContext* context,
                             grpc::CompletionQueue* cq, void* tag) {
          return stub->AsyncBidi(context, cq, tag);
        });
  }

  void TestCompileGeneric() {
    auto unary = cm_.CreateResponseReader<grpc::ByteBuffer, grpc::ByteBuffer>(
        [](grpc::ClientContext* context, const grpc::ByteBuffer& request,
           grpc::CompletionQueue* cq) { return nullptr; });
    auto ss = cm_.CreateReader<grpc::ByteBuffer, grpc::ByteBuffer>(
        [](grpc::ClientContext* context, const grpc::ByteBuffer& request,
           grpc::CompletionQueue* cq, void* tag) { return nullptr; });
    auto cs = cm_.CreateWriter<grpc::ByteBuffer, grpc::ByteBuffer>(
        [](grpc::ClientContext* context, grpc::ByteBuffer* response,
           grpc::CompletionQueue* cq, void* tag) { return nullptr; });
    auto bidi = cm_.CreateReaderWriter<grpc::ByteBuffer, grpc::ByteBuffer>(
        [](grpc::ClientContext* context, grpc::CompletionQueue* cq, void* tag) {
          return nullptr;
        });
    grpc::ByteBuffer req;
    cs->Write(req);
    bidi->Write(req);
  }
};

// Bidiクライアントの接続コールバック時にいろいろやってもちゃんと動くか
void test_client_bidi_connect_callback() {
  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  TestServer server;
  server.Start("localhost:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(2));

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
    std::atomic<int> n = 0;
    auto bidi = cm.CreateBidi();
    bidi->SetOnConnect([bidi]() {
      gg::BidiRequest req;
      req.set_value(100);
      bidi->Write(req);
    });
    bidi->SetOnRead([&n](const gg::BidiResponse& resp) { n += resp.value(); });
    bidi->Connect();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT(n == 201);
  }

  {
    auto bidi = cm.CreateBidi();
    bidi->SetOnConnect([bidi]() { bidi->WritesDone(); });
    bidi->Connect();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  {
    auto bidi = cm.CreateBidi();
    bidi->SetOnConnect([bidi]() {});
    bidi->Connect();
  }
}

void test_client_unary() {
  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  gg::UnaryRequest req;

  // サーバを立てずにやると接続エラーになる
  {
    auto unary = cm.CreateUnary();
    req.set_value(100);
    unary->SetOnFinish([unary](gg::UnaryResponse resp, grpc::Status status) {
      ASSERT(status.error_code() == grpc::UNAVAILABLE);
      unary->Close();
    });
    unary->SetOnError(
        [unary](ggrpc::ClientResponseReaderError error) { ASSERT(false); });
    unary->Connect(req);
    unary.reset();
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  TestServer server;
  server.Start("localhost:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  {
    auto unary = cm.CreateUnary();
    req.set_value(100);
    unary->SetOnFinish([unary](gg::UnaryResponse resp, grpc::Status status) {
      ASSERT(status.ok());
      ASSERT(resp.value() == 10000);
      unary->Close();
    });
    unary->SetOnError(
        [unary](ggrpc::ClientResponseReaderError error) { ASSERT(false); });
    unary->Connect(req);
    unary.reset();
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  {
    auto unary = cm.CreateUnary();
    req.set_value(100);
    unary->SetOnFinish([unary](gg::UnaryResponse resp, grpc::Status status) {
      ASSERT(status.error_code() == grpc::CANCELLED);
      unary->Close();
    });
    unary->SetOnError(
        [unary](ggrpc::ClientResponseReaderError error) { ASSERT(false); });
    unary->Connect(req);
    // レスポンスには1秒程度掛かるので、500ミリ秒経って先に進むと
    // 各マネージャのデストラクタが呼ばれてリクエストがキャンセルされる。
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}

void test_client_sstream() {
  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  TestServer server;
  server.Start("localhost:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  gg::SstreamRequest req;

  {
    auto ss = cm.CreateSstream();
    req.set_value(100);
    std::atomic<int> n = 0;
    ss->SetOnRead([&n](gg::SstreamResponse resp) { n += resp.value(); });
    ss->Connect(req);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT(n == 300);
  }

  {
    auto ss = cm.CreateSstream();
    req.set_value(100);
    ss->SetOnRead([ss](gg::SstreamResponse resp) { ss->Close(); });
    ss->Connect(req);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}

void test_client_cstream() {
  TestServer server;
  server.Start("localhost:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  gg::CstreamRequest req;

  {
    auto cs = cm.CreateCstream();
    std::atomic<int> n = 0;
    cs->SetOnFinish(
        [&n](gg::CstreamResponse resp, grpc::Status) { n = resp.value(); });

    cs->Connect();
    req.set_value(1);
    cs->Write(req);
    req.set_value(2);
    cs->Write(req);
    req.set_value(4);
    cs->Write(req);
    cs->WritesDone();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT(n == 7);
  }

  {
    auto cs = cm.CreateCstream();
    std::atomic<int> n = 0;
    cs->SetOnFinish(
        [&n](gg::CstreamResponse resp, grpc::Status) { n = resp.value(); });

    cs->Connect();
    req.set_value(1);
    cs->Write(req);
    req.set_value(100);
    cs->Write(req);
    req.set_value(4);
    cs->Write(req);
    cs->Write(req);
    cs->Write(req);
    cs->Write(req);
    //cs->WritesDone();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT(n == 1);
  }
}

void test_server() {
  TestServer server;
  server.Start("localhost:50051", 1);
  std::this_thread::sleep_for(std::chrono::seconds(1));
}

void _test_alarm(std::shared_ptr<ggrpc::Alarm> alarm) {
  std::atomic<int> n = 0;
  std::atomic<int> m = 0;

  n = 0;
  alarm->Set(std::chrono::milliseconds(100), [&n](bool ok) { n = ok ? 1 : 2; });
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT(n == 1);

  n = 0;
  alarm->Set(std::chrono::milliseconds(100), [&n](bool ok) { n = ok ? 1 : 2; });
  alarm->Cancel();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT(n == 2);

  n = m = 0;
  alarm->Set(std::chrono::milliseconds(100), [&n](bool ok) { n = ok ? 1 : 2; });
  alarm->Set(std::chrono::milliseconds(100), [&m](bool ok) { m = ok ? 1 : 2; });
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT(n == 2 && m == 1);

  n = m = 0;
  alarm->Set(std::chrono::milliseconds(100), [&n](bool ok) { n = ok ? 1 : 2; });
  alarm->Set(std::chrono::milliseconds(100), [&m](bool ok) { m = ok ? 1 : 2; });
  alarm->Cancel();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT(n == 2 && m == 2);

  // ネスト系のテスト
  n = 0;
  alarm->Set(std::chrono::milliseconds(100), [&n, alarm](bool ok) {
    if (ok) {
      alarm->Set(std::chrono::milliseconds(100),
                 [&n](bool ok) { n = ok ? 1 : 2; });
    }
  });
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT(n == 1);

  n = m = 0;
  alarm->Set(std::chrono::milliseconds(100), [&n, &m, alarm](bool ok) {
    if (!ok) {
      alarm->Set(std::chrono::milliseconds(100),
                 [&n](bool ok) { n = ok ? 1 : 2; });
      alarm->Set(std::chrono::milliseconds(100),
                 [&m](bool ok) { m = ok ? 1 : 2; });
    }
  });
  alarm->Cancel();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT(n == 2 && m == 1);

  n = m = 0;
  alarm->Set(std::chrono::milliseconds(100), [&n, &m, alarm](bool ok) {
    if (!ok) {
      alarm->Set(std::chrono::milliseconds(100),
                 [&n](bool ok) { n = ok ? 1 : 2; });
      alarm->Set(std::chrono::milliseconds(100),
                 [&m](bool ok) { m = ok ? 1 : 2; });
      alarm->Cancel();
    }
  });
  alarm->Cancel();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT(n == 2 && m == 2);
}

void test_client_alarm() {
  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  auto alarm = cm.ClientManager()->CreateAlarm();
  _test_alarm(alarm);

  // シャットダウンのテスト
  std::atomic<int> n = 0;
  alarm->Set(std::chrono::milliseconds(100), [&n, alarm](bool ok) {
    n = ok ? 1 : 2;
    if (!ok) {
      bool b = alarm->Set(std::chrono::milliseconds(100), [](bool ok) {});
      ASSERT(b == false);
    }
  });
  cm.ClientManager()->Shutdown();
  ASSERT(n == 2);
  bool b = alarm->Set(std::chrono::milliseconds(100), [](bool ok) {});
  ASSERT(b == false);
}

void test_server_alarm() {
  TestServer server;
  server.Start("localhost:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  auto alarm = server.Server()->CreateAlarm();
  _test_alarm(alarm);

  // シャットダウンのテスト
  std::atomic<int> n = 0;
  alarm->Set(std::chrono::milliseconds(100), [&n, alarm](bool ok) {
    n = ok ? 1 : 2;
    if (!ok) {
      bool b = alarm->Set(std::chrono::milliseconds(100), [](bool ok) {});
      ASSERT(b == false);
    }
  });
  server.Server()->Shutdown();
  ASSERT(n == 2);
  bool b = alarm->Set(std::chrono::milliseconds(100), [](bool ok) {});
  ASSERT(b == false);
}

void test_client_generic() {
  TestServer server;
  server.Start("localhost:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  gg::UnaryRequest req;

  {
    grpc::ByteBuffer buf;

    auto unary = cm.CreateUnaryGeneric();
    req.set_value(100);

    bool own_buf;
    auto result = grpc::SerializationTraits<gg::UnaryRequest, void>::Serialize(
        req, &buf, &own_buf);
    ASSERT(result.ok());
    if (!own_buf) {
      buf.Duplicate();
    }

    unary->SetOnFinish([unary](grpc::ByteBuffer buf, grpc::Status) {
      gg::UnaryResponse resp;
      auto result =
          grpc::SerializationTraits<gg::UnaryResponse, void>::Deserialize(
              &buf, &resp);
      ASSERT(result.ok());
      ASSERT(resp.value() == 10000);
      unary->Close();
    });
    unary->Connect(std::move(buf));
    unary.reset();
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
}

void test_client_timeout() {
  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  TestServer server;
  server.Start("localhost:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  gg::UnaryRequest req;

  {
    auto unary = cm.CreateUnary(2000);
    req.set_value(100);

    unary->SetOnFinish([unary](gg::UnaryResponse resp, grpc::Status status) {
      ASSERT(status.ok());
      ASSERT(resp.value() == 10000);
      unary->Close();
    });
    unary->Connect(std::move(req));
    unary.reset();
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  {
    auto unary = cm.CreateUnary(100);
    req.set_value(100);

    unary->SetOnFinish([unary](gg::UnaryResponse resp, grpc::Status status) {
      ASSERT(status.error_code() == grpc::DEADLINE_EXCEEDED);
      unary->Close();
    });
    unary->Connect(std::move(req));
    unary.reset();
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
}

void test_client_notify() {
  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  TestServer server;
  server.Start("localhost:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  gg::UnaryRequest req;

  {
    auto unary = cm.CreateUnary();
    req.set_value(100);

    std::atomic<int> n = 0;
    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(100);
    auto on_notify = [&n](grpc::Channel* channel, bool ok,
                          std::chrono::system_clock::time_point& deadline,
                          bool& repeated) {
      bool expired = std::chrono::system_clock::now() > deadline;
      SPDLOG_TRACE("OnStateChange: state={}, expired={}",
                   (int)channel->GetState(false), expired);
      if (!expired) {
        repeated = true;
        return;
      }
      n += 1;
    };
    cm.ClientManager()->NotifyOnStateChange(channel.get(), deadline, on_notify);
    deadline += std::chrono::milliseconds(100);
    cm.ClientManager()->NotifyOnStateChange(channel.get(), deadline, on_notify);
    deadline += std::chrono::milliseconds(100);
    cm.ClientManager()->NotifyOnStateChange(channel.get(), deadline, on_notify);

    unary->SetOnFinish([unary](gg::UnaryResponse resp, grpc::Status status) {});
    unary->Connect(std::move(req));
    unary.reset();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    ASSERT(n == 3);
  }

  {
    auto unary = cm.CreateUnary();
    req.set_value(100);

    auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(500);
    auto on_notify = [unary](grpc::Channel* channel, bool ok,
                             std::chrono::system_clock::time_point& deadline,
                             bool& repeated) {
      bool expired = std::chrono::system_clock::now() > deadline;
      SPDLOG_TRACE("OnStateChange: state={}, expired={}",
                   (int)channel->GetState(false), expired);
      if (!expired) {
        repeated = true;
        return;
      }
      unary->Close();
    };
    cm.ClientManager()->NotifyOnStateChange(channel.get(), deadline, on_notify);
    unary->SetOnFinish([unary](gg::UnaryResponse resp, grpc::Status status) {
      ASSERT(false);
    });
    unary->Connect(std::move(req));
    unary.reset();
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
}

void test_client_cancel() {
  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  TestServer server;
  server.Start("localhost:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  std::atomic<int> n = 0;

  {
    gg::UnaryRequest req;
    n = 0;
    auto unary = cm.CreateUnary();

    unary->SetOnFinish([unary](gg::UnaryResponse resp, grpc::Status status) {
      ASSERT(false);
    });
    unary->SetOnError([&n, unary](ggrpc::ClientResponseReaderError error) {
      ASSERT(error == ggrpc::ClientResponseReaderError::CANCEL);
      n = 1;
    });
    unary->Connect(std::move(req));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    unary->Cancel();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT(n == 1);
  }

  {
    gg::SstreamRequest req;
    n = 0;
    auto ss = cm.CreateSstream();

    ss->SetOnRead([](gg::SstreamResponse resp) { ASSERT(false); });
    ss->SetOnFinish([](grpc::Status status) { ASSERT(false); });
    ss->SetOnError([&n, ss](ggrpc::ClientReaderError error) {
      ASSERT(error == ggrpc::ClientReaderError::CONNECT_CANCEL);
      n = 1;
    });
    ss->Connect(std::move(req));
    ss->Cancel();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT(n == 1);

    n = 0;
    ss = cm.CreateSstream();

    ss->SetOnRead([](gg::SstreamResponse resp) { ASSERT(false); });
    ss->SetOnFinish([](grpc::Status status) { ASSERT(false); });
    ss->SetOnError([&n, ss](ggrpc::ClientReaderError error) {
      ASSERT(error == ggrpc::ClientReaderError::READ_CANCEL);
      n = 1;
    });
    ss->Connect(std::move(req));
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ss->Cancel();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT(n == 1);
  }

  {
    gg::CstreamRequest req;
    n = 0;
    auto cs = cm.CreateCstream();

    cs->SetOnFinish(
        [](gg::CstreamResponse resp, grpc::Status status) { ASSERT(false); });
    cs->SetOnError([&n, cs](ggrpc::ClientWriterError error) {
      ASSERT(error == ggrpc::ClientWriterError::CONNECT_CANCEL);
      n = 1;
    });
    cs->Connect();
    cs->Cancel();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT(n == 1);
  }

  {
    n = 0;
    auto ss = cm.CreateBidi();
    gg::BidiRequest req;

    ss->SetOnRead([](gg::BidiResponse resp) { ASSERT(false); });
    ss->SetOnFinish([](grpc::Status status) { ASSERT(false); });
    ss->SetOnError([&n, ss](ggrpc::ClientReaderWriterError error) {
      ASSERT(error == ggrpc::ClientReaderWriterError::CONNECT_CANCEL);
      n = 1;
    });
    ss->Connect();
    ss->Cancel();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    ASSERT(n == 1);
  }
}

void test_metadata() {
  auto channel = grpc::CreateChannel("localhost:50051",
                                     grpc::InsecureChannelCredentials());
  TestClientManager cm(channel, 10);
  cm.Start();

  TestServer server;
  server.Start("localhost:50051", 10);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  {
    gg::UnaryRequest req;
    auto unary = cm.CreateUnary(0, {{"hoge", "fuga"}, {"foo", "bar"}});

    unary->SetOnFinish([unary](gg::UnaryResponse resp, grpc::Status status) {
      const auto& meta = unary->GetGrpcContext()->GetServerInitialMetadata();
      SPDLOG_INFO("metasize={}", meta.size());
      for (const auto& p : meta) {
        SPDLOG_INFO("{}={}", std::string(p.first.begin(), p.first.end()),
                    std::string(p.second.begin(), p.second.end()));
      }
      ASSERT(meta.size() == 3);
      ASSERT(meta.find("hoge") != meta.end());
      ASSERT(meta.find("hoge")->second == "fuga");
      ASSERT(meta.find("foo") != meta.end());
      ASSERT(meta.find("foo")->second == "bar");
      // user-agent=grpc-c++/1.33.1 grpc-c/13.0.0 (osx; chttp2)
      ASSERT(meta.find("user-agent") != meta.end());
    });
    unary->Connect(std::move(req));
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  }
}

int main() {
  spdlog::set_level(spdlog::level::trace);

  test_client_bidi_connect_callback();
  test_client_unary();
  test_client_sstream();
  test_client_cstream();
  test_server();
  test_client_alarm();
  test_server_alarm();
  test_client_generic();
  test_client_timeout();
  test_client_notify();
  test_client_cancel();
  test_metadata();
}
