#include <ggrpc/server.h>

#include "helloworld.grpc.pb.h"

class SayHelloHandler
    : public ggrpc::ServerResponseWriterHandler<helloworld::HelloResponse,
                                                helloworld::HelloRequest> {
  helloworld::Greeter::AsyncService* service_;

 public:
  SayHelloHandler(helloworld::Greeter::AsyncService* service)
      : service_(service) {}

  // サーバ開始時にインスタンスが作られて Request() が呼ばれる。
  // Request() では接続待ち状態にする処理を書くこと。
  void Request(
      grpc::ServerContext* context, helloworld::HelloRequest* request,
      grpc::ServerAsyncResponseWriter<helloworld::HelloResponse>* writer,
      grpc::ServerCompletionQueue* cq, void* tag) override {
    service_->RequestSayHello(context, request, writer, cq, cq, tag);
  }
  // 接続が確立すると OnAccept() が呼ばれる。
  void OnAccept(helloworld::HelloRequest req) override {
    helloworld::HelloResponse resp;
    resp.set_message("Hello, " + req.name());
    Context()->Finish(resp, grpc::Status::OK);
  }
  void OnError(ggrpc::ServerResponseWriterError error) override {}
};

int main() {
  ggrpc::Server server;
  helloworld::Greeter::AsyncService service;

  grpc::ServerBuilder builder;
  builder.AddListeningPort("127.0.0.1:50051",
                           grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  // リクエストハンドラの登録
  server.AddResponseWriterHandler<SayHelloHandler>(&service);

  // サーバを起動して待つ
  server.Start(builder, 1);
  server.Wait();
}
