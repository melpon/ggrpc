#include <chrono>

// ggrpc
#include <ggrpc/ggrpc.h>

#include "helloworld.grpc.pb.h"

using SayHelloClient = ggrpc::ClientResponseReader<helloworld::HelloRequest,
                                                   helloworld::HelloResponse>;

int main() {
  // スレッド数1でクライアントマネージャを作る
  ggrpc::ClientManager cm(1);
  cm.Start();

  // 接続先の stub を作る
  std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials());
  std::unique_ptr<helloworld::Greeter::Stub> stub(
      helloworld::Greeter::NewStub(channel));

  // SayHello リクエストを送るクライアントを作る
  SayHelloClient::RequestFunc request = [stub = stub.get()](
                                            grpc::ClientContext* context,
                                            const helloworld::HelloRequest& req,
                                            grpc::CompletionQueue* cq) {
    return stub->AsyncSayHello(context, req, cq);
  };
  std::shared_ptr<SayHelloClient> client =
      cm.CreateResponseReader<helloworld::HelloRequest,
                              helloworld::HelloResponse>(request);

  // レスポンスが返ってきた時の処理
  client->SetOnResponse(
      [](helloworld::HelloResponse resp, grpc::Status status) {
        std::cout << resp.message() << std::endl;
      });

  // リクエスト送信
  helloworld::HelloRequest req;
  req.set_name("melpon");
  client->Request(req);

  std::this_thread::sleep_for(std::chrono::seconds(1));

  // 送信直後、レスポンスを受け取る前にクライアントマネージャを Shutdown しても大丈夫
  // （cm 経由で作った全てのクライアントに対して client->Close() する）
  req.set_name("melponnn");
  client->Request(req);
  cm.Shutdown();
}
