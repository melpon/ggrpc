# ggrpc

ggrpc は、マルチスレッドで安全に動作し、グレースフルシャットダウンを可能にした gRPC C++ ライブラリです。

## 使い方

proto ファイル:

```proto
syntax = "proto3";

package helloworld;

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloResponse) {}
}

message HelloRequest {
  string name = 1;
}

message HelloResponse {
  string message = 1;
}
```

クライアント側:

```cpp
#include <chrono>

// ggrpc
#include <ggrpc/client.h>

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
```

サーバ側:

```cpp
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

  // スレッド数1でサーバを起動して待つ
  server.Start(builder, 1);
  server.Wait();
}
```

実行:

```
$ ./helloworld-server

$ ./helloworld-client
Hello, melpon
```

## ルール

- `ClientManager` が死んだら、その `ClientManager` で生成した接続は全て安全に切断される
- `Server` が死んだら、その `Server` に紐付けられていたハンドラは全て安全に切断される
- 各クライアントは、そのオブジェクトが参照されている限り安全に読み書きできる
- 各ハンドラは、その `Context()` が参照されている限り安全に読み書きできる
- 強制的に接続を閉じたい場合は `Close()` 関数を呼ぶ
- たとえ `ClientManager` や `Server` のライフタイムが終了していたとしても、クライアントやハンドラのオブジェクトが参照されている限りはアクセスが可能。ただしデータは闇に捨てられていく
- `Close()` 関数を呼んだ後もクライアントやハンドラのオブジェクトが参照されている限りはアクセスが可能。ただしデータは闇に捨てられていく

## シグネチャ

クライアント側:

```cpp
namespace ggrpc {

// 単体リクエスト用
enum class ClientResponseWriterError {
  FINISH,
};

template <class W, class R>
class ClientResponseReader {
 public:
  typedef std::function<std::unique_ptr<grpc::ClientAsyncResponseReader<R>>(
      grpc::ClientContext*, const W&, grpc::CompletionQueue*)>
      RequestFunc;
  typedef std::function<void(R, grpc::Status)> OnResponseFunc;
  typedef std::function<void(ClientResponseWriterError)> OnErrorFunc;

  void SetOnResponse(OnResponseFunc on_response);
  void SetOnError(OnErrorFunc on_error);

  void Request(const W& request);

  void Close();
};

// 双方向ストリーミング用
enum class ClientReaderWriterError {
  CONNECT,
  READ,
  WRITE,
};

template <class W, class R>
class ClientReaderWriter {
 public:
  typedef std::function<std::unique_ptr<grpc::ClientAsyncReaderWriter<W, R>>(
      grpc::ClientContext*, grpc::CompletionQueue*, void*)>
      ConnectFunc;
  typedef std::function<void()> OnConnectFunc;
  typedef std::function<void(R)> OnReadFunc;
  typedef std::function<void(grpc::Status)> OnReadDoneFunc;
  typedef std::function<void(ClientReaderWriterError)> OnErrorFunc;
  typedef std::function<void(W, int64_t)> OnWriteFunc;
  typedef std::function<void()> OnWritesDoneFunc;

  void SetOnConnect(OnConnectFunc on_connect);
  void SetOnRead(OnReadFunc on_read);
  void SetOnReadDone(OnReadDoneFunc on_read_done);
  void SetOnWrite(OnWriteFunc on_write);
  void SetOnWritesDone(OnWritesDoneFunc on_writes_done);
  void SetOnError(OnErrorFunc on_error);

  void Connect();

  void Close();

  void Write(W request, int64_t id = 0);
  void WritesDone();
};

class ClientManager {
public:
  ClientManager(int threads);

  void Start();
  void Shutdown();

  template <class W, class R>
  std::shared_ptr<ClientResponseReader<W, R>> CreateResponseReader(
      typename ClientResponseReader<W, R>::RequestFunc request);

  template <class W, class R>
  std::shared_ptr<ClientReaderWriter<W, R>> CreateReaderWriter(
      typename ClientReaderWriter<W, R>::ConnectFunc connect);

  std::shared_ptr<Alarm> CreateAlarm();
};

}
```

サーバ側:

```cpp
namespace ggrpc {

// 単体リクエスト用
enum class ServerResponseWriterError {
  WRITE,
};

template <class W, class R>
class ServerResponseWriterContext {
 public:
  Server* GetServer() const;
  void Finish(W resp, grpc::Status status);
  void FinishWithError(grpc::Status status);
  void Close();
};

template <class W, class R>
class ServerResponseWriterHandler {
 public:
  std::shared_ptr<ServerResponseWriterContext<W, R>> Context() const;

  virtual void Request(grpc::ServerContext* context, R* request,
                       grpc::ServerAsyncResponseWriter<W>* response_writer,
                       grpc::ServerCompletionQueue* cq, void* tag) = 0;
  virtual void OnAccept(R request) {}
  virtual void OnFinish(W response, grpc::Status status) {}
  virtual void OnError(ServerResponseWriterError error) {}
};

// サーバストリーミング用
enum class ServerWriterError {
  WRITE,
};

template <class W, class R>
class ServerWriterContext {
 public:
  Server* GetServer() const;
  void Write(W resp, int64_t id = 0);
  void Finish(grpc::Status status);
  void Close();
};

template <class W, class R>
class ServerWriterHandler {
 public:
  std::shared_ptr<ServerWriterContext<W, R>> Context() const;

  virtual void Request(grpc::ServerContext* context, R* request,
                       grpc::ServerAsyncWriter<W>* writer,
                       grpc::ServerCompletionQueue* cq, void* tag) = 0;
  virtual void OnAccept(R request) {}
  virtual void OnWrite(W response, int64_t id) {}
  virtual void OnFinish(grpc::Status status) {}
  virtual void OnError(ServerWriterError error) {}
};

// 双方向ストリーミング用
enum class ServerReaderWriterError {
  WRITE,
};

template <class W, class R>
class ServerReaderWriterContext {
 public:
  Server* GetServer() const;
  void Write(W resp, int64_t id);
  void Finish(grpc::Status status);
  void Close();
};

template <class W, class R>
class ServerReaderWriterHandler {
 public:
  std::shared_ptr<ServerReaderWriterContext<W, R>> Context() const;

  virtual void Request(grpc::ServerContext* context,
                       grpc::ServerAsyncReaderWriter<W, R>* streamer,
                       grpc::ServerCompletionQueue* cq, void* tag) = 0;
  virtual void OnAccept() {}
  virtual void OnRead(R req) {}
  virtual void OnReadDoneOrError() {}
  virtual void OnWrite(W response, int64_t id) {}
  virtual void OnFinish(grpc::Status status) {}
  virtual void OnError(ServerReaderWriterError error) {}
};

class Server {
 public:
  template <class H, class... Args>
  void AddResponseWriterHandler(Args... args);

  template <class H, class... Args>
  void AddReaderWriterHandler(Args... args);

  void Start(grpc::ServerBuilder& builder, int threads);
  void Wait();
  void Shutdown();

  std::shared_ptr<Alarm> CreateAlarm();
};

}
```

共通:

```cpp
namespace ggrpc {

class Alarm {
 public:
  typedef std::function<void(bool)> AlarmFunc;

  bool Set(std::chrono::milliseconds deadline, AlarmFunc alarm);

  void Cancel();
  void Close();
};

}
```

## TODO

- ドキュメント書く
- クライアントストリーミングを実装する
- spdlog 依存を無くす
- pubsub 的なサンプルを書く
