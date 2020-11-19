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
  SayHelloClient::ConnectFunc connect = [stub = stub.get()](
                                            grpc::ClientContext* context,
                                            const helloworld::HelloRequest& req,
                                            grpc::CompletionQueue* cq) {
    return stub->AsyncSayHello(context, req, cq);
  };
  std::shared_ptr<SayHelloClient> client =
      cm.CreateResponseReader<helloworld::HelloRequest,
                              helloworld::HelloResponse>(connect);

  // レスポンスが返ってきた時の処理
  client->SetOnFinish([](helloworld::HelloResponse resp, grpc::Status status) {
    std::cout << resp.message() << std::endl;
  });

  // リクエスト送信
  helloworld::HelloRequest req;
  req.set_name("melpon");
  client->Connect(req);

  std::this_thread::sleep_for(std::chrono::seconds(1));

  // 送信直後、レスポンスを受け取る前にクライアントマネージャを Shutdown しても大丈夫
  // （cm 経由で作った全てのクライアントに対して client->Close() する）
  req.set_name("melponnn");
  client->Connect(req);
  cm.Shutdown();
}
```

サーバ側:

```cpp
#include <ggrpc/ggrpc.h>

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
  TIMEOUT,
};

template <class W, class R>
class ClientResponseReader {
 public:
  typedef std::function<std::unique_ptr<grpc::ClientAsyncResponseReader<R>>(
      grpc::ClientContext*, const W&, grpc::CompletionQueue*)>
      ConnectFunc;
  typedef std::function<void(R, grpc::Status)> OnFinishFunc;
  typedef std::function<void(ClientResponseWriterError)> OnErrorFunc;

  grpc::ClientContext* GetGrpcContext() { return &context_; }
  const grpc::ClientContext* GetGrpcContext() const { return &context_; }

  void SetOnFinish(OnFinishFunc on_finish);
  void SetOnError(OnErrorFunc on_error);
  void SetTimeout(std::chrono::milliseconds timeout);

  void Connect(const W& request);

  void Close();
};

// サーバストリーミング用
enum class ClientReaderError {
  CONNECT,
  READ,
};

template <class W, class R>
class ClientReader {
 public:
  typedef std::function<std::unique_ptr<grpc::ClientAsyncReader<R>>(
      grpc::ClientContext*, const W&, grpc::CompletionQueue*, void*)>
      ConnectFunc;
  typedef std::function<void()> OnConnectFunc;
  typedef std::function<void(R)> OnReadFunc;
  typedef std::function<void(grpc::Status)> OnFinishFunc;
  typedef std::function<void(ClientReaderError)> OnErrorFunc;

  grpc::ClientContext* GetGrpcContext() { return &context_; }
  const grpc::ClientContext* GetGrpcContext() const { return &context_; }

  void SetOnConnect(OnConnectFunc on_connect);
  void SetOnRead(OnReadFunc on_read);
  void SetOnFinish(OnFinishFunc on_finish);
  void SetOnError(OnErrorFunc on_error);

  void Connect(const W& request);

  void Close();
};

// クライアントストリーミング用
enum class ClientWriterError {
  CONNECT,
  READ,
  WRITE,
};

template <class W, class R>
class ClientWriter {
 public:
  typedef std::function<std::unique_ptr<grpc::ClientAsyncWriter<W>>(
      grpc::ClientContext*, R*, grpc::CompletionQueue*, void*)>
      ConnectFunc;
  typedef std::function<void()> OnConnectFunc;
  typedef std::function<void(R, grpc::Status)> OnFinishFunc;
  typedef std::function<void(ClientWriterError)> OnErrorFunc;
  typedef std::function<void(W, int64_t)> OnWriteFunc;
  typedef std::function<void()> OnWritesDoneFunc;

  grpc::ClientContext* GetGrpcContext() { return &context_; }
  const grpc::ClientContext* GetGrpcContext() const { return &context_; }

  void SetOnConnect(OnConnectFunc on_connect);
  void SetOnFinish(OnFinishFunc on_finish);
  void SetOnWrite(OnWriteFunc on_write);
  void SetOnWritesDone(OnWritesDoneFunc on_writes_done);
  void SetOnError(OnErrorFunc on_error);

  void Connect();

  void Close();

  void Write(W request, int64_t id = 0);
  void WritesDone();
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
  typedef std::function<void(grpc::Status)> OnFinishFunc;
  typedef std::function<void(ClientReaderWriterError)> OnErrorFunc;
  typedef std::function<void(W, int64_t)> OnWriteFunc;
  typedef std::function<void()> OnWritesDoneFunc;

  grpc::ClientContext* GetGrpcContext() { return &context_; }
  const grpc::ClientContext* GetGrpcContext() const { return &context_; }

  void SetOnConnect(OnConnectFunc on_connect);
  void SetOnRead(OnReadFunc on_read);
  void SetOnFinish(OnFinishFunc on_finish);
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
  std::shared_ptr<ClientReader<W, R>> CreateReader(
      typename ClientReader<W, R>::ConnectFunc connect);

  template <class W, class R>
  std::shared_ptr<ClientWriter<W, R>> CreateWriter(
      typename ClientWriter<W, R>::ConnectFunc connect);

  template <class W, class R>
  std::shared_ptr<ClientReaderWriter<W, R>> CreateReaderWriter(
      typename ClientReaderWriter<W, R>::ConnectFunc connect);

  std::shared_ptr<Alarm> CreateAlarm();

  template <class T>
  using OnStateChangeFunc =
      std::function<void(grpc::Channel*, std::chrono::system_clock::time_point,
                         std::shared_ptr<T>, bool&)>;

  template <class T>
  void NotifyOnStateChange(grpc::Channel* channel,
                           std::chrono::system_clock::time_point deadline,
                           std::shared_ptr<T> target,
                           OnStateChangeFunc<T> on_notify);
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
  grpc::ServerContext* GetGrpcContext();
  const grpc::ServerContext* GetGrpcContext() const;

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
  grpc::ServerContext* GetGrpcContext();
  const grpc::ServerContext* GetGrpcContext() const;

  std::shared_ptr<ServerWriterContext<W, R>> Context() const;

  virtual void Request(grpc::ServerContext* context, R* request,
                       grpc::ServerAsyncWriter<W>* writer,
                       grpc::ServerCompletionQueue* cq, void* tag) = 0;
  virtual void OnAccept(R request) {}
  virtual void OnWrite(W response, int64_t id) {}
  virtual void OnFinish(grpc::Status status) {}
  virtual void OnError(ServerWriterError error) {}
};

// クライアントストリーミング用
enum class ServerReaderError {
  WRITE,
};

template <class W, class R>
class ServerReaderContext {
 public:
  Server* GetServer() const;
  void Finish(W resp, grpc::Status status);
  void FinishWithError(grpc::Status status);
  void Close();
};

template <class W, class R>
class ServerReaderHandler {
 public:
  grpc::ServerContext* GetGrpcContext();
  const grpc::ServerContext* GetGrpcContext() const;

  std::shared_ptr<ServerReaderContext<W, R>> Context() const;

  virtual void Request(grpc::ServerContext* context,
                       grpc::ServerAsyncReader<W, R>* streamer,
                       grpc::ServerCompletionQueue* cq, void* tag) = 0;
  virtual void OnAccept() {}
  virtual void OnRead(R req) {}
  virtual void OnReadDoneOrError() {}
  virtual void OnFinish(W response, grpc::Status status) {}
  virtual void OnError(ServerReaderError error) {}
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
  grpc::ServerContext* GetGrpcContext();
  const grpc::ServerContext* GetGrpcContext() const;

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
  template <class T, class... Args>
  void AddResponseWriterHandler(Args... args);

  template <class T, class... Args>
  void AddWriterHandler(Args... args);

  template <class T, class... Args>
  void AddReaderHandler(Args... args);

  template <class T, class... Args>
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
- spdlog 依存を無くす
- pubsub 的なサンプルを書く
