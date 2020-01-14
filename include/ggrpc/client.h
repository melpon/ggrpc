#ifndef GGRPC_CLIENT_H_INCLUDED
#define GGRPC_CLIENT_H_INCLUDED

// グレースフルシャットダウンやマルチスレッドに対応した、
// 安全に利用できる gRPC クライアント

#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <thread>

// gRPC
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>

// spdlog
#include <spdlog/spdlog.h>

#include "handler.h"

namespace ggrpc {

enum ClientReaderWriterError {
  CONNECT,
  READ,
  WRITE,
};

template <class W, class R>
class ClientReaderWriterImpl {
  struct ConnectorThunk : Handler {
    ClientReaderWriterImpl* p;
    ConnectorThunk(ClientReaderWriterImpl* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToConnect(ok); }
  };
  ConnectorThunk connector_thunk_;
  friend class ConnectorThunk;

  struct ReaderThunk : Handler {
    ClientReaderWriterImpl* p;
    ReaderThunk(ClientReaderWriterImpl* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToRead(ok); }
  };
  ReaderThunk reader_thunk_;
  friend class ReaderThunk;

  struct WriterThunk : Handler {
    ClientReaderWriterImpl* p;
    WriterThunk(ClientReaderWriterImpl* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToWrite(ok); }
  };
  WriterThunk writer_thunk_;
  friend class WriterThunk;

  // ClientAsyncReaderWriter よりも ClientContext の方が寿命が長くなるようにしないといけないので、
  // 必ず streamer_ より上に context_ を定義すること
  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientAsyncReaderWriter<W, R>> streamer_;

  enum class ReadStatus { CONNECTING, READING, FINISHING, FINISHED };
  ReadStatus read_status_ = ReadStatus::CONNECTING;
  enum class WriteStatus { CONNECTING, WRITING, IDLE, FINISHING, FINISHED };
  WriteStatus write_status_ = WriteStatus::CONNECTING;

  std::deque<std::shared_ptr<void>> request_queue_;
  R response_;

  grpc::Status grpc_status_;

  bool shutdown_ = false;
  std::mutex mutex_;

  std::function<void(R)> on_read_;
  std::function<void(grpc::Status)> on_done_;
  std::function<void(ClientReaderWriterError)> on_error_;

  bool in_error_ = false;

 public:
  ClientReaderWriterImpl(
      std::function<std::unique_ptr<grpc::ClientAsyncReaderWriter<W, R>>(
          grpc::ClientContext*,
          void*)> async_connect,
      std::function<void(R)> on_read,
      std::function<void(grpc::Status)> on_done,
      std::function<void(ClientReaderWriterError)> on_error)
      : connector_thunk_(this),
        reader_thunk_(this),
        writer_thunk_(this),
        on_read_(std::move(on_read)),
        on_done_(std::move(on_done)),
        on_error_(std::move(on_error)) {
    std::lock_guard<std::mutex> guard(mutex_);
    streamer_ = async_connect(&context_, &connector_thunk_);
  }
  ~ClientReaderWriterImpl() { SPDLOG_DEBUG("deleted: {}", (void*)this); }

  // コピー、ムーブ禁止
  ClientReaderWriterImpl(const ClientReaderWriterImpl&) = delete;
  ClientReaderWriterImpl(ClientReaderWriterImpl&&) = delete;
  ClientReaderWriterImpl& operator=(const ClientReaderWriterImpl&) = delete;
  ClientReaderWriterImpl& operator=(ClientReaderWriterImpl&&) = delete;

  void Shutdown() {
    std::unique_ptr<ClientReaderWriterImpl> p;
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_) {
      return;
    }

    shutdown_ = true;

    // 読み書き中だったらキャンセルされるまで待つ
    if (read_status_ == ReadStatus::CONNECTING ||
        read_status_ == ReadStatus::READING ||
        read_status_ == ReadStatus::FINISHING ||
        write_status_ == WriteStatus::CONNECTING ||
        write_status_ == WriteStatus::WRITING ||
        write_status_ == WriteStatus::FINISHING) {
      context_.TryCancel();
    } else {
      // そうでないなら即座に終わらせて良い
      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
    }
    if (Deletable()) {
      p.reset(this);
    }
  }

  void Write(W request) {
    std::lock_guard<std::mutex> guard(mutex_);
    SPDLOG_TRACE("Write: {}", (void*)this);

    if (shutdown_) {
      return;
    }

    if (write_status_ == WriteStatus::IDLE) {
      streamer_->Write(request, &writer_thunk_);
      write_status_ = WriteStatus::WRITING;
    } else if (write_status_ == WriteStatus::WRITING ||
               write_status_ == WriteStatus::CONNECTING) {
      request_queue_.push_back(
          std::shared_ptr<void>(new W(std::move(request))));
    }
  }

  void WritesDone() {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_) {
      return;
    }

    if (write_status_ == WriteStatus::IDLE) {
      streamer_->WritesDone(&writer_thunk_);
      write_status_ = WriteStatus::FINISHING;
    } else if (write_status_ == WriteStatus::WRITING) {
      request_queue_.push_back(nullptr);
    }
  }

 private:
  void OnError(std::unique_lock<std::mutex>& lock,
               ClientReaderWriterError error) {
    in_error_ = true;
    lock.unlock();
    try {
      on_error_(error);
      lock.lock();
      in_error_ = false;
    } catch (...) {
      lock.lock();
      in_error_ = false;
    }
  }

  void ProceedToConnect(bool ok) {
    std::unique_ptr<ClientReaderWriterImpl> p;
    std::unique_lock<std::mutex> lock(mutex_);
    SPDLOG_TRACE("ProceedToConnect: {}", (void*)this);

    if (shutdown_) {
      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("connection error");

      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
      OnError(lock, ClientReaderWriterError::CONNECT);
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    // 読み込み
    streamer_->Read(&response_, &reader_thunk_);
    read_status_ = ReadStatus::READING;

    HandleRequestQueue();
  }

  void ProceedToRead(bool ok) {
    std::unique_ptr<ClientReaderWriterImpl> p;
    std::unique_lock<std::mutex> lock(mutex_);
    SPDLOG_TRACE("ProceedToRead: {}", (void*)this);

    if (shutdown_) {
      read_status_ = ReadStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    if (!ok) {
      if (read_status_ == ReadStatus::READING) {
        // 正常に finish した可能性があるので Finish する
        streamer_->Finish(&grpc_status_, &reader_thunk_);
        read_status_ = ReadStatus::FINISHING;
      } else if (read_status_ == ReadStatus::FINISHING) {
        SPDLOG_ERROR("reading or finishing error");
        // 書き込み中ならキャンセルする
        if (write_status_ == WriteStatus::CONNECTING ||
            write_status_ == WriteStatus::WRITING ||
            write_status_ == WriteStatus::FINISHING) {
          context_.TryCancel();
        } else {
          write_status_ = WriteStatus::FINISHED;
        }
        read_status_ = ReadStatus::FINISHED;
        OnError(lock, ClientReaderWriterError::READ);
        if (Deletable()) {
          p.reset(this);
        }
      }
      return;
    }

    if (read_status_ == ReadStatus::READING) {
      // 結果が取得できた

      // 普通にコールバックするとデッドロックの可能性があるので
      // unlock してからコールバックする。
      // 再度ロックした時に状態が変わってる可能性があるので注意すること。
      lock.unlock();
      try {
        on_read_(std::move(response_));
        lock.lock();
      } catch (...) {
        lock.lock();
      }

      // 終了要求が来てたので次の読み込みをせずに終了
      if (shutdown_) {
        read_status_ = ReadStatus::FINISHED;
        if (Deletable()) {
          p.reset(this);
        }
        return;
      }

      // 次の読み込み
      streamer_->Read(&response_, &reader_thunk_);
      read_status_ = ReadStatus::READING;
    } else if (read_status_ == ReadStatus::FINISHING) {
      // 終了

      // 普通にコールバックするとデッドロックの可能性があるので
      // unlock してからコールバックする。
      // 再度ロックした時に状態が変わってる可能性があるので注意すること。
      lock.unlock();
      try {
        on_done_(std::move(grpc_status_));
        lock.lock();
      } catch (...) {
        lock.lock();
      }

      if (grpc_status_.ok()) {
        SPDLOG_DEBUG("gRPC Read finished");
      } else {
        SPDLOG_ERROR("gRPC error: {} ({})", grpc_status_.error_message(),
                     grpc_status_.error_code());
        SPDLOG_ERROR("   details: {}", grpc_status_.error_details());
      }
      // 正常終了
      read_status_ = ReadStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
    }
  }

  void ProceedToWrite(bool ok) {
    std::unique_ptr<ClientReaderWriterImpl> p;
    std::unique_lock<std::mutex> lock(mutex_);
    SPDLOG_TRACE("ProceedToWrite: {}", (void*)this);

    if (shutdown_) {
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    if (!ok) {
      // 読み込み中ならキャンセルする
      if (read_status_ == ReadStatus::CONNECTING ||
          read_status_ == ReadStatus::READING ||
          read_status_ == ReadStatus::FINISHING) {
        context_.TryCancel();
      } else {
        read_status_ = ReadStatus::FINISHED;
      }
      write_status_ = WriteStatus::FINISHED;
      OnError(lock, ClientReaderWriterError::WRITE);
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    if (write_status_ == WriteStatus::FINISHING) {
      // 書き込み完了。
      // あとは読み込みが全て終了したら終わり。
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    // 書き込みが成功したら次のキューを処理する
    HandleRequestQueue();
  }

  void HandleRequestQueue() {
    if (request_queue_.empty()) {
      write_status_ = WriteStatus::IDLE;
    } else {
      auto ptr = request_queue_.front();
      request_queue_.pop_front();
      if (ptr != nullptr) {
        // 通常の書き込みリクエスト
        streamer_->Write(*std::static_pointer_cast<W>(ptr), &writer_thunk_);
        write_status_ = WriteStatus::WRITING;
      } else {
        // 完了のリクエスト
        streamer_->WritesDone(&writer_thunk_);
        write_status_ = WriteStatus::FINISHING;
      }
    }
  }

  bool Deletable() const {
    return shutdown_ && !in_error_ && read_status_ == ReadStatus::FINISHED &&
           write_status_ == WriteStatus::FINISHED;
  }
};

template <class W, class R>
class ClientReaderWriter {
  ClientReaderWriterImpl<W, R>* p_;

 public:
  template <class... Args>
  ClientReaderWriter(Args... args)
      : p_(new ClientReaderWriterImpl<W, R>(std::forward<Args>(args)...)) {}

  ~ClientReaderWriter() { Shutdown(); }

  // コピー、ムーブ禁止
  ClientReaderWriter(const ClientReaderWriter&) = delete;
  ClientReaderWriter(ClientReaderWriter&&) = delete;
  ClientReaderWriter& operator=(const ClientReaderWriter&) = delete;
  ClientReaderWriter& operator=(ClientReaderWriter&&) = delete;

  void Shutdown() { p_->Shutdown(); }
  void Write(W request) { p_->Write(std::move(request)); }

  void WritesDone() { p_->WritesDone(); }
};

enum ClientResponseReaderError {
  FINISH,
};

template <class R>
class ClientResponseReaderImpl {
  struct ReaderThunk : Handler {
    ClientResponseReaderImpl* p;
    ReaderThunk(ClientResponseReaderImpl* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToRead(ok); }
  };
  ReaderThunk reader_thunk_;
  friend class ReaderThunk;

  // ClientAsyncResponseReader よりも ClientContext の方が寿命が長くなるようにしないといけないので、
  // 必ず reader_ より上に context_ を定義すること
  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientAsyncResponseReader<R>> reader_;

  R response_;
  grpc::Status grpc_status_;

  bool shutdown_ = false;
  std::mutex mutex_;

  std::function<void(R, grpc::Status)> on_done_;
  std::function<void(ClientResponseReaderError)> on_error_;

 public:
  ClientResponseReaderImpl(
      std::function<std::unique_ptr<grpc::ClientAsyncResponseReader<R>>(
          grpc::ClientContext*)> async_request,
      std::function<void(R, grpc::Status)> on_done,
      std::function<void(ClientResponseReaderError)> on_error)
      : reader_thunk_(this),
        on_done_(std::move(on_done)),
        on_error_(std::move(on_error)) {
    std::lock_guard<std::mutex> guard(mutex_);
    reader_ = async_request(&context_);
    reader_->Finish(&response_, &grpc_status_, &reader_thunk_);
  }
  ~ClientResponseReaderImpl() { SPDLOG_DEBUG("deleted: {}", (void*)this); }

  // コピー、ムーブ禁止
  ClientResponseReaderImpl(const ClientResponseReaderImpl&) = delete;
  ClientResponseReaderImpl(ClientResponseReaderImpl&&) = delete;
  ClientResponseReaderImpl& operator=(const ClientResponseReaderImpl&) = delete;
  ClientResponseReaderImpl& operator=(ClientResponseReaderImpl&&) = delete;

  void Shutdown() {
    std::unique_ptr<ClientResponseReaderImpl> p;
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_) {
      return;
    }

    shutdown_ = true;

    // キャンセルされるまで待つ
    context_.TryCancel();
  }

 private:
  void ProceedToRead(bool ok) {
    std::unique_ptr<ClientResponseReaderImpl> p;
    std::unique_lock<std::mutex> lock(mutex_);
    SPDLOG_TRACE("ProceedToRead: {}", (void*)this);

    if (shutdown_) {
      SPDLOG_INFO("shutdown ClientResponseReaderImpl");
      p.reset(this);
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("finishing error");
      lock.unlock();
      try {
        on_error_(ClientResponseReaderError::FINISH);
        lock.lock();
      } catch (...) {
        lock.lock();
      }

      p.reset(this);
      return;
    }

    // 結果が取得できた

    lock.unlock();
    try {
      on_done_(std::move(response_), std::move(grpc_status_));
      lock.lock();
    } catch (...) {
      lock.lock();
    }

    p.reset(this);
  }
};

template <class R>
class ClientResponseReader {
  ClientResponseReaderImpl<R>* p_;

 public:
  template <class... Args>
  ClientResponseReader(Args... args)
      : p_(new ClientResponseReaderImpl<R>(std::forward<Args>(args)...)) {}

  ~ClientResponseReader() { Shutdown(); }

  // コピー、ムーブ禁止
  ClientResponseReader(const ClientResponseReader&) = delete;
  ClientResponseReader(ClientResponseReader&&) = delete;
  ClientResponseReader& operator=(const ClientResponseReader&) = delete;
  ClientResponseReader& operator=(ClientResponseReader&&) = delete;

  void Shutdown() { p_->Shutdown(); }
};

class ClientManager;

class Client {
  uint32_t client_id_;
  ClientManager* cm_;

 public:
  Client(uint32_t client_id, ClientManager* cm)
      : client_id_(client_id), cm_(cm) {}
  ~Client();
};

template <class W>
class WritableClient {
  uint32_t client_id_;
  ClientManager* cm_;

 public:
  WritableClient(uint32_t client_id, ClientManager* cm)
      : client_id_(client_id), cm_(cm) {}
  ~WritableClient();

  void Write(W data);
  void WritesDone();
};

class ClientManager {
  std::mutex mutex_;

  struct ThreadData {
    grpc::CompletionQueue cq;
    std::unique_ptr<std::thread> thread;
  };
  std::vector<ThreadData> threads_;

  struct Holder {
    virtual ~Holder() {}
    virtual void Shutdown() = 0;
  };

  template <class R>
  struct ResponseReaderHolderImpl : Holder {
    ClientResponseReader<R> v;

    template <class... Args>
    ResponseReaderHolderImpl(Args... args) : v(std::forward<Args>(args)...) {}
    void Shutdown() override { v.Shutdown(); }
  };

  // 全ての ClientReaderWriter<W, R> を同じ変数に入れたいので
  // type erasure イディオムを使う
  struct WritableHolder : Holder {
    virtual ~WritableHolder() {}
    virtual const std::type_info& WriterTypeInfo() const = 0;
    virtual void Write(void* p) = 0;
    virtual void WritesDone() = 0;
    virtual void Shutdown() = 0;
  };

  template <class W, class R>
  struct ReaderWriterHolderImpl : WritableHolder {
    ClientReaderWriter<W, R> v;

    template <class... Args>
    ReaderWriterHolderImpl(Args... args) : v(std::forward<Args>(args)...) {}

    const std::type_info& WriterTypeInfo() const override { return typeid(W); }
    void Write(void* p) override { v.Write(std::move(*(W*)p)); }
    void WritesDone() override { v.WritesDone(); }
    void Shutdown() override { v.Shutdown(); }
  };

  uint32_t next_client_id_ = 0;
  std::map<uint32_t, std::unique_ptr<Holder>> clients_;
  std::map<uint32_t, std::unique_ptr<WritableHolder>> writable_clients_;
  bool shutdown_ = false;

 public:
  ClientManager(int threads) : threads_(threads) {
    for (auto& th : threads_) {
      th.thread.reset(new std::thread([& cq = th.cq]() {
        void* got_tag;
        bool ok = false;

        while (cq.Next(&got_tag, &ok)) {
          Handler* call = static_cast<Handler*>(got_tag);
          call->Proceed(ok);
        }
      }));
    }
  }

  ~ClientManager() { Shutdown(); }

  void Shutdown() {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_) return;
    SPDLOG_TRACE("ClientManager::Shutdown started");

    shutdown_ = true;

    // これで各接続の ClientReaderWriter::Shutdown が呼ばれる。
    clients_.clear();
    writable_clients_.clear();

    // キューを Shutdown して、全てのスレッドが終了するのを待つ
    // コールバックの処理で無限ループしてるとかじゃない限りは終了するはず
    for (auto& th : threads_) {
      th.cq.Shutdown();
    }

    SPDLOG_TRACE("ClientManager::Shutdown released completion queue");

    for (auto& th : threads_) {
      th.thread->join();
    }

    SPDLOG_TRACE("ClientManager::Shutdown finished");
  }

  template <class W, class R>
  std::unique_ptr<WritableClient<W>> CreateReaderWriterClient(
      std::function<std::unique_ptr<grpc::ClientAsyncReaderWriter<W, R>>(
          grpc::ClientContext*,
          grpc::CompletionQueue*,
          void*)> async_connect,
      std::function<void(R)> on_read,
      std::function<void(grpc::Status)> on_done,
      std::function<void(ClientReaderWriterError)> on_error) {
    std::lock_guard<std::mutex> guard(mutex_);

    auto client_id = next_client_id_++;

    // cq を bind して引数を減らす
    auto cq = &threads_[client_id % threads_.size()].cq;
    auto async_connect2 = [f = std::move(async_connect), cq](
                              grpc::ClientContext* client, void* tag) {
      return std::move(f)(client, cq, tag);
    };

    std::unique_ptr<WritableHolder> holder(new ReaderWriterHolderImpl<W, R>(
        std::move(async_connect2), std::move(on_read), std::move(on_done),
        std::move(on_error)));
    writable_clients_.insert(std::make_pair(client_id, std::move(holder)));
    return std::unique_ptr<WritableClient<W>>(
        new WritableClient<W>(client_id, this));
  }

  template <class W, class R>
  std::unique_ptr<Client> CreateClient(
      std::function<std::unique_ptr<grpc::ClientAsyncResponseReader<R>>(
          grpc::ClientContext*,
          const W&,
          grpc::CompletionQueue*)> async_request,
      const W& request,
      std::function<void(R, grpc::Status)> on_done,
      std::function<void(ClientResponseReaderError)> on_error) {
    std::lock_guard<std::mutex> guard(mutex_);

    auto client_id = next_client_id_++;

    // cq と request を bind して引数を減らす
    auto cq = &threads_[client_id % threads_.size()].cq;
    auto async_request2 = [f = std::move(async_request), &request,
                           cq](grpc::ClientContext* client) {
      return std::move(f)(client, request, cq);
    };

    std::unique_ptr<Holder> holder(new ResponseReaderHolderImpl<R>(
        std::move(async_request2), std::move(on_done), std::move(on_error)));
    clients_.insert(std::make_pair(client_id, std::move(holder)));
    return std::unique_ptr<Client>(new Client(client_id, this));
  }

 private:
  WritableHolder* GetWritableClient(uint32_t client_id) {
    if (shutdown_) {
      return nullptr;
    }

    auto it = writable_clients_.find(client_id);
    if (it == writable_clients_.end()) {
      return nullptr;
    }

    return it->second.get();
  }

  // 以下は Client からのみアクセスさせる
  friend class Client;

  void DestroyClient(uint32_t client_id) {
    std::lock_guard<std::mutex> guard(mutex_);
    clients_.erase(client_id);
  }

  // 以下は WritableClient<W> からのみアクセスさせる
  template <class W>
  friend class WritableClient;

  void DestroyWritableClient(uint32_t client_id) {
    std::lock_guard<std::mutex> guard(mutex_);
    writable_clients_.erase(client_id);
  }

  template <class W>
  void Write(uint32_t client_id, W req) {
    std::lock_guard<std::mutex> guard(mutex_);

    auto client = GetWritableClient(client_id);
    if (client == nullptr) {
      return;
    }
    // 書き込む型が一致してない
    if (client->WriterTypeInfo() != typeid(W)) {
      return;
    }

    client->Write(&req);
  }
  void WritesDone(uint32_t client_id) {
    std::lock_guard<std::mutex> guard(mutex_);

    auto client = GetWritableClient(client_id);
    if (client == nullptr) {
      return;
    }

    client->WritesDone();
  }
};

inline Client::~Client() { cm_->DestroyClient(client_id_); }

template <class W>
WritableClient<W>::~WritableClient() {
  cm_->DestroyWritableClient(client_id_);
}
template <class W>
void WritableClient<W>::Write(W data) {
  cm_->Write(client_id_, std::move(data));
}
template <class W>
void WritableClient<W>::WritesDone() {
  cm_->WritesDone(client_id_);
}

}  // namespace ggrpc

#endif  // GGRPC_CLIENT_H_INCLUDED
