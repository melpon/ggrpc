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
#include <grpcpp/alarm.h>
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
class ClientReaderWriterListener {
 public:
  virtual ~ClientReaderWriterListener() {}
  virtual std::unique_ptr<grpc::ClientAsyncReaderWriter<W, R>> AsyncConnect(
      grpc::ClientContext* client, grpc::CompletionQueue* cq, void* tag) = 0;
  virtual void OnConnect() = 0;
  virtual void OnRead(R response) = 0;
  virtual void OnDone(grpc::Status status) = 0;
  virtual void OnError(ClientReaderWriterError error) = 0;
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

  ClientReaderWriterListener* listener_;
  std::function<void()> unregister_;

  bool in_error_ = false;

 public:
  ClientReaderWriterImpl(ClientReaderWriterListener* listener)
      : listener_(listener),
        connector_thunk_(this),
        reader_thunk_(this),
        writer_thunk_(this) {
  }
  ~ClientReaderWriterImpl() { SPDLOG_TRACE("deleted: {}", (void*)this); }

  // コピー、ムーブ禁止
  ClientReaderWriterImpl(const ClientReaderWriterImpl&) = delete;
  ClientReaderWriterImpl(ClientReaderWriterImpl&&) = delete;
  ClientReaderWriterImpl& operator=(const ClientReaderWriterImpl&) = delete;
  ClientReaderWriterImpl& operator=(ClientReaderWriterImpl&&) = delete;

  void InitInternal(grpc::CompletionQueue* cq,
                    std::function<void()> unregister) {
    std::lock_guard<std::mutex> guard(mutex_);
    unregister_ = std::move(unregister);
    streamer_ = listener_->AsyncConnect(&context_, cq, &connector_thunk_);
  }

  void Shutdown() {
    unregister_();

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
      listener_->OnError(error);
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

    lock.unlock();
    try {
      listener_->OnConnected();
      lock.lock();
    } catch (...) {
      lock.lock();
    }

    // 終了要求が来てたので読み込みをせずに終了
    if (shutdown_) {
      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
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
        listener_->OnRead(std::move(response_));
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
        listener_->OnDone(std::move(grpc_status_));
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
class ClientReaderWriter : public ClientReaderWriterListener<W, R> {
  ClientReaderWriterImpl<W, R>* impl_;

 public:
  ClientReaderWriter() { impl_ = new ClientReaderWriterImpl<W, R>(this); }
  virtual ~ClientReaderWriter() { Shutdown(); }

  // コピー、ムーブ禁止
  ClientReaderWriter(const ClientReaderWriter&) = delete;
  ClientReaderWriter(ClientReaderWriter&&) = delete;
  ClientReaderWriter& operator=(const ClientReaderWriter&) = delete;
  ClientReaderWriter& operator=(ClientReaderWriter&&) = delete;

  void Shutdown() { impl_->Shutdown(); }
  void Write(W request) { impl_->Write(std::move(request)); }
  void WritesDone() { impl_->WritesDone(); }
};

enum ClientResponseReaderError {
  FINISH,
};

template <class R>
class ClientResponseReaderListener {
 public:
  virtual ~ClientResponseReaderListener() {}
  virtual std::function <
      std::unique_ptr<grpc::ClientAsyncResponseReader<R>> AsyncRequest(
          grpc::ClientContext* client) = 0;
  virtual void OnDone(R response, grpc::Status status) = 0;
  virtual void OnError(ClientResponseReaderError error) = 0;
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
  bool done_ = false;
  std::mutex mutex_;

  ClientResponseReaderListener* listener_;

 public:
  ClientResponseReaderImpl(ClientResponseReaderListener* listener)
      : listener_(listener), reader_thunk_(this) {
    std::lock_guard<std::mutex> guard(mutex_);
    reader_ = listener_->AsyncRequest(&context_);
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

    if (done_) {
      p.reset(this);
    } else {
      // キャンセルされるまで待つ
      context_.TryCancel();
    }
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
        listener_->OnError(ClientResponseReaderError::FINISH);
        lock.lock();
      } catch (...) {
        lock.lock();
      }

      done_ = true;
      if (shutdown_) {
        p.reset(this);
      }
      return;
    }

    // 結果が取得できた

    lock.unlock();
    try {
      listener_->OnDone(std::move(response_), std::move(grpc_status_));
      lock.lock();
    } catch (...) {
      lock.lock();
    }

    done_ = true;
    if (shutdown_) {
      p.reset(this);
    }
  }
};

template <class R>
class ClientResponseReader : public ClientResponseReaderListener {
  ClientResponseReaderImpl<R>* impl_;

 public:
  ClientResponseReader() { impl_ = new ClientResponseReaderImpl<R>(this); }
  ~ClientResponseReader() override { Shutdown(); }

  // コピー、ムーブ禁止
  ClientResponseReader(const ClientResponseReader&) = delete;
  ClientResponseReader(ClientResponseReader&&) = delete;
  ClientResponseReader& operator=(const ClientResponseReader&) = delete;
  ClientResponseReader& operator=(ClientResponseReader&&) = delete;

  void Shutdown() { impl_->Shutdown(); }
};

class ClientAlarmImpl {
  std::mutex mutex_;

  struct NotifierThunk : Handler {
    ClientAlarmImpl* p;
    NotifierThunk(ClientAlarmImpl* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToNotify(ok); }
  };
  NotifierThunk notifier_thunk_;
  friend class NotifierThunk;

  grpc::CompletionQueue* cq_;
  grpc::Alarm alarm_;
  std::function<void()> on_done_;
  bool shutdown_ = false;
  bool notified_ = false;

 public:
  ClientAlarmImpl(grpc::CompletionQueue* cq, std::chrono::milliseconds ms,
                  std::function<void()> f)
      : notifier_thunk_(this), cq_(cq) {
    std::lock_guard<std::mutex> guard(mutex_);
    on_done_ = std::move(f);
    gpr_timespec ts;
    ts.clock_type = GPR_TIMESPAN;
    ts.tv_sec = ms.count() / 1000;
    ts.tv_nsec = (ms.count() % 1000) * 1000000;
    alarm_.Set(cq_, ts, &notifier_thunk_);
  }
  ~ClientAlarmImpl() { SPDLOG_TRACE("deleted: {}", (void*)this); }

  // コピー、ムーブ禁止
  ClientAlarmImpl(const ClientAlarmImpl&) = delete;
  ClientAlarmImpl(ClientAlarmImpl&&) = delete;
  ClientAlarmImpl& operator=(const ClientAlarmImpl&) = delete;
  ClientAlarmImpl& operator=(ClientAlarmImpl&&) = delete;

  void Shutdown() {
    std::unique_ptr<ClientAlarmImpl> p;

    std::lock_guard<std::mutex> guard(mutex_);
    if (shutdown_) {
      return;
    }
    alarm_.Cancel();
    shutdown_ = true;
    if (notified_ && shutdown_) {
      p.reset(this);
    }
  }

 private:
  void ProceedToNotify(bool ok) {
    std::unique_ptr<ClientAlarmImpl> p;

    std::unique_lock<std::mutex> lock(mutex_);
    SPDLOG_TRACE("ProceedToNotify: this={}, ok={}", (void*)this, ok);

    if (!ok || shutdown_) {
      notified_ = true;

      // キャンセルされた
      if (shutdown_ && notified_) {
        p.reset(this);
      }
      return;
    }

    // ロック中に on_done_ を取得しておく
    auto f = std::move(on_done_);
    lock.unlock();
    try {
      f();
      lock.lock();
    } catch (...) {
      lock.lock();
    }

    notified_ = true;

    // コールバックが終わって Shutdown が呼ばれてたら終了
    if (shutdown_ && notified_) {
      p.reset(this);
    }
  }
};

class ClientAlarm {
  ClientAlarmImpl* impl_;

 public:
  template <class... Args>
  ClientAlarm(Args... args)
      : impl_(new ClientAlarmImpl(std::forward<Args>(args)...)) {}
  ~ClientAlarm() { Shutdown(); }

  ClientAlarm(const ClientAlarm&) = delete;
  ClientAlarm(ClientAlarm&&) = delete;
  ClientAlarm& operator=(const ClientAlarm&) = delete;
  ClientAlarm& operator=(ClientAlarm&&) = delete;

  void Shutdown() { impl_->Shutdown(); }
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
  void Shutdown();
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

  template <class T>
  struct HolderImpl : Holder {
    T* p;

    HolderImpl(T* p) : p(p) {}
    void Shutdown() override { p->Shutdown(); }
  };

  uint32_t next_client_id_ = 0;
  std::map<uint32_t, std::unique_ptr<Holder>> clients_;
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

    // 全て Shutdown する
    for (auto&& pair : clients_) {
      pair.second->Shutdown();
    }
    clients_.clear();

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

  template <class T, class... Args>
  std::unique_ptr<T> CreateReaderWriterClient(Args... args) {
    std::lock_guard<std::mutex> guard(mutex_);

    auto client_id = next_client_id_++;

    std::unique_ptr<T> p(new T(std::forward<Args>(args)...));
    std::unique_ptr<Holder> holder(new HolderImpl<T>(p.get()));
    clients_.insert(std::make_pair(client_id, std::move(holder)));

    auto cq = &threads_[client_id % threads_.size()].cq;
    p->impl_->InitInternal(cq, [this, client_id]() {
      std::lock_guard<std::mutex> guard(mutex_);
      clients_.erase(client_id);
    });

    return p;
  }

  template <class W, class R>
  ClientReaderWriter<W, R> CreateReaderWriterClient(
      std::function<std::unique_ptr<grpc::ClientAsyncReaderWriter<W, R>>(
          grpc::ClientContext*, grpc::CompletionQueue*, void*)>
          async_connect,
      std::function<void()> on_connected, std::function<void(R)> on_read,
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
        std::move(async_connect2), std::move(on_connected), std::move(on_read),
        std::move(on_done), std::move(on_error)));
    writable_clients_.insert(std::make_pair(client_id, std::move(holder)));
    return std::unique_ptr<WritableClient<W>>(
        new WritableClient<W>(client_id, this));
  }

  template <class W, class R>
  std::unique_ptr<Client> CreateClient(
      std::function<std::unique_ptr<grpc::ClientAsyncResponseReader<R>>(
          grpc::ClientContext*, const W&, grpc::CompletionQueue*)>
          async_request,
      const W& request, std::function<void(R, grpc::Status)> on_done,
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

  std::unique_ptr<Client> CreateAlarm(std::chrono::milliseconds ms,
                                      std::function<void()> f) {
    std::lock_guard<std::mutex> guard(mutex_);

    auto client_id = next_client_id_++;
    auto cq = &threads_[client_id % threads_.size()].cq;

    std::unique_ptr<Holder> holder(new AlarmHolderImpl(cq, ms, std::move(f)));
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
template <class W>
void WritableClient<W>::Shutdown() {
  cm_->DestroyWritableClient(client_id_);
}

}  // namespace ggrpc

#endif  // GGRPC_CLIENT_H_INCLUDED
