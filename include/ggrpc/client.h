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

class ClientManager;

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

 private:
  struct ReaderThunk : Handler {
    ClientResponseReader* p;
    ReaderThunk(ClientResponseReader* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToRead(ok); }
  };
  ReaderThunk reader_thunk_;
  friend class ReaderThunk;

  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientAsyncResponseReader<R>> reader_;

  R response_;
  grpc::Status grpc_status_;

  enum class Status { INIT, REQUESTING, CANCELING, DONE };
  Status status_ = Status::INIT;
  bool release_ = false;
  int nesting_ = 0;
  std::mutex mutex_;

  grpc::CompletionQueue* cq_;

  RequestFunc request_;
  OnResponseFunc on_response_;
  OnErrorFunc on_error_;

  struct SafeDeleter {
    ClientResponseReader* p;
    std::unique_lock<std::mutex> lock;
    SafeDeleter(ClientResponseReader* p) : p(p), lock(p->mutex_) {}
    ~SafeDeleter() {
      bool del = p->release_ &&
                 p->status_ == ClientResponseReader::Status::DONE &&
                 p->nesting_ == 0;
      lock.unlock();
      if (del) {
        delete p;
      }
    }
  };
  friend struct SafeDeleter;

  ClientResponseReader(grpc::CompletionQueue* cq, RequestFunc request)
      : reader_thunk_(this), cq_(cq), request_(std::move(request)) {}
  ~ClientResponseReader() { SPDLOG_TRACE("[0x{}] deleted", (void*)this); }

  // コピー、ムーブ禁止
  ClientResponseReader(const ClientResponseReader&) = delete;
  ClientResponseReader(ClientResponseReader&&) = delete;
  ClientResponseReader& operator=(const ClientResponseReader&) = delete;
  ClientResponseReader& operator=(ClientResponseReader&&) = delete;

 public:
  void SetOnResponse(OnResponseFunc on_response) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (status_ == Status::DONE) {
      return;
    }
    on_response_ = std::move(on_response);
  }
  void SetOnError(OnErrorFunc on_error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (status_ != Status::INIT) {
      return;
    }
    on_error_ = std::move(on_error);
  }

  void Request(const W& request) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (status_ != Status::INIT) {
      return;
    }
    status_ = Status::REQUESTING;

    reader_ = request_(&context_, request, cq_);
    reader_->Finish(&response_, &grpc_status_, &reader_thunk_);
  }

 private:
  void Release() {
    SafeDeleter d(this);

    release_ = true;

    DoClose(d.lock);
  }
  friend class ClientManager;

 public:
  void Close() {
    SafeDeleter d(this);

    DoClose(d.lock);
  }

 private:
  void DoClose(std::unique_lock<std::mutex>& lock) {
    if (status_ == Status::REQUESTING) {
      context_.TryCancel();
      status_ = Status::CANCELING;
      return;
    }

    if (status_ == Status::CANCELING) {
      return;
    }

    status_ = Status::DONE;
    Done(lock);
  }

  void Done(std::unique_lock<std::mutex>& lock) {
    if (status_ != Status::DONE) {
      return;
    }

    auto on_response = std::move(on_response_);
    auto on_error = std::move(on_error_);

    ++nesting_;
    lock.unlock();
    on_response = nullptr;
    on_error = nullptr;
    lock.lock();
    --nesting_;
  }

 private:
  template <class F, class... Args>
  void RunCallback(std::unique_lock<std::mutex>& lock, std::string funcname,
                   F f, Args&&... args) {
    // 普通にコールバックするとデッドロックの可能性があるので
    // unlock してからコールバックする。
    // 再度ロックした時に状態が変わってる可能性があるので注意すること。
    if (f) {
      ++nesting_;
      lock.unlock();
      try {
        f(std::forward<Args>(args)...);
      } catch (std::exception& e) {
        SPDLOG_ERROR("{} error: what={}", funcname, e.what());
      } catch (...) {
        SPDLOG_ERROR("{} error", funcname);
      }
      f = nullptr;
      lock.lock();
      --nesting_;
    }
  }

  void ProceedToRead(bool ok) {
    SafeDeleter d(this);
    SPDLOG_TRACE("[0x{}] ProceedToRead: ok={} status={} grpc_status={}",
                 (void*)this, ok, (int)status_, grpc_status_.error_message());

    assert(status_ == Status::REQUESTING || status_ == Status::CANCELING);

    auto st = status_;
    status_ = Status::DONE;

    if (!ok) {
      SPDLOG_ERROR("finishing error");
      if (st == Status::REQUESTING) {
        RunCallback(d.lock, "OnError", on_error_,
                    ClientResponseWriterError::FINISH);
      }
      Done(d.lock);
      return;
    }

    // 結果が取得できた
    if (st == Status::REQUESTING) {
      RunCallback(d.lock, "OnResponse", on_response_, std::move(response_),
                  std::move(grpc_status_));
    }
    Done(d.lock);
  }
};

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

 private:
  struct ConnectorThunk : Handler {
    ClientReaderWriter* p;
    ConnectorThunk(ClientReaderWriter* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToConnect(ok); }
  };
  ConnectorThunk connector_thunk_;
  friend class ConnectorThunk;

  struct ReaderThunk : Handler {
    ClientReaderWriter* p;
    ReaderThunk(ClientReaderWriter* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToRead(ok); }
  };
  ReaderThunk reader_thunk_;
  friend class ReaderThunk;

  struct WriterThunk : Handler {
    ClientReaderWriter* p;
    WriterThunk(ClientReaderWriter* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToWrite(ok); }
  };
  WriterThunk writer_thunk_;
  friend class WriterThunk;

  // ClientAsyncReaderWriter よりも ClientContext
  // の方が寿命が長くなるようにしないといけないので、 必ず streamer_ より上に
  // context_ を定義すること
  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientAsyncReaderWriter<W, R>> streamer_;

  enum class ReadStatus {
    INIT,
    CONNECTING,
    READING,
    FINISHING,
    CANCELING,
    FINISHED
  };
  ReadStatus read_status_ = ReadStatus::INIT;
  enum class WriteStatus {
    INIT,
    CONNECTING,
    WRITING,
    IDLE,
    FINISHING,
    CANCELING,
    FINISHED
  };
  WriteStatus write_status_ = WriteStatus::INIT;

  struct RequestData {
    bool is_done;
    int64_t id;
    W request;
  };
  std::deque<RequestData> request_queue_;
  R response_;

  grpc::Status grpc_status_;

  bool release_ = false;
  int nesting_ = 0;
  std::mutex mutex_;

  grpc::CompletionQueue* cq_;
  ConnectFunc connect_;
  OnConnectFunc on_connect_;
  OnReadFunc on_read_;
  OnReadDoneFunc on_read_done_;
  OnErrorFunc on_error_;
  OnWriteFunc on_write_;
  OnWritesDoneFunc on_writes_done_;

  struct SafeDeleter {
    ClientReaderWriter* p;
    std::unique_lock<std::mutex> lock;
    SafeDeleter(ClientReaderWriter* p) : p(p), lock(p->mutex_) {}
    ~SafeDeleter() {
      bool del =
          p->release_ &&
          p->read_status_ == ClientReaderWriter::ReadStatus::FINISHED &&
          p->write_status_ == ClientReaderWriter::WriteStatus::FINISHED &&
          p->nesting_ == 0;
      lock.unlock();
      if (del) {
        delete p;
      }
    }
  };
  friend struct SafeDeleter;

  ClientReaderWriter(grpc::CompletionQueue* cq, ConnectFunc connect)
      : connector_thunk_(this),
        reader_thunk_(this),
        writer_thunk_(this),
        cq_(cq),
        connect_(std::move(connect)) {}

  ~ClientReaderWriter() { SPDLOG_TRACE("[0x{}] deleted", (void*)this); }

  // コピー、ムーブ禁止
  ClientReaderWriter(const ClientReaderWriter&) = delete;
  ClientReaderWriter(ClientReaderWriter&&) = delete;
  ClientReaderWriter& operator=(const ClientReaderWriter&) = delete;
  ClientReaderWriter& operator=(ClientReaderWriter&&) = delete;

 public:
  void SetOnConnect(OnConnectFunc on_connect) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_connect_ = std::move(on_connect);
  }
  void SetOnRead(OnReadFunc on_read) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_read_ = std::move(on_read);
  }
  void SetOnReadDone(OnReadDoneFunc on_read_done) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_read_done_ = std::move(on_read_done);
  }
  void SetOnWrite(OnWriteFunc on_write) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_write_ = std::move(on_write_);
  }
  void SetOnWritesDone(OnWritesDoneFunc on_writes_done) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_writes_done_ = std::move(on_writes_done);
  }
  void SetOnError(OnErrorFunc on_error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_error_ = std::move(on_error_);
  }

  void Connect() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    read_status_ = ReadStatus::CONNECTING;
    write_status_ = WriteStatus::CONNECTING;
    streamer_ = connect_(&context_, cq_, &connector_thunk_);
  }

 private:
  void Release() {
    SafeDeleter d(this);

    release_ = true;

    DoClose(d.lock);
  }
  friend class ClientManager;

 public:
  void Close() {
    SafeDeleter d(this);

    DoClose(d.lock);
  }

 private:
  void DoClose(std::unique_lock<std::mutex>& lock) {
    // 読み書き中だったらキャンセルされるまで待つ
    if (read_status_ == ReadStatus::CONNECTING ||
        read_status_ == ReadStatus::READING ||
        read_status_ == ReadStatus::FINISHING ||
        write_status_ == WriteStatus::CONNECTING ||
        write_status_ == WriteStatus::WRITING ||
        write_status_ == WriteStatus::FINISHING) {
      context_.TryCancel();
    }
    if (read_status_ == ReadStatus::CONNECTING ||
        read_status_ == ReadStatus::READING ||
        read_status_ == ReadStatus::FINISHING) {
      read_status_ = ReadStatus::CANCELING;
    } else {
      read_status_ = ReadStatus::FINISHED;
    }
    if (write_status_ == WriteStatus::CONNECTING ||
        write_status_ == WriteStatus::WRITING ||
        write_status_ == WriteStatus::FINISHING) {
      write_status_ = WriteStatus::CANCELING;
    } else {
      write_status_ = WriteStatus::FINISHED;
    }
    Done(lock);
  }

  void Done(std::unique_lock<std::mutex>& lock) {
    if (read_status_ != ReadStatus::FINISHED ||
        write_status_ != WriteStatus::FINISHED) {
      return;
    }

    auto on_connect = std::move(on_connect_);
    auto on_read = std::move(on_read_);
    auto on_read_done = std::move(on_read_done_);
    auto on_writes_done = std::move(on_writes_done_);
    auto on_error = std::move(on_error_);

    ++nesting_;
    lock.unlock();
    on_connect = nullptr;
    on_read = nullptr;
    on_read_done = nullptr;
    on_write_ = nullptr;
    on_writes_done = nullptr;
    on_error = nullptr;
    lock.lock();
    --nesting_;
  }

 public:
  void Write(W request, int64_t id = 0) {
    std::lock_guard<std::mutex> guard(mutex_);
    SPDLOG_TRACE("[0x{}] Write: request={}", (void*)this,
                 request.DebugString());

    if (write_status_ == WriteStatus::IDLE) {
      streamer_->Write(request, &writer_thunk_);
      write_status_ = WriteStatus::WRITING;
    } else if (write_status_ == WriteStatus::INIT ||
               write_status_ == WriteStatus::CONNECTING ||
               write_status_ == WriteStatus::WRITING) {
      RequestData req;
      req.is_done = false;
      req.id = id;
      req.request = std::move(request);
      request_queue_.push_back(std::move(req));
    }
  }

  void WritesDone() {
    std::lock_guard<std::mutex> guard(mutex_);

    if (write_status_ == WriteStatus::IDLE) {
      streamer_->WritesDone(&writer_thunk_);
      write_status_ = WriteStatus::FINISHING;
    } else if (write_status_ == WriteStatus::INIT ||
               write_status_ == WriteStatus::CONNECTING ||
               write_status_ == WriteStatus::WRITING) {
      RequestData req;
      req.is_done = true;
      request_queue_.push_back(std::move(req));
    }
  }

 private:
  template <class F, class... Args>
  void RunCallback(std::unique_lock<std::mutex>& lock, std::string funcname,
                   F f, Args&&... args) {
    // 普通にコールバックするとデッドロックの可能性があるので
    // unlock してからコールバックする。
    // 再度ロックした時に状態が変わってる可能性があるので注意すること。
    if (f) {
      ++nesting_;
      lock.unlock();
      try {
        f(std::forward<Args>(args)...);
      } catch (std::exception& e) {
        SPDLOG_ERROR("{} error: what={}", funcname, e.what());
      } catch (...) {
        SPDLOG_ERROR("{} error", funcname);
      }
      f = nullptr;
      lock.lock();
      --nesting_;
    }
  }

  void ProceedToConnect(bool ok) {
    SafeDeleter d(this);
    SPDLOG_TRACE("[0x{}] ProceedToConnect: ok={}", (void*)this, ok);

    // read/write 両方が CONNECTING か CANCELING になることしか無いはず
    assert(read_status_ == ReadStatus::CONNECTING &&
               write_status_ == WriteStatus::CONNECTING ||
           read_status_ == ReadStatus::CANCELING &&
               write_status_ == WriteStatus::CANCELING);

    if (read_status_ != ReadStatus::CONNECTING ||
        write_status_ != WriteStatus::CONNECTING) {
      // 既に Close が呼ばれてるので終わる
      Done(d.lock);
      return;
    }

    // 接続失敗
    if (!ok) {
      SPDLOG_ERROR("connection error");

      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnError", on_error_,
                  ClientReaderWriterError::CONNECT);
      Done(d.lock);
      return;
    }

    // 読み込み
    streamer_->Read(&response_, &reader_thunk_);
    read_status_ = ReadStatus::READING;

    HandleRequestQueue();

    RunCallback(d.lock, "OnConnect", on_connect_);
  }

  void ProceedToRead(bool ok) {
    SafeDeleter d(this);
    SPDLOG_TRACE("[0x{}] ProceedToRead: ok={}", (void*)this, ok);

    assert(read_status_ == ReadStatus::READING ||
           read_status_ == ReadStatus::FINISHING ||
           read_status_ == ReadStatus::CANCELING);

    if (read_status_ == ReadStatus::CANCELING) {
      read_status_ = ReadStatus::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      if (read_status_ == ReadStatus::READING) {
        // 正常に読み込み完了した可能性があるので Finish する
        streamer_->Finish(&grpc_status_, &reader_thunk_);
        read_status_ = ReadStatus::FINISHING;
      } else if (read_status_ == ReadStatus::FINISHING) {
        SPDLOG_ERROR("reading or finishing error");
        read_status_ = ReadStatus::FINISHED;
        RunCallback(d.lock, "OnError", on_error_,
                    ClientReaderWriterError::READ);
        Done(d.lock);
      }
      return;
    }

    if (read_status_ == ReadStatus::READING) {
      // 結果が取得できた
      auto resp = std::move(response_);

      // 次の読み込み
      streamer_->Read(&response_, &reader_thunk_);
      read_status_ = ReadStatus::READING;

      // 読み込み成功コールバック
      RunCallback(d.lock, "OnRead", on_read_, std::move(resp));
    } else if (read_status_ == ReadStatus::FINISHING) {
      // 終了

      if (grpc_status_.ok()) {
        SPDLOG_DEBUG("gRPC Read finished");
      } else {
        SPDLOG_ERROR("gRPC error: {} ({})", grpc_status_.error_message(),
                     grpc_status_.error_code());
        SPDLOG_ERROR("   details: {}", grpc_status_.error_details());
      }
      // 正常終了
      read_status_ = ReadStatus::FINISHED;
      RunCallback(d.lock, "OnReadDone", on_read_done_, grpc_status_);
      Done(d.lock);
    }
  }

  void ProceedToWrite(bool ok) {
    SafeDeleter d(this);
    SPDLOG_TRACE("[0x{}] ProceedToWrite: ok={}", (void*)this, ok);

    assert(write_status_ == WriteStatus::WRITING ||
           write_status_ == WriteStatus::FINISHING ||
           write_status_ == WriteStatus::CANCELING);

    if (write_status_ == WriteStatus::CANCELING) {
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnError", on_error_, ClientReaderWriterError::WRITE);
      Done(d.lock);
      return;
    }

    if (write_status_ == WriteStatus::FINISHING) {
      // 書き込み完了。
      // あとは読み込みが全て終了したら終わり。
      request_queue_.pop_front();
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnWritesDone", on_writes_done_);
      Done(d.lock);
      return;
    }

    auto req = std::move(request_queue_.front());
    request_queue_.pop_front();

    // 書き込みが成功したら次のキューを処理する
    HandleRequestQueue();

    RunCallback(d.lock, "OnWrite", on_write_, std::move(req.request), req.id);
  }

  void HandleRequestQueue() {
    if (request_queue_.empty()) {
      write_status_ = WriteStatus::IDLE;
    } else {
      auto& req = request_queue_.front();
      if (!req.is_done) {
        // 通常の書き込みリクエスト
        streamer_->Write(req.request, &writer_thunk_);
        write_status_ = WriteStatus::WRITING;
      } else {
        // 完了のリクエスト
        streamer_->WritesDone(&writer_thunk_);
        write_status_ = WriteStatus::FINISHING;
      }
    }
  }
};

class ClientManager {
  std::mutex mutex_;

  struct ThreadData {
    grpc::CompletionQueue cq;
    std::unique_ptr<std::thread> thread;
  };
  std::vector<ThreadData> threads_;

  uint32_t next_client_id_ = 0;
  bool shutdown_ = false;

  struct Holder {
    virtual ~Holder() {}
    virtual void Close() = 0;
    virtual bool Expired() = 0;
  };
  template <class W, class R>
  struct ResponseReaderHolder : Holder {
    std::weak_ptr<ClientResponseReader<W, R>> wp;
    ResponseReaderHolder(std::shared_ptr<ClientResponseReader<W, R>> p)
        : wp(p) {}
    void Close() override {
      auto sp = wp.lock();
      if (sp) {
        sp->Close();
      }
    }
    bool Expired() override { return wp.expired(); }
  };
  template <class W, class R>
  struct ReaderWriterHolder : Holder {
    std::weak_ptr<ClientReaderWriter<W, R>> wp;
    ReaderWriterHolder(std::shared_ptr<ClientReaderWriter<W, R>> p) : wp(p) {}
    void Close() override {
      auto sp = wp.lock();
      if (sp) {
        sp->Close();
      }
    }
    bool Expired() override { return wp.expired(); }
  };
  std::vector<std::unique_ptr<Holder>> holders_;

 public:
  ClientManager(int threads) : threads_(threads) {}

  ~ClientManager() { Shutdown(); }

  void Start() {
    for (auto& th : threads_) {
      th.thread.reset(new std::thread([& cq = th.cq]() { ThreadRun(&cq); }));
    }
  }

 private:
  static void ThreadRun(grpc::CompletionQueue* cq) {
    void* got_tag;
    bool ok = false;

    while (cq->Next(&got_tag, &ok)) {
      Handler* call = static_cast<Handler*>(got_tag);
      call->Proceed(ok);
    }
  }

  void Collect() {
    // expired な要素を削除する
    holders_.erase(std::remove_if(holders_.begin(), holders_.end(),
                                  [](const std::unique_ptr<Holder>& holder) {
                                    return holder->Expired();
                                  }),
                   holders_.end());
  }

 public:
  void Shutdown() {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_) {
      return;
    }
    shutdown_ = true;

    SPDLOG_TRACE("ClientManager::Shutdown started");

    for (auto&& holder : holders_) {
      holder->Close();
    }
    holders_.clear();

    SPDLOG_TRACE("ClientManager::Shutdown all client closed");

    // まず通常のキューを Shutdown して、全てのスレッドが終了するのを待つ
    // コールバックの処理で無限ループしてるとかじゃない限りは終了するはず
    for (auto& th : threads_) {
      th.cq.Shutdown();
    }

    SPDLOG_TRACE("ClientManager::Shutdown cq shutdown completed");

    for (auto& th : threads_) {
      th.thread->join();
      th.thread = nullptr;
    }

    SPDLOG_TRACE("ClientManager::Shutdown finished");
  }

  template <class W, class R>
  std::shared_ptr<ClientResponseReader<W, R>> CreateResponseReader(
      typename ClientResponseReader<W, R>::RequestFunc request) {
    std::lock_guard<std::mutex> guard(mutex_);

    Collect();

    auto client_id = next_client_id_++;
    auto cq = &threads_[client_id % threads_.size()].cq;

    std::shared_ptr<ClientResponseReader<W, R>> p(
        new ClientResponseReader<W, R>(cq, std::move(request)),
        [](ClientResponseReader<W, R>* p) { p->Release(); });
    holders_.push_back(
        std::unique_ptr<Holder>(new ResponseReaderHolder<W, R>(p)));
    return p;
  }

  template <class W, class R>
  std::shared_ptr<ClientReaderWriter<W, R>> CreateReaderWriter(
      typename ClientReaderWriter<W, R>::ConnectFunc connect) {
    std::lock_guard<std::mutex> guard(mutex_);

    Collect();

    auto client_id = next_client_id_++;
    auto cq = &threads_[client_id % threads_.size()].cq;

    std::shared_ptr<ClientReaderWriter<W, R>> p(
        new ClientReaderWriter<W, R>(cq, std::move(connect)),
        [](ClientReaderWriter<W, R>* p) { p->Release(); });
    holders_.push_back(
        std::unique_ptr<Holder>(new ReaderWriterHolder<W, R>(p)));
    return p;
  }
};

}  // namespace ggrpc

#endif  // GGRPC_CLIENT_H_INCLUDED
