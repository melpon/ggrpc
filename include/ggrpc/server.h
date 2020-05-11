#ifndef GGRPC_SERVER_H_INCLUDED
#define GGRPC_SERVER_H_INCLUDED

// グレースフルシャットダウンやマルチスレッドに対応した、
// 安全に利用できる gRPC サーバ

#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <sstream>
#include <thread>
#include <tuple>

// gRPC
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>

// spdlog
#include <spdlog/spdlog.h>

#include "alarm.h"
#include "handler.h"

namespace ggrpc {

class Server;

enum class ServerResponseWriterError {
  WRITE,
};

template <class W, class R>
class ServerResponseWriterHandler;

template <class W, class R>
class ServerResponseWriterContext {
  ServerResponseWriterHandler<W, R>* p_;

  ServerResponseWriterContext(ServerResponseWriterHandler<W, R>* p);

  friend class Server;

 public:
  ~ServerResponseWriterContext();
  Server* GetServer() const;
  void Finish(W resp, grpc::Status status);
  void FinishWithError(grpc::Status status);
  void Close();
};

template <class W, class R>
class ServerResponseWriterHandler {
  mutable std::mutex mutex_;
  Server* server_;
  grpc::ServerCompletionQueue* cq_;

  grpc::ServerContext server_context_;
  grpc::ServerAsyncResponseWriter<W> response_writer_;

  std::function<void(grpc::ServerCompletionQueue*)> gen_handler_;

  R request_;
  struct ResponseData {
    bool error;
    // finish == false
    W response;
    grpc::Status status;
    int64_t id;
  };
  ResponseData response_;

  struct AcceptorThunk : Handler {
    ServerResponseWriterHandler* p;
    AcceptorThunk(ServerResponseWriterHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToAccept(ok); }
  };
  AcceptorThunk acceptor_thunk_;
  friend class AcceptorThunk;

  struct WriterThunk : Handler {
    ServerResponseWriterHandler* p;
    WriterThunk(ServerResponseWriterHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToWrite(ok); }
  };
  WriterThunk writer_thunk_;
  friend class WriterThunk;

  struct NotifierThunk : Handler {
    ServerResponseWriterHandler* p;
    NotifierThunk(ServerResponseWriterHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToNotify(ok); }
  };
  NotifierThunk notifier_thunk_;
  friend class NotifierThunk;

  // 状態マシン
  enum class Status { LISTENING, IDLE, FINISHING, CANCELING, FINISHED };
  Status status_ = Status::LISTENING;

  bool release_ = false;
  int nesting_ = 0;

  grpc::Alarm alarm_;
  void Notify() { alarm_.Set(cq_, gpr_time_0(GPR_TIMESPAN), &notifier_thunk_); }

  std::shared_ptr<ServerResponseWriterContext<W, R>> context_;
  std::shared_ptr<ServerResponseWriterContext<W, R>> tmp_context_;

  struct SafeDeleter {
    ServerResponseWriterHandler<W, R>* p;
    std::unique_lock<std::mutex> lock;
    SafeDeleter(ServerResponseWriterHandler<W, R>* p) : p(p), lock(p->mutex_) {}
    ~SafeDeleter() {
      bool del =
          p->release_ &&
          p->status_ == ServerResponseWriterHandler<W, R>::Status::FINISHED &&
          p->nesting_ == 0;
      lock.unlock();
      if (del) {
        delete p;
      }
    }
  };

 public:
  ServerResponseWriterHandler()
      : response_writer_(&server_context_),
        acceptor_thunk_(this),
        writer_thunk_(this),
        notifier_thunk_(this) {}

  virtual ~ServerResponseWriterHandler() {
    SPDLOG_TRACE("[0x{}] delete ServerResponseWriterHandler", (void*)this);
  }

 private:
  // ServerResponseWriterContext への参照が消えると自身の Release() が呼ばれるように仕込む
  // Close() や Finish() を呼ぶと自身の持つ context は nullptr になる
  // 誰かが参照してれば引き続き読み書きできる
  void Init(Server* server, grpc::ServerCompletionQueue* cq,
            std::function<void(grpc::ServerCompletionQueue*)> gen_handler,
            std::shared_ptr<ServerResponseWriterContext<W, R>> context) {
    server_ = server;
    cq_ = cq;
    gen_handler_ = std::move(gen_handler);
    context_ = context;
    Request(&server_context_, &request_, &response_writer_, cq_,
            &acceptor_thunk_);
  }
  typedef W WriteType;
  typedef R ReadType;
  friend class Server;

  void Release() {
    SafeDeleter d(this);

    release_ = true;

    DoClose(d.lock);
  }
  friend class ServerResponseWriterContext<W, R>;

  void Close() {
    SafeDeleter d(this);

    DoClose(d.lock);
  }

 public:
  // Close を呼ぶと context_ が nullptr になってしまうが、
  // コールバック中に Context() が無効になると使いにくいので、
  // コールバック中は tmp_context_ を設定しておいて、それが利用可能な場合はそちらを使う
  std::shared_ptr<ServerResponseWriterContext<W, R>> Context() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return tmp_context_ ? tmp_context_ : context_;
  }

 private:
  void DoClose(std::unique_lock<std::mutex>& lock) {
    server_ = nullptr;

    // Listen 中ならサーバのシャットダウンで終わるのでキャンセル状態にして待つだけ
    if (status_ == Status::LISTENING) {
      status_ = Status::CANCELING;
      return;
    }

    // 読み書き中ならキャンセルする
    if (status_ == Status::FINISHING) {
      server_context_.TryCancel();
      status_ = Status::CANCELING;
      return;
    }

    if (status_ == Status::CANCELING) {
      return;
    }

    status_ = Status::FINISHED;
    Done(lock);
  }

  void Done(std::unique_lock<std::mutex>& lock) {
    if (status_ != Status::FINISHED) {
      return;
    }

    auto context = std::move(context_);

    ++nesting_;
    lock.unlock();
    // 誰も参照してなければここで Release() が呼ばれる
    context.reset();
    lock.lock();
    --nesting_;
  }

 private:
  Server* GetServer() const { return server_; }

  void Finish(W resp, grpc::Status status) {
    std::lock_guard<std::mutex> guard(mutex_);
    SPDLOG_TRACE("[0x{}] Finish: {}", (void*)this, resp.DebugString());

    if (status_ == Status::IDLE) {
      response_.error = false;
      response_.response = std::move(resp);
      response_.status = status;

      status_ = Status::FINISHING;
      Notify();
    }
  }

  void FinishWithError(grpc::Status status) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (status_ == Status::IDLE) {
      response_.error = true;
      response_.status = status;

      status_ = Status::FINISHING;
      Notify();
    }
  }

 private:
  template <class MemF, class... Args>
  void RunCallback(std::unique_lock<std::mutex>& lock, std::string funcname,
                   MemF mf, Args&&... args) {
    // コールバック中は tmp_context_ を有効にする
    if (nesting_ == 0) {
      tmp_context_ = context_;
    }
    ++nesting_;
    lock.unlock();
    try {
      SPDLOG_TRACE("call {}", funcname);
      (this->*mf)(std::forward<Args>(args)...);
    } catch (std::exception& e) {
      SPDLOG_ERROR("{} error: what={}", funcname, e.what());
    } catch (...) {
      SPDLOG_ERROR("{} error", funcname);
    }
    lock.lock();
    --nesting_;

    if (nesting_ == 0) {
      auto context = std::move(tmp_context_);
      ++nesting_;
      lock.unlock();
      context.reset();
      lock.lock();
      --nesting_;
    }
  }

  void ProceedToAccept(bool ok) {
    SafeDeleter d(this);

    SPDLOG_TRACE("[0x{}] ProceedToAccept: ok={}", (void*)this, ok);

    assert(status_ == Status::LISTENING || status_ == Status::CANCELING);

    if (status_ == Status::CANCELING) {
      // 終了
      status_ = Status::FINISHED;
      Done(d.lock);
      return;
    }

    // Accept 失敗
    if (!ok) {
      SPDLOG_ERROR("accept failed");
      status_ = Status::FINISHED;
      Done(d.lock);
      return;
    }

    // 次の要求に備える
    d.lock.unlock();
    gen_handler_(cq_);
    d.lock.lock();

    status_ = Status::IDLE;

    RunCallback(d.lock, "OnAccept", &ServerResponseWriterHandler::OnAccept,
                std::move(request_));
  }

  void ProceedToWrite(bool ok) {
    SafeDeleter d(this);

    SPDLOG_TRACE("[0x{}] ProceedToWrite: ok={}", (void*)this, ok);

    assert(status_ == Status::FINISHING || status_ == Status::CANCELING);

    if (status_ == Status::CANCELING) {
      // 終了
      status_ = Status::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("write failed");
      status_ = Status::FINISHED;
      RunCallback(d.lock, "OnError", &ServerResponseWriterHandler::OnError,
                  ServerResponseWriterError::WRITE);
      Done(d.lock);
      return;
    }

    status_ = Status::FINISHED;
    RunCallback(d.lock, "OnFinish", &ServerResponseWriterHandler::OnFinish,
                std::move(response_.response), response_.status);
    Done(d.lock);
  }

  void ProceedToNotify(bool ok) {
    SafeDeleter d(this);

    SPDLOG_TRACE("[0x{}] ProceedToNotify: ok={}", (void*)this, ok);

    assert(status_ == Status::FINISHING || status_ == Status::CANCELING);

    if (status_ == Status::CANCELING) {
      // 終了
      status_ = Status::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      SPDLOG_WARN("Alarm cancelled");
      status_ = Status::FINISHED;
      RunCallback(d.lock, "OnError", &ServerResponseWriterHandler::OnError,
                  ServerResponseWriterError::WRITE);
      Done(d.lock);
      return;
    }

    if (!response_.error) {
      response_writer_.Finish(response_.response, response_.status,
                              &writer_thunk_);
    } else {
      response_writer_.FinishWithError(response_.status, &writer_thunk_);
    }
  }

 public:
  virtual void Request(grpc::ServerContext* context, R* request,
                       grpc::ServerAsyncResponseWriter<W>* response_writer,
                       grpc::ServerCompletionQueue* cq, void* tag) = 0;
  virtual void OnAccept(R request) {}
  virtual void OnFinish(W response, grpc::Status status) {}
  virtual void OnError(ServerResponseWriterError error) {}
};

template <class W, class R>
ServerResponseWriterContext<W, R>::ServerResponseWriterContext(
    ServerResponseWriterHandler<W, R>* p)
    : p_(p) {}
template <class W, class R>
ServerResponseWriterContext<W, R>::~ServerResponseWriterContext() {
  p_->Release();
}
template <class W, class R>
Server* ServerResponseWriterContext<W, R>::GetServer() const {
  return p_->GetServer();
}
template <class W, class R>
void ServerResponseWriterContext<W, R>::Finish(W resp, grpc::Status status) {
  p_->Finish(std::move(resp), status);
}
template <class W, class R>
void ServerResponseWriterContext<W, R>::FinishWithError(grpc::Status status) {
  p_->FinishWithError(status);
}
template <class W, class R>
void ServerResponseWriterContext<W, R>::Close() {
  p_->Close();
}

enum class ServerReaderWriterError {
  WRITE,
};

template <class W, class R>
class ServerReaderWriterHandler;

template <class W, class R>
class ServerReaderWriterContext {
  ServerReaderWriterHandler<W, R>* p_;

  ServerReaderWriterContext(ServerReaderWriterHandler<W, R>* p);
  friend class Server;

 public:
  ~ServerReaderWriterContext();
  Server* GetServer() const;
  void Write(W resp, int64_t id = 0);
  void Finish(grpc::Status status);
  void Close();
};

template <class W, class R>
class ServerReaderWriterHandler {
  mutable std::mutex mutex_;
  Server* server_;
  grpc::ServerCompletionQueue* cq_;

  grpc::ServerContext server_context_;
  grpc::ServerAsyncReaderWriter<W, R> streamer_;

  std::function<bool()> shutdown_requested_;
  std::function<void(grpc::ServerCompletionQueue*)> gen_handler_;

  R request_;
  struct ResponseData {
    bool finish;
    // finish == false
    W response;
    int64_t id;
    // finish == true
    grpc::Status status;
  };
  std::deque<ResponseData> response_queue_;

  struct AcceptorThunk : Handler {
    ServerReaderWriterHandler* p;
    AcceptorThunk(ServerReaderWriterHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToAccept(ok); }
  };
  AcceptorThunk acceptor_thunk_;
  friend class AcceptorThunk;

  struct ReaderThunk : Handler {
    ServerReaderWriterHandler* p;
    ReaderThunk(ServerReaderWriterHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToRead(ok); }
  };
  ReaderThunk reader_thunk_;
  friend class ReaderThunk;

  struct WriterThunk : Handler {
    ServerReaderWriterHandler* p;
    WriterThunk(ServerReaderWriterHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToWrite(ok); }
  };
  WriterThunk writer_thunk_;
  friend class WriterThunk;

  struct NotifierThunk : Handler {
    ServerReaderWriterHandler* p;
    NotifierThunk(ServerReaderWriterHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToNotify(ok); }
  };
  NotifierThunk notifier_thunk_;
  friend class NotifierThunk;

  // 状態マシン
  enum class ReadStatus { LISTENING, READING, CANCELING, FINISHED };
  enum class WriteStatus {
    LISTENING,
    WRITING,
    IDLE,
    FINISHING,
    CANCELING,
    FINISHED
  };
  ReadStatus read_status_ = ReadStatus::LISTENING;
  WriteStatus write_status_ = WriteStatus::LISTENING;

  bool release_ = false;
  int nesting_ = 0;

  grpc::Alarm alarm_;
  void Notify() { alarm_.Set(cq_, gpr_time_0(GPR_TIMESPAN), &notifier_thunk_); }

  std::shared_ptr<ServerReaderWriterContext<W, R>> context_;
  std::shared_ptr<ServerReaderWriterContext<W, R>> tmp_context_;

  struct SafeDeleter {
    ServerReaderWriterHandler<W, R>* p;
    std::unique_lock<std::mutex> lock;
    SafeDeleter(ServerReaderWriterHandler<W, R>* p) : p(p), lock(p->mutex_) {}
    ~SafeDeleter() {
      bool del = p->release_ &&
                 p->read_status_ ==
                     ServerReaderWriterHandler<W, R>::ReadStatus::FINISHED &&
                 p->write_status_ ==
                     ServerReaderWriterHandler<W, R>::WriteStatus::FINISHED &&
                 p->nesting_ == 0;
      lock.unlock();
      if (del) {
        delete p;
      }
    }
  };

 public:
  ServerReaderWriterHandler()
      : streamer_(&server_context_),
        acceptor_thunk_(this),
        reader_thunk_(this),
        writer_thunk_(this),
        notifier_thunk_(this) {}
  virtual ~ServerReaderWriterHandler() {
    SPDLOG_TRACE("[0x{}] deleted ServerReaderWriterHandler", (void*)this);
  }

 private:
  // ServerResponseWriterContext への参照が消えると自身の Release() が呼ばれるように仕込む
  // Close() や Finish() を呼ぶと自身の持つ context は nullptr になる
  // 誰かが参照してれば引き続き読み書きできる
  void Init(Server* server, grpc::ServerCompletionQueue* cq,
            std::function<void(grpc::ServerCompletionQueue*)> gen_handler,
            std::shared_ptr<ServerReaderWriterContext<W, R>> context) {
    server_ = server;
    cq_ = cq;
    gen_handler_ = std::move(gen_handler);
    context_ = context;
    Request(&server_context_, &streamer_, cq_, &acceptor_thunk_);
  }
  typedef W WriteType;
  typedef R ReadType;
  friend class Server;

  void Release() {
    SafeDeleter d(this);

    release_ = true;

    DoClose(d.lock);
  }
  friend class ServerReaderWriterContext<W, R>;

  void Close() {
    SafeDeleter d(this);

    DoClose(d.lock);
  }

 public:
  // Close を呼ぶと context_ が nullptr になってしまうが、
  // コールバック中に Context() が無効になると使いにくいので、
  // コールバック中は tmp_context_ を設定しておいて、それが利用可能な場合はそちらを使う
  std::shared_ptr<ServerReaderWriterContext<W, R>> Context() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return tmp_context_ ? tmp_context_ : context_;
  }

 private:
  void DoClose(std::unique_lock<std::mutex>& lock) {
    server_ = nullptr;

    // 読み書き中ならキャンセルする
    if (read_status_ == ReadStatus::READING ||
        write_status_ == WriteStatus::WRITING ||
        write_status_ == WriteStatus::FINISHING) {
      server_context_.TryCancel();
      // 書き込み中ならアラームもキャンセルする
      if (write_status_ == WriteStatus::WRITING ||
          write_status_ == WriteStatus::FINISHING) {
        alarm_.Cancel();
      }
    }

    if (read_status_ == ReadStatus::READING ||
        read_status_ == ReadStatus::CANCELING ||
        read_status_ == ReadStatus::LISTENING) {
      read_status_ = ReadStatus::CANCELING;
    } else {
      read_status_ = ReadStatus::FINISHED;
    }
    if (write_status_ == WriteStatus::WRITING ||
        write_status_ == WriteStatus::FINISHING ||
        write_status_ == WriteStatus::CANCELING ||
        write_status_ == WriteStatus::LISTENING) {
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

    auto context = std::move(context_);

    ++nesting_;
    lock.unlock();
    // 誰も参照してなければここで Release() が呼ばれる
    context.reset();
    lock.lock();
    --nesting_;
  }

 private:
  Server* GetServer() const { return server_; }

  void Write(W resp, int64_t id) {
    std::lock_guard<std::mutex> guard(mutex_);

    SPDLOG_TRACE("[0x{}] Write: {}", (void*)this, resp.DebugString());

    if (write_status_ == WriteStatus::IDLE) {
      ResponseData d;
      d.finish = false;
      d.response = std::move(resp);
      d.id = id;

      write_status_ = WriteStatus::WRITING;
      response_queue_.push_back(std::move(d));
      Notify();
    } else if (write_status_ == WriteStatus::WRITING) {
      ResponseData d;
      d.finish = false;
      d.response = std::move(resp);
      d.id = id;

      response_queue_.push_back(std::move(d));
    }
  }

  void Finish(grpc::Status status) {
    std::lock_guard<std::mutex> guard(mutex_);

    SPDLOG_TRACE("[0x{}] Finish", (void*)this);

    if (write_status_ == WriteStatus::IDLE) {
      ResponseData d;
      d.finish = true;
      d.status = std::move(status);

      write_status_ = WriteStatus::FINISHING;
      response_queue_.push_back(std::move(d));
      Notify();
    } else if (write_status_ == WriteStatus::WRITING) {
      ResponseData d;
      d.finish = true;
      d.status = std::move(status);

      response_queue_.push_back(std::move(d));
    }
  }

 private:
  template <class MemF, class... Args>
  void RunCallback(std::unique_lock<std::mutex>& lock, std::string funcname,
                   MemF mf, Args&&... args) {
    // コールバック中は tmp_context_ を有効にする
    if (nesting_ == 0) {
      tmp_context_ = context_;
    }
    ++nesting_;
    lock.unlock();
    try {
      SPDLOG_TRACE("call {}", funcname);
      (this->*mf)(std::forward<Args>(args)...);
    } catch (std::exception& e) {
      SPDLOG_ERROR("{} error: what={}", funcname, e.what());
    } catch (...) {
      SPDLOG_ERROR("{} error", funcname);
    }
    lock.lock();
    --nesting_;

    if (nesting_ == 0) {
      auto context = std::move(tmp_context_);
      ++nesting_;
      lock.unlock();
      context.reset();
      lock.lock();
      --nesting_;
    }
  }

  void ProceedToAccept(bool ok) {
    SafeDeleter d(this);

    SPDLOG_TRACE("[0x{}] ProceedToAccept: ok={}", (void*)this, ok);

    assert(read_status_ == ReadStatus::LISTENING &&
               write_status_ == WriteStatus::LISTENING ||
           read_status_ == ReadStatus::CANCELING &&
               write_status_ == WriteStatus::CANCELING);

    if (read_status_ == ReadStatus::CANCELING ||
        write_status_ == WriteStatus::CANCELING) {
      // 終了
      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    // Accept 失敗
    if (!ok) {
      SPDLOG_ERROR("Accept failed");
      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    // 次の要求に備える
    d.lock.unlock();
    gen_handler_(cq_);
    d.lock.lock();

    read_status_ = ReadStatus::READING;
    write_status_ = WriteStatus::IDLE;

    streamer_.Read(&request_, &reader_thunk_);

    RunCallback(d.lock, "OnAccept", &ServerReaderWriterHandler::OnAccept);
  }

  void ProceedToRead(bool ok) {
    SafeDeleter d(this);

    SPDLOG_TRACE("[0x{}] ProceedToRead: ok={}", (void*)this, ok);

    assert(read_status_ == ReadStatus::READING ||
           read_status_ == ReadStatus::CANCELING);

    if (read_status_ == ReadStatus::CANCELING) {
      // 終了
      read_status_ = ReadStatus::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      // 読み込みがすべて完了した（あるいは失敗した）
      // あとは書き込み処理が終わるのを待つだけ
      read_status_ = ReadStatus::FINISHED;
      RunCallback(d.lock, "OnReadDoneOrError",
                  &ServerReaderWriterHandler::OnReadDoneOrError);
      Done(d.lock);
      return;
    }

    RunCallback(d.lock, "OnRead", &ServerReaderWriterHandler::OnRead,
                std::move(request_));

    // 次の読み込み
    streamer_.Read(&request_, &reader_thunk_);
  }

  void ProceedToWrite(bool ok) {
    SafeDeleter d(this);

    SPDLOG_TRACE("[0x{}] ProceedToWrite: ok={}", (void*)this, ok);

    assert(write_status_ == WriteStatus::WRITING ||
           write_status_ == WriteStatus::FINISHING ||
           write_status_ == WriteStatus::CANCELING);
    assert(!response_queue_.empty());

    if (write_status_ == WriteStatus::CANCELING) {
      // 終了
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("write failed");
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnError", &ServerReaderWriterHandler::OnError,
                  ServerReaderWriterError::WRITE);
      Done(d.lock);
      return;
    }

    auto resp = std::move(response_queue_.front());
    response_queue_.pop_front();

    if (write_status_ == WriteStatus::WRITING) {
      if (response_queue_.empty()) {
        write_status_ = WriteStatus::IDLE;
      } else {
        auto& next = response_queue_.front();
        if (!next.finish) {
          // Response
          streamer_.Write(next.response, &writer_thunk_);
        } else {
          // Finish
          streamer_.Finish(next.status, &writer_thunk_);
          write_status_ = WriteStatus::FINISHING;
        }
      }
      RunCallback(d.lock, "OnWrite", &ServerReaderWriterHandler::OnWrite,
                  std::move(resp.response), resp.id);
    } else if (write_status_ == WriteStatus::FINISHING) {
      assert(resp.finish);
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnFinish", &ServerReaderWriterHandler::OnFinish,
                  resp.status);
      Done(d.lock);
    }
  }

  void ProceedToNotify(bool ok) {
    SafeDeleter d(this);

    SPDLOG_TRACE("[0x{}] ProceedToNotify: ok={}", (void*)this, ok);

    assert(write_status_ == WriteStatus::WRITING ||
           write_status_ == WriteStatus::FINISHING ||
           write_status_ == WriteStatus::CANCELING);
    assert(!response_queue_.empty());

    if (write_status_ == WriteStatus::CANCELING) {
      // 終了
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("alarm cancelled");
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnError", &ServerReaderWriterHandler::OnError,
                  ServerReaderWriterError::WRITE);
      Done(d.lock);
      return;
    }

    auto& data = response_queue_.front();
    if (!data.finish) {
      // Response
      streamer_.Write(std::move(data.response), &writer_thunk_);
    } else {
      // Finish
      streamer_.Finish(data.status, &writer_thunk_);
    }
  }

 public:
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

template <class W, class R>
ServerReaderWriterContext<W, R>::ServerReaderWriterContext(
    ServerReaderWriterHandler<W, R>* p)
    : p_(p) {}
template <class W, class R>
ServerReaderWriterContext<W, R>::~ServerReaderWriterContext() {
  p_->Release();
}
template <class W, class R>
Server* ServerReaderWriterContext<W, R>::GetServer() const {
  return p_->GetServer();
}
template <class W, class R>
void ServerReaderWriterContext<W, R>::Write(W resp, int64_t id) {
  p_->Write(std::move(resp), id);
}
template <class W, class R>
void ServerReaderWriterContext<W, R>::Finish(grpc::Status status) {
  p_->Finish(status);
}
template <class W, class R>
void ServerReaderWriterContext<W, R>::Close() {
  p_->Close();
}

template <class T, class Tuple, std::size_t... Index>
T* new_from_tuple_impl(Tuple&& t, std::index_sequence<Index...>) {
  return new T(std::get<Index>(std::forward<Tuple>(t))...);
}

template <class T, class Tuple>
T* new_from_tuple(Tuple&& t) {
  return new_from_tuple_impl<T>(
      std::forward<Tuple>(t),
      std::make_index_sequence<
          std::tuple_size<std::remove_reference_t<Tuple>>::value>{});
}

class Server {
 public:
  Server() {}
  ~Server() { Shutdown(); }

  template <class H, class... Args>
  void AddResponseWriterHandler(Args... args) {
    typedef typename H::WriteType W;
    typedef typename H::ReadType R;

    std::lock_guard<std::mutex> guard(mutex_);

    // 既に Start 済み
    if (threads_.size() != 0) {
      return;
    }
    // 既に Shutdown 済み
    if (shutdown_) {
      return;
    }

    std::tuple<Args...> targs(std::move(args)...);

    std::unique_ptr<GenHandler> gh(new GenHandler());
    gh->gen_handler = [this, gh = gh.get(), targs = std::move(targs)](
                          grpc::ServerCompletionQueue* cq) {
      std::lock_guard<std::mutex> guard(mutex_);

      H* handler = new_from_tuple<H>(std::move(targs));
      auto context = std::shared_ptr<ServerResponseWriterContext<W, R>>(
          new ServerResponseWriterContext<W, R>(handler));
      handler->Init(this, cq, gh->gen_handler, context);
      Collect();
      holders_.push_back(
          std::unique_ptr<Holder>(new ResponseWriterHolder<W, R>(context)));
    };

    gen_handlers_.push_back(std::move(gh));
  }

  template <class H, class... Args>
  void AddReaderWriterHandler(Args... args) {
    typedef typename H::WriteType W;
    typedef typename H::ReadType R;

    std::lock_guard<std::mutex> guard(mutex_);

    // 既に Start 済み
    if (threads_.size() != 0) {
      return;
    }
    // 既に Shutdown 済み
    if (shutdown_) {
      return;
    }

    std::tuple<Args...> targs(std::move(args)...);

    std::unique_ptr<GenHandler> gh(new GenHandler());
    gh->gen_handler = [this, gh = gh.get(), targs = std::move(targs)](
                          grpc::ServerCompletionQueue* cq) {
      std::lock_guard<std::mutex> guard(mutex_);

      H* handler = new_from_tuple<H>(std::move(targs));
      auto context = std::shared_ptr<ServerReaderWriterContext<W, R>>(
          new ServerReaderWriterContext<W, R>(handler));
      handler->Init(this, cq, gh->gen_handler, context);
      Collect();
      holders_.push_back(
          std::unique_ptr<Holder>(new ReaderWriterHolder<W, R>(context)));
    };

    gen_handlers_.push_back(std::move(gh));
  }

  void Start(grpc::ServerBuilder& builder, int threads) {
    std::lock_guard<std::mutex> guard(mutex_);

    // 既に Start 済み
    if (threads_.size() != 0) {
      return;
    }
    // 既に Shutdown 済み
    if (shutdown_) {
      return;
    }

    std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs;
    for (int i = 0; i < threads; i++) {
      cqs.push_back(builder.AddCompletionQueue());
    }

    server_ = builder.BuildAndStart();
    cqs_ = std::move(cqs);

    for (int i = 0; i < cqs_.size(); i++) {
      auto cq = cqs_[i].get();
      threads_.push_back(std::unique_ptr<std::thread>(
          new std::thread([this, cq] { this->HandleRpcs(cq); })));
    }
  }
  void Wait() { server_->Wait(); }

  void Shutdown() {
    std::unique_lock<std::mutex> lock(mutex_);

    // Start してない
    if (threads_.size() == 0) {
      return;
    }
    if (shutdown_) {
      return;
    }
    shutdown_ = true;

    SPDLOG_INFO("Server Shutdown start");

    // 全ての実行中のハンドラに対して Close() を呼ぶ
    for (auto& holder : holders_) {
      holder->Close();
    }
    holders_.clear();

    server_->Shutdown();
    // サーバをシャットダウンした後に completion queue を削除する必要がある
    for (auto& cq : cqs_) {
      cq->Shutdown();
    }

    SPDLOG_INFO("Server Shutdown waiting");

    for (auto& thread : threads_) {
      lock.unlock();
      try {
        thread->join();
      } catch (...) {
        SPDLOG_ERROR("Server Shutdown thread joining failed");
      }
      lock.lock();
    }

    SPDLOG_INFO("Server Shutdown completed");
  }

  std::shared_ptr<Alarm> CreateAlarm() {
    std::lock_guard<std::mutex> guard(mutex_);

    // Start してない
    if (threads_.size() == 0) {
      return nullptr;
    }
    if (shutdown_) {
      return nullptr;
    }

    Collect();

    auto id = next_id_++;
    auto cq = cqs_[id % cqs_.size()].get();

    std::shared_ptr<Alarm> p(new Alarm(cq), [](Alarm* p) { p->Release(); });
    holders_.push_back(std::unique_ptr<Holder>(new AlarmHolder(p)));
    return p;
  }

 private:
  void HandleRpcs(grpc::ServerCompletionQueue* cq) {
    for (auto& gh : gen_handlers_) {
      gh->gen_handler(cq);
    }

    void* got_tag = nullptr;
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

  struct Holder {
    virtual ~Holder() {}
    virtual void Close() = 0;
    virtual bool Expired() = 0;
  };
  template <class W, class R>
  struct ResponseWriterHolder : Holder {
    std::weak_ptr<ServerResponseWriterContext<W, R>> wp;
    ResponseWriterHolder(std::shared_ptr<ServerResponseWriterContext<W, R>> p)
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
    std::weak_ptr<ServerReaderWriterContext<W, R>> wp;
    ReaderWriterHolder(std::shared_ptr<ServerReaderWriterContext<W, R>> p)
        : wp(p) {}
    void Close() override {
      auto sp = wp.lock();
      if (sp) {
        sp->Close();
      }
    }
    bool Expired() override { return wp.expired(); }
  };
  struct AlarmHolder : Holder {
    std::weak_ptr<Alarm> wp;
    AlarmHolder(std::shared_ptr<Alarm> p) : wp(p) {}
    void Close() override {
      auto sp = wp.lock();
      if (sp) {
        sp->Close();
      }
    }
    bool Expired() override { return wp.expired(); }
  };
  std::vector<std::unique_ptr<Holder>> holders_;

  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  std::vector<std::unique_ptr<std::thread>> threads_;
  std::unique_ptr<grpc::Server> server_;
  mutable std::mutex mutex_;

  struct GenHandler {
    std::function<void(grpc::ServerCompletionQueue*)> gen_handler;
  };
  std::vector<std::unique_ptr<GenHandler>> gen_handlers_;

  bool shutdown_ = false;
  int next_id_ = 0;
};

}  // namespace ggrpc

#endif // GGRPC_SERVER_H_INCLUDED
