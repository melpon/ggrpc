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
#include <grpcpp/alarm.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>

// spdlog
#include <spdlog/spdlog.h>

#include "handler.h"

namespace ggrpc {

template <class W>
class ResponseWriterHandler {
 public:
  virtual ~ResponseWriterHandler() {}
  virtual void Shutdown() = 0;
  virtual void Finish(W value, grpc::Status status) = 0;
  virtual void FinishWithError(grpc::Status status) = 0;
};

template <class W>
class ResponseWriterContext {
  ResponseWriterHandler<W>* handler_;

 public:
  ResponseWriterContext(ResponseWriterHandler<W>* handler)
      : handler_(handler) {}
  ~ResponseWriterContext() { handler_->Shutdown(); }
  void Finish(W resp, grpc::Status status) {
    handler_->Finish(std::move(resp), status);
  }
  void FinishWithError(grpc::Status status) {
    handler_->FinishWithError(status);
  }
};

template <class W, class R>
class ServerResponseWriterHandler : public ResponseWriterHandler<W> {
  std::mutex mutex_;
  grpc::ServerCompletionQueue* cq_;

  bool shutdown_ = false;

  grpc::ServerContext server_context_;
  grpc::ServerAsyncResponseWriter<W> response_writer_;

  std::shared_ptr<ResponseWriterContext<W>> context_;

  std::function<bool()> shutdown_requested_;
  std::function<void(grpc::ServerCompletionQueue*)> gen_handler_;

  R request_;
  struct ResponseData {
    bool error;
    // finish == false
    W response;
    // finish == true
    grpc::Status status;
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
  enum class WriteStatus { LISTENING, IDLE, FINISHING, FINISHED };
  WriteStatus write_status_ = WriteStatus::LISTENING;
  void SetWriteStatus(WriteStatus status) {
    write_status_ = status;
    if (write_status_ == WriteStatus::FINISHED) {
      context_.reset();
    }
  }

  grpc::Alarm alarm_;
  void Notify() { alarm_.Set(cq_, gpr_time_0(GPR_TIMESPAN), &notifier_thunk_); }

  struct Guard {
    ServerResponseWriterHandler* p_;
    std::shared_ptr<ResponseWriterContext<W>> context_;

    Guard(ServerResponseWriterHandler* p) : p_(p) {}
    void Init(std::shared_ptr<ResponseWriterContext<W>> context) {
      context_ = context;
    }

    ~Guard() {
      if (context_ == nullptr) {
        return;
      }

      // ここで Shutdown() が呼ばれる可能性がある
      context_.reset();
      // Shutdown 済みで、削除可能な状態なら削除する
      if (p_->shutdown_ && p_->write_status_ == WriteStatus::FINISHED) {
        delete p_;
      }
    }
  };

 public:
  ServerResponseWriterHandler()
      : response_writer_(&server_context_),
        acceptor_thunk_(this),
        writer_thunk_(this),
        notifier_thunk_(this) {}

  typedef W WriteType;
  typedef R ReadType;
  std::shared_ptr<ResponseWriterContext<W>> GetContext() { return context_; }

  virtual void OnRequest(grpc::ServerContext*, R*,
                         grpc::ServerAsyncResponseWriter<W>*,
                         grpc::ServerCompletionQueue*, void*) = 0;
  virtual void OnAccept(R request) = 0;

  void Init(std::function<bool()> shutdown_requested,
            std::function<void(grpc::ServerCompletionQueue*)> gen_handler,
            grpc::ServerCompletionQueue* cq,
            std::shared_ptr<ResponseWriterContext<W>> context) {
    shutdown_requested_ = std::move(shutdown_requested);
    gen_handler_ = std::move(gen_handler);
    cq_ = cq;
    context_ = context;
    OnRequest(&server_context_, &request_, &response_writer_, cq_,
              &acceptor_thunk_);
  }

  void Shutdown() override {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_) {
      return;
    }

    shutdown_ = true;

    // 読み書き中だったらキャンセルされるまで待つ
    if (write_status_ == WriteStatus::LISTENING ||
        write_status_ == WriteStatus::FINISHING) {
      server_context_.TryCancel();
    } else {
      // そうでないなら即座に終わらせて良い
      SetWriteStatus(WriteStatus::FINISHED);
    }
  }

  void Finish(W resp, grpc::Status status) override {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_ || shutdown_requested_()) {
      return;
    }

    if (write_status_ == WriteStatus::IDLE) {
      response_.error = false;
      response_.response = std::move(resp);
      response_.status = status;

      SetWriteStatus(WriteStatus::FINISHING);
      Notify();
    }
  }

  void FinishWithError(grpc::Status status) override {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_ || shutdown_requested_()) {
      return;
    }

    if (write_status_ == WriteStatus::IDLE) {
      response_.error = true;
      response_.status = status;

      SetWriteStatus(WriteStatus::FINISHING);
      Notify();
    }
  }

 private:
  void ProceedToAccept(bool ok) {
    Guard guard(this);
    std::unique_lock<std::mutex> lock(mutex_);
    guard.Init(context_);

    SPDLOG_TRACE("ProceedToAccept: ok={}", ok);

    // サーバのシャットダウン要求が来てたら次の Accept 待ちをしない
    if (shutdown_requested_()) {
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    // Accept の失敗も次の Accept 待ちをしない
    if (!ok) {
      SPDLOG_ERROR("Accept failed");
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    // 次の要求に備える
    gen_handler_(cq_);

    // このリクエストのシャットダウン要求されただけだった場合は次の Accept 待ちをする
    if (shutdown_) {
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    SetWriteStatus(WriteStatus::IDLE);

    lock.unlock();
    try {
      OnAccept(std::move(request_));
      lock.lock();
    } catch (...) {
      lock.lock();
    }
  }

  void ProceedToWrite(bool ok) {
    Guard guard(this);
    std::unique_lock<std::mutex> lock(mutex_);
    guard.Init(context_);

    SPDLOG_TRACE("ProceedToWrite: ok={}", ok);

    if (shutdown_ || shutdown_requested_()) {
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("write failed");
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    SetWriteStatus(WriteStatus::FINISHED);
  }

  void ProceedToNotify(bool ok) {
    std::lock_guard<std::mutex> guard(mutex_);

    SPDLOG_TRACE("ProceedToNotify: ok={}", ok);

    if (shutdown_ || shutdown_requested_()) {
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    if (!ok) {
      SPDLOG_WARN("Alarm cancelled");
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    if (!response_.error) {
      response_writer_.Finish(std::move(response_.response), response_.status,
                              &writer_thunk_);
    } else {
      response_writer_.FinishWithError(response_.status, &writer_thunk_);
    }
    SetWriteStatus(WriteStatus::FINISHING);
  }
};

template <class W>
class WritableHandler {
 public:
  virtual ~WritableHandler() {}
  virtual void Shutdown() = 0;
  virtual void Write(W resp) = 0;
  virtual void Finish(grpc::Status status) = 0;
};

template <class W>
class WritableContext {
  WritableHandler<W>* handler_;

 public:
  WritableContext(WritableHandler<W>* handler) : handler_(handler) {}
  ~WritableContext() { handler_->Shutdown(); }
  void Write(W resp) { handler_->Write(resp); }
  void Finish(grpc::Status status) { handler_->Finish(status); }
};

template <class W, class R>
class ServerReaderWriterHandler : public WritableHandler<W> {
  std::mutex mutex_;
  grpc::ServerCompletionQueue* cq_;

  bool shutdown_ = false;

  grpc::ServerContext server_context_;
  grpc::ServerAsyncReaderWriter<W, R> streamer_;

  std::shared_ptr<WritableContext<W>> context_;

  std::function<bool()> shutdown_requested_;
  std::function<void(grpc::ServerCompletionQueue*)> gen_handler_;

  R request_;
  struct ResponseData {
    bool finish;
    // finish == false
    W response;
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
  enum class ReadStatus { LISTENING, READING, FINISHED };
  enum class WriteStatus { LISTENING, WRITING, IDLE, FINISHING, FINISHED };
  ReadStatus read_status_ = ReadStatus::LISTENING;
  WriteStatus write_status_ = WriteStatus::LISTENING;
  void SetReadStatus(ReadStatus status) {
    read_status_ = status;
    if (read_status_ == ReadStatus::FINISHED &&
        write_status_ == WriteStatus::FINISHED) {
      context_.reset();
    }
  }
  void SetWriteStatus(WriteStatus status) {
    write_status_ = status;
    if (read_status_ == ReadStatus::FINISHED &&
        write_status_ == WriteStatus::FINISHED) {
      context_.reset();
    }
  }

  grpc::Alarm alarm_;
  void Notify() { alarm_.Set(cq_, gpr_time_0(GPR_TIMESPAN), &notifier_thunk_); }

  struct Guard {
    ServerReaderWriterHandler* p_;
    std::shared_ptr<WritableContext<W>> context_;

    Guard(ServerReaderWriterHandler* p) : p_(p) {}
    void Init(std::shared_ptr<WritableContext<W>> context) {
      context_ = context;
    }

    ~Guard() {
      if (context_ == nullptr) {
        return;
      }

      // ここで Shutdown() が呼ばれる可能性がある
      context_.reset();
      // Shutdown 済みで、削除可能な状態なら削除する
      if (p_->shutdown_ && p_->read_status_ == ReadStatus::FINISHED &&
          p_->write_status_ == WriteStatus::FINISHED) {
        delete p_;
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

  typedef W WriteType;
  typedef R ReadType;
  std::shared_ptr<WritableContext<W>> GetContext() { return context_; }

  virtual void OnRequest(grpc::ServerContext*,
                         grpc::ServerAsyncReaderWriter<W, R>*,
                         grpc::ServerCompletionQueue*, void*) = 0;
  virtual void OnAccept() = 0;
  virtual void OnRead(R) = 0;
  virtual void OnReadDoneOrError() = 0;

  void Init(std::function<bool()> shutdown_requested,
            std::function<void(grpc::ServerCompletionQueue*)> gen_handler,
            grpc::ServerCompletionQueue* cq,
            std::shared_ptr<WritableContext<W>> context) {
    shutdown_requested_ = std::move(shutdown_requested);
    gen_handler_ = std::move(gen_handler);
    cq_ = cq;
    context_ = context;
    OnRequest(&server_context_, &streamer_, cq_, &acceptor_thunk_);
  }

  void Shutdown() override {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_) {
      return;
    }

    shutdown_ = true;

    // 読み書き中だったらキャンセルされるまで待つ
    if (read_status_ == ReadStatus::LISTENING ||
        read_status_ == ReadStatus::READING ||
        write_status_ == WriteStatus::LISTENING ||
        write_status_ == WriteStatus::WRITING ||
        write_status_ == WriteStatus::FINISHING) {
      server_context_.TryCancel();
    } else {
      // そうでないなら即座に終わらせて良い
      SetReadStatus(ReadStatus::FINISHED);
      SetWriteStatus(WriteStatus::FINISHED);
    }
  }

  void Write(W resp) override {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_ || shutdown_requested_()) {
      return;
    }

    if (write_status_ == WriteStatus::IDLE) {
      ResponseData d;
      d.finish = false;
      d.response = std::move(resp);

      SetWriteStatus(WriteStatus::WRITING);
      response_queue_.push_back(std::move(d));
      Notify();
    } else if (write_status_ == WriteStatus::WRITING) {
      ResponseData d;
      d.finish = false;
      d.response = std::move(resp);

      response_queue_.push_back(std::move(d));
    }
  }

  void Finish(grpc::Status status) override {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_ || shutdown_requested_()) {
      return;
    }

    if (write_status_ == WriteStatus::IDLE) {
      ResponseData d;
      d.finish = false;
      d.status = std::move(status);

      SetWriteStatus(WriteStatus::FINISHING);
      response_queue_.push_back(std::move(d));
      Notify();
    } else if (write_status_ == WriteStatus::WRITING) {
      ResponseData d;
      d.finish = false;
      d.status = std::move(status);

      response_queue_.push_back(std::move(d));
    }
  }

 private:
  void ProceedToAccept(bool ok) {
    Guard guard(this);
    std::unique_lock<std::mutex> lock(mutex_);
    guard.Init(context_);

    SPDLOG_TRACE("ProceedToAccept: ok={}", ok);

    // サーバのシャットダウン要求が来てたら次の Accept 待ちをしない
    if (shutdown_requested_()) {
      SetReadStatus(ReadStatus::FINISHED);
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    // Accept の失敗も次の Accept 待ちをしない
    if (!ok) {
      SPDLOG_ERROR("Accept failed");
      SetReadStatus(ReadStatus::FINISHED);
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    // 次の要求に備える
    gen_handler_(cq_);

    // このリクエストのシャットダウン要求されただけだった場合は次の Accept 待ちをする
    if (shutdown_) {
      SetReadStatus(ReadStatus::FINISHED);
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    SetWriteStatus(WriteStatus::IDLE);

    lock.unlock();
    try {
      OnAccept();
      lock.lock();
    } catch (...) {
      lock.lock();
    }

    SetReadStatus(ReadStatus::READING);
    streamer_.Read(&request_, &reader_thunk_);
  }

  void ProceedToRead(bool ok) {
    Guard guard(this);
    std::unique_lock<std::mutex> lock(mutex_);
    guard.Init(context_);

    SPDLOG_TRACE("ProceedToRead: ok={}", ok);

    if (shutdown_ || shutdown_requested_()) {
      SetReadStatus(ReadStatus::FINISHED);
      return;
    }

    if (!ok) {
      // 読み込みがすべて完了した（あるいは失敗した）
      // あとは書き込み処理が終わるのを待つだけ
      lock.unlock();
      try {
        OnReadDoneOrError();
      } catch (...) {
        lock.lock();
      }

      SetReadStatus(ReadStatus::FINISHED);
      return;
    }

    lock.unlock();
    try {
      OnRead(std::move(request_));
      lock.lock();
    } catch (...) {
      lock.lock();
    }

    if (shutdown_) {
      SetReadStatus(ReadStatus::FINISHED);
      return;
    }

    // 次の読み込み
    streamer_.Read(&request_, &reader_thunk_);
  }

  void ProceedToWrite(bool ok) {
    Guard guard(this);
    std::unique_lock<std::mutex> lock(mutex_);
    guard.Init(context_);

    SPDLOG_TRACE("ProceedToWrite: ok={}", ok);

    if (shutdown_ || shutdown_requested_()) {
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("write failed");
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    if (write_status_ == WriteStatus::IDLE) {
    } else if (write_status_ == WriteStatus::WRITING) {
      response_queue_.pop_front();
      if (response_queue_.empty()) {
        SetWriteStatus(WriteStatus::IDLE);
      } else {
        auto& d = response_queue_.front();
        if (!d.finish) {
          // Response
          streamer_.Write(std::move(d.response), &writer_thunk_);
        } else {
          // Finish
          streamer_.Finish(d.status, &writer_thunk_);
          SetWriteStatus(WriteStatus::FINISHING);
        }
      }
    } else if (write_status_ == WriteStatus::FINISHING) {
      SetWriteStatus(WriteStatus::FINISHED);
    }
  }

  void ProceedToNotify(bool ok) {
    std::lock_guard<std::mutex> guard(mutex_);

    SPDLOG_TRACE("ProceedToNotify: ok={}", ok);

    if (shutdown_ || shutdown_requested_()) {
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    if (!ok) {
      SPDLOG_WARN("Alarm cancelled");
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    if (response_queue_.empty()) {
      SPDLOG_WARN("response_queue_ is empty");
      SetWriteStatus(WriteStatus::FINISHED);
      return;
    }

    auto& d = response_queue_.front();
    if (!d.finish) {
      // Response
      streamer_.Write(std::move(d.response), &writer_thunk_);
    } else {
      // Finish
      streamer_.Finish(d.status, &writer_thunk_);
      SetWriteStatus(WriteStatus::FINISHING);
    }
  }
};

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
  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  std::vector<std::unique_ptr<std::thread>> threads_;
  std::unique_ptr<grpc::Server> server_;
  std::mutex mutex_;

  struct GenHandler {
    std::function<void(grpc::ServerCompletionQueue*)> gen_handler;
  };
  std::vector<std::unique_ptr<GenHandler>> gen_handlers_;

  bool shutdown_ = false;

 public:
  Server(std::unique_ptr<grpc::Server> server,
         std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs)
      : server_(std::move(server)), cqs_(std::move(cqs)) {}
  ~Server() { Shutdown(); }

  template <class H, class... Args>
  void AddReaderWriterHandler(Args... args) {
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
    std::function<bool()> shutdown_requested = [this]() {
      std::lock_guard<std::mutex> guard(mutex_);
      return shutdown_;
    };

    std::unique_ptr<GenHandler> gh(new GenHandler());
    gh->gen_handler = [shutdown_requested = std::move(shutdown_requested),
                       gh = gh.get(), targs = std::move(targs)](
                          grpc::ServerCompletionQueue* cq) {
      H* handler = new_from_tuple<H>(std::move(targs));
      auto context =
          std::make_shared<WritableContext<typename H::WriteType>>(handler);
      handler->Init(shutdown_requested, gh->gen_handler, cq, context);
    };

    gen_handlers_.push_back(std::move(gh));
  }

  template <class H, class... Args>
  void AddResponseWriterHandler(Args... args) {
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
    std::function<bool()> shutdown_requested = [this]() {
      std::lock_guard<std::mutex> guard(mutex_);
      return shutdown_;
    };

    std::unique_ptr<GenHandler> gh(new GenHandler());
    gh->gen_handler =
        [shutdown_requested = std::move(shutdown_requested), gh = gh.get(),
         targs = std::move(targs)](grpc::ServerCompletionQueue* cq) {
          H* handler = new_from_tuple<H>(std::move(targs));
          auto context =
              std::make_shared<ResponseWriterContext<typename H::WriteType>>(
                  handler);
          handler->Init(shutdown_requested, gh->gen_handler, cq, context);
        };

    gen_handlers_.push_back(std::move(gh));
  }

  void Start() {
    std::lock_guard<std::mutex> guard(mutex_);

    // 既に Start 済み
    if (threads_.size() != 0) {
      return;
    }
    // 既に Shutdown 済み
    if (shutdown_) {
      return;
    }

    for (int i = 0; i < cqs_.size(); i++) {
      auto cq = cqs_[i].get();
      threads_.push_back(std::unique_ptr<std::thread>(
          new std::thread([this, cq] { this->HandleRpcs(cq); })));
    }
  }

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

    server_->Shutdown();
    // サーバをシャットダウンした後に completion queue を削除する必要がある
    for (auto& cq : cqs_) {
      cq->Shutdown();
    }
    for (auto& thread : threads_) {
      lock.unlock();
      try {
        thread->join();
        lock.lock();
      } catch (...) {
        lock.lock();
      }
    }
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
};

}  // namespace ggrpc

#endif // GGRPC_SERVER_H_INCLUDED
