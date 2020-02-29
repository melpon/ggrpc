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

template <class W, class R>
class ServerResponseWriterHandler {
  std::mutex mutex_;
  grpc::ServerCompletionQueue* cq_;

  grpc::ServerContext server_context_;
  grpc::ServerAsyncResponseWriter<W> response_writer_;

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

  grpc::Alarm alarm_;
  void Notify() { alarm_.Set(cq_, gpr_time_0(GPR_TIMESPAN), &notifier_thunk_); }

 public:
  ServerResponseWriterHandler()
      : response_writer_(&server_context_),
        acceptor_thunk_(this),
        writer_thunk_(this),
        notifier_thunk_(this) {}

  ~ServerResponseWriterHandler() {}

  void Init(std::function<bool()> shutdown_requested,
            grpc::ServerCompletionQueue* cq,
            std::function<void(grpc::ServerCompletionQueue*)> gen_handler) {
    shutdown_requested_ = std::move(shutdown_requested);
    cq_ = cq;
    gen_handler_ = std::move(gen_handler);
    Request(&server_context_, &request_, &response_writer_, cq_,
            &acceptor_thunk_);
  }

  typedef W WriteType;
  typedef R ReadType;

  void Finish(W resp, grpc::Status status) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_requested_()) {
      return;
    }

    SPDLOG_TRACE("[0x{}] Finish: {}", (void*)this, resp.DebugString());

    if (write_status_ == WriteStatus::IDLE) {
      response_.error = false;
      response_.response = std::move(resp);
      response_.status = status;

      write_status_ = WriteStatus::FINISHING;
      Notify();
    }
  }

  void FinishWithError(grpc::Status status) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_requested_()) {
      return;
    }

    if (write_status_ == WriteStatus::IDLE) {
      response_.error = true;
      response_.status = status;

      write_status_ = WriteStatus::FINISHING;
      Notify();
    }
  }

 private:
  void ProceedToAccept(bool ok) {
    std::unique_ptr<ServerResponseWriterHandler> p;
    std::unique_lock<std::mutex> lock(mutex_);

    SPDLOG_TRACE("ProceedToAccept: ok={}", ok);

    // サーバのシャットダウン要求が来てたら次の Accept 待ちをしない
    if (shutdown_requested_()) {
      write_status_ = WriteStatus::FINISHED;
      p.reset(this);
      return;
    }

    // Accept の失敗も次の Accept 待ちをしない
    if (!ok) {
      SPDLOG_ERROR("Accept failed");
      write_status_ = WriteStatus::FINISHED;
      p.reset(this);
      return;
    }

    // 次の要求に備える
    gen_handler_(cq_);

    write_status_ = WriteStatus::IDLE;

    lock.unlock();
    try {
      OnAccept(std::move(request_));
      lock.lock();
    } catch (...) {
      lock.lock();
    }
  }

  void ProceedToWrite(bool ok) {
    std::unique_ptr<ServerResponseWriterHandler> p;
    std::unique_lock<std::mutex> lock(mutex_);

    SPDLOG_TRACE("ProceedToWrite: ok={}", ok);

    if (shutdown_requested_()) {
      write_status_ = WriteStatus::FINISHED;
      p.reset(this);
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("write failed");
      write_status_ = WriteStatus::FINISHED;
      p.reset(this);
      return;
    }

    write_status_ = WriteStatus::FINISHED;
    p.reset(this);
  }

  void ProceedToNotify(bool ok) {
    std::unique_ptr<ServerResponseWriterHandler> p;
    std::lock_guard<std::mutex> guard(mutex_);

    SPDLOG_TRACE("ProceedToNotify: ok={}", ok);

    if (shutdown_requested_()) {
      write_status_ = WriteStatus::FINISHED;
      p.reset(this);
      return;
    }

    if (!ok) {
      SPDLOG_WARN("Alarm cancelled");
      write_status_ = WriteStatus::FINISHED;
      p.reset(this);
      return;
    }

    if (!response_.error) {
      response_writer_.Finish(std::move(response_.response), response_.status,
                              &writer_thunk_);
    } else {
      response_writer_.FinishWithError(response_.status, &writer_thunk_);
    }
    write_status_ = WriteStatus::FINISHING;
  }

  virtual void Request(grpc::ServerContext* context, R* request,
                       grpc::ServerAsyncResponseWriter<W>* response_writer,
                       grpc::ServerCompletionQueue* cq, void* tag) = 0;
  virtual void OnAccept(R request) = 0;
};

template <class W, class R>
class ServerReaderWriterHandler {
  std::mutex mutex_;
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

  grpc::Alarm notify_alarm_;
  void Notify() {
    notify_alarm_.Set(cq_, gpr_time_0(GPR_TIMESPAN), &notifier_thunk_);
  }

 public:
  ServerReaderWriterHandler()
      : streamer_(&server_context_),
        acceptor_thunk_(this),
        reader_thunk_(this),
        writer_thunk_(this),
        notifier_thunk_(this) {}
  virtual ~ServerReaderWriterHandler() {
    SPDLOG_TRACE("[0x{}] deleted", (void*)this);
  }

  typedef W WriteType;
  typedef R ReadType;

  void Init(std::function<bool()> shutdown_requested,
            grpc::ServerCompletionQueue* cq,
            std::function<void(grpc::ServerCompletionQueue*)> gen_handler) {
    shutdown_requested_ = std::move(shutdown_requested);
    cq_ = cq;
    gen_handler_ = std::move(gen_handler);
    Request(&server_context_, &streamer_, cq_, &acceptor_thunk_);
  }

  void Write(W resp) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_requested_()) {
      return;
    }

    SPDLOG_TRACE("[0x{}] Write: {}", (void*)this, resp.DebugString());

    if (write_status_ == WriteStatus::IDLE) {
      ResponseData d;
      d.finish = false;
      d.response = std::move(resp);

      write_status_ = WriteStatus::WRITING;
      response_queue_.push_back(std::move(d));
      Notify();
    } else if (write_status_ == WriteStatus::WRITING) {
      ResponseData d;
      d.finish = false;
      d.response = std::move(resp);

      response_queue_.push_back(std::move(d));
    }
  }

  void Finish(grpc::Status status) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_requested_()) {
      return;
    }

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
  void FinishRead() {
    if (read_status_ == ReadStatus::LISTENING ||
        read_status_ == ReadStatus::READING) {
      server_context_.TryCancel();
    } else {
      read_status_ = ReadStatus::FINISHED;
    }
  }
  void FinishWrite() {
    if (write_status_ == WriteStatus::LISTENING ||
        write_status_ == WriteStatus::WRITING ||
        write_status_ == WriteStatus::FINISHING) {
      server_context_.TryCancel();
    } else {
      write_status_ = WriteStatus::FINISHED;
    }
  }

  bool Deletable() {
    return read_status_ == ReadStatus::FINISHED &&
           write_status_ == WriteStatus::FINISHED;
  }

  void ProceedToAccept(bool ok) {
    std::unique_ptr<ServerReaderWriterHandler> p;
    std::unique_lock<std::mutex> lock(mutex_);

    SPDLOG_TRACE("[0x{}] ProceedToAccept: ok={}", (void*)this, ok);

    // サーバのシャットダウン要求が来てたら次の Accept 待ちをしない
    if (shutdown_requested_()) {
      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    // Accept の失敗も次の Accept 待ちをしない
    if (!ok) {
      SPDLOG_ERROR("Accept failed");
      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    // 次の要求に備える
    gen_handler_(cq_);

    write_status_ = WriteStatus::IDLE;

    lock.unlock();
    try {
      OnAccept();
      lock.lock();
    } catch (...) {
      lock.lock();
    }

    read_status_ = ReadStatus::READING;
    streamer_.Read(&request_, &reader_thunk_);
  }

  void ProceedToRead(bool ok) {
    std::unique_ptr<ServerReaderWriterHandler> p;
    std::unique_lock<std::mutex> lock(mutex_);

    SPDLOG_TRACE("[0x{}] ProceedToRead: ok={}", (void*)this, ok);

    if (shutdown_requested_()) {
      SPDLOG_DEBUG("shutdown requested");
      read_status_ = ReadStatus::FINISHED;
      FinishWrite();
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    if (!ok) {
      // 読み込みがすべて完了した（あるいは失敗した）
      // あとは書き込み処理が終わるのを待つだけ
      lock.unlock();
      try {
        OnReadDoneOrError();
        lock.lock();
      } catch (...) {
        lock.lock();
      }

      read_status_ = ReadStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    lock.unlock();
    try {
      OnRead(std::move(request_));
      lock.lock();
    } catch (...) {
      lock.lock();
    }

    // 次の読み込み
    streamer_.Read(&request_, &reader_thunk_);
  }

  void ProceedToWrite(bool ok) {
    std::unique_ptr<ServerReaderWriterHandler> p;
    std::unique_lock<std::mutex> lock(mutex_);

    SPDLOG_TRACE("[0x{}] ProceedToWrite: ok={}", (void*)this, ok);

    if (shutdown_requested_()) {
      SPDLOG_DEBUG("shutdown requested");
      FinishRead();
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("write failed");
      FinishRead();
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    if (write_status_ == WriteStatus::IDLE) {
    } else if (write_status_ == WriteStatus::WRITING) {
      response_queue_.pop_front();
      if (response_queue_.empty()) {
        write_status_ = WriteStatus::IDLE;
      } else {
        auto& d = response_queue_.front();
        if (!d.finish) {
          // Response
          streamer_.Write(std::move(d.response), &writer_thunk_);
        } else {
          // Finish
          streamer_.Finish(d.status, &writer_thunk_);
          write_status_ = WriteStatus::FINISHING;
        }
      }
    } else if (write_status_ == WriteStatus::FINISHING) {
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
    }
  }

  void ProceedToNotify(bool ok) {
    std::unique_ptr<ServerReaderWriterHandler> p;
    std::lock_guard<std::mutex> guard(mutex_);

    SPDLOG_TRACE("[0x{}] ProceedToNotify: ok={}", (void*)this, ok);

    if (shutdown_requested_()) {
      SPDLOG_DEBUG("shutdown requested");
      FinishRead();
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    if (!ok) {
      SPDLOG_WARN("Alarm cancelled");
      if (read_status_ == ReadStatus::LISTENING ||
          read_status_ == ReadStatus::READING) {
        server_context_.TryCancel();
      } else {
        read_status_ = ReadStatus::FINISHED;
      }
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    if (response_queue_.empty()) {
      SPDLOG_WARN("response_queue_ is empty");
      FinishRead();
      write_status_ = WriteStatus::FINISHED;
      if (Deletable()) {
        p.reset(this);
      }
      return;
    }

    auto& d = response_queue_.front();
    if (!d.finish) {
      // Response
      streamer_.Write(std::move(d.response), &writer_thunk_);
    } else {
      // Finish
      streamer_.Finish(d.status, &writer_thunk_);
      write_status_ = WriteStatus::FINISHING;
    }
  }

 private:
  // Success を呼ばずに return した場合、必ず Finish を呼ぶガード
  struct FinishGuard {
    ServerReaderWriterHandler* p;
    FinishGuard(ServerReaderWriterHandler* p) : p(p) {}
    ~FinishGuard() {
      if (p) {
        p->Finish(grpc::Status::CANCELLED);
      }
    }
    void Success() { p = nullptr; }
  };

 public:
  virtual void Request(grpc::ServerContext* context,
                       grpc::ServerAsyncReaderWriter<W, R>* streamer,
                       grpc::ServerCompletionQueue* cq, void* tag) = 0;
  virtual void OnAccept() = 0;
  virtual void OnRead(R req) = 0;
  virtual void OnReadDoneOrError() = 0;
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
 public:
  Server() {}
  ~Server() { Shutdown(); }

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
    auto shutdown_requested = [this]() -> bool { return shutdown_; };

    std::unique_ptr<GenHandler> gh(new GenHandler());
    gh->gen_handler =
        [shutdown_requested = std::move(shutdown_requested), gh = gh.get(),
         targs = std::move(targs)](grpc::ServerCompletionQueue* cq) {
          H* handler = new_from_tuple<H>(std::move(targs));
          handler->Init(shutdown_requested, cq, gh->gen_handler);
        };

    gen_handlers_.push_back(std::move(gh));
  }

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
    auto shutdown_requested = [this]() -> bool { return shutdown_; };

    std::unique_ptr<GenHandler> gh(new GenHandler());
    gh->gen_handler =
        [shutdown_requested = std::move(shutdown_requested), gh = gh.get(),
         targs = std::move(targs)](grpc::ServerCompletionQueue* cq) {
          H* handler = new_from_tuple<H>(std::move(targs));
          handler->Init(shutdown_requested, cq, gh->gen_handler);
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
        lock.lock();
      } catch (...) {
        lock.lock();
      }
    }

    SPDLOG_INFO("Server Shutdown completed");
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

  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  std::vector<std::unique_ptr<std::thread>> threads_;
  std::unique_ptr<grpc::Server> server_;
  std::mutex mutex_;

  struct GenHandler {
    std::function<void(grpc::ServerCompletionQueue*)> gen_handler;
  };
  std::vector<std::unique_ptr<GenHandler>> gen_handlers_;

  std::atomic<bool> shutdown_{false};
};

}  // namespace ggrpc

#endif // GGRPC_SERVER_H_INCLUDED
