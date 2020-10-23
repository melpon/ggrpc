#ifndef GGRPC_IMPL_CLIENT_RESPONSE_REDER_H_INCLUDED
#define GGRPC_IMPL_CLIENT_RESPONSE_REDER_H_INCLUDED

#include <functional>
#include <memory>
#include <mutex>
#include <string>

// gRPC
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>

// spdlog
#include <spdlog/spdlog.h>

#include "../handler.h"
#include "../util.h"

namespace ggrpc {

class ClientManager;

enum class ClientResponseReaderError {
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
  typedef std::function<void(ClientResponseReaderError)> OnErrorFunc;

 private:
  detail::ReaderThunk<ClientResponseReader> reader_thunk_;
  friend class detail::ReaderThunk<ClientResponseReader>;

  detail::NotifierThunk<ClientResponseReader> notifier_thunk_;
  friend class detail::NotifierThunk<ClientResponseReader>;

  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientAsyncResponseReader<R>> reader_;

  R response_;
  grpc::Status grpc_status_;

  enum class Status { INIT, CONNECTING, CANCELING, DONE };
  Status status_ = Status::INIT;
  bool release_ = false;
  int nesting_ = 0;
  std::mutex mutex_;

  grpc::CompletionQueue* cq_;

  ConnectFunc connect_;
  OnFinishFunc on_finish_;
  OnErrorFunc on_error_;

  grpc::Alarm timeout_alarm_;
  bool timeout_alarm_set_ = false;
  std::chrono::milliseconds timeout_ = std::chrono::milliseconds::zero();

  struct SafeDeleter {
    ClientResponseReader* p;
    std::unique_lock<std::mutex> lock;
    SafeDeleter(ClientResponseReader* p) : p(p), lock(p->mutex_) {}
    ~SafeDeleter() {
      bool del = p->release_ &&
                 p->status_ == ClientResponseReader::Status::DONE &&
                 p->timeout_alarm_set_ == false && p->nesting_ == 0;
      //SPDLOG_TRACE("del={}, release={}, status={}, timeout={}, nesting={}", del,
      //             p->release_, (int)p->status_, p->timeout_alarm_set_, p->nesting_);
      lock.unlock();
      if (del) {
        delete p;
      }
    }
  };
  friend struct SafeDeleter;

  ClientResponseReader(grpc::CompletionQueue* cq, ConnectFunc connect)
      : reader_thunk_(this),
        notifier_thunk_(this),
        cq_(cq),
        connect_(std::move(connect)) {}
  ~ClientResponseReader() {
    SPDLOG_TRACE("[0x{}] deleted ClientResponseReader", (void*)this);
  }

  // コピー、ムーブ禁止
  ClientResponseReader(const ClientResponseReader&) = delete;
  ClientResponseReader(ClientResponseReader&&) = delete;
  ClientResponseReader& operator=(const ClientResponseReader&) = delete;
  ClientResponseReader& operator=(ClientResponseReader&&) = delete;

 public:
  void SetOnFinish(OnFinishFunc on_finish) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (status_ == Status::DONE) {
      return;
    }
    on_finish_ = std::move(on_finish);
  }
  void SetOnError(OnErrorFunc on_error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (status_ != Status::INIT) {
      return;
    }
    on_error_ = std::move(on_error);
  }
  void SetTimeout(std::chrono::milliseconds timeout) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (status_ == Status::DONE) {
      return;
    }
    timeout_ = timeout;
  }

  void Connect(const W& connect) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (status_ != Status::INIT) {
      return;
    }
    status_ = Status::CONNECTING;

    reader_ = connect_(&context_, connect, cq_);
    reader_->Finish(&response_, &grpc_status_, &reader_thunk_);

    if (timeout_ != std::chrono::milliseconds::zero()) {
      auto timepoint = std::chrono::system_clock::now() + timeout_;
      timeout_alarm_.Set(cq_, timepoint, &notifier_thunk_);
      timeout_alarm_set_ = true;
    }
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
    if (status_ == Status::CONNECTING) {
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

    auto on_finish = std::move(on_finish_);
    auto on_error = std::move(on_error_);
    on_finish_ = nullptr;
    on_error_ = nullptr;

    ++nesting_;
    lock.unlock();
    on_finish = nullptr;
    on_error = nullptr;
    lock.lock();
    --nesting_;
  }

 private:
  template <class F, class... Args>
  void RunCallback(std::unique_lock<std::mutex>& lock, std::string funcname,
                   F& f, Args&&... args) {
    detail::RunCallbackClient(lock, nesting_, std::move(funcname), f,
                              std::forward<Args>(args)...);
  }

  void ProceedToRead(bool ok) {
    SafeDeleter d(this);
    SPDLOG_TRACE(
        "[0x{}] ProceedToRead: ok={} status={} grpc_status={}, nesting={}",
        (void*)this, ok, (int)status_, grpc_status_.error_message(), nesting_);

    assert(status_ == Status::CONNECTING || status_ == Status::CANCELING);

    auto st = status_;
    status_ = Status::DONE;

    timeout_alarm_.Cancel();

    if (!ok) {
      SPDLOG_ERROR("finishing error");
      if (st == Status::CONNECTING) {
        RunCallback(d.lock, "OnError", on_error_,
                    ClientResponseReaderError::FINISH);
      }
      Done(d.lock);
      return;
    }

    // 結果が取得できた
    if (st == Status::CONNECTING) {
      RunCallback(d.lock, "OnFinish", on_finish_, std::move(response_),
                  std::move(grpc_status_));
    }
    Done(d.lock);
  }
  void ProceedToNotify(bool ok) {
    SafeDeleter d(this);
    SPDLOG_TRACE("[0x{}] ProceedToNotify: ok={} status={}", (void*)this, ok,
                 (int)status_);

    assert(status_ == Status::CONNECTING || status_ == Status::CANCELING ||
           status_ == Status::DONE);

    timeout_alarm_set_ = false;

    if (!ok) {
      Done(d.lock);
      return;
    }

    if (status_ == Status::CONNECTING) {
      context_.TryCancel();
      status_ = Status::CANCELING;
      RunCallback(d.lock, "OnError", on_error_,
                  ClientResponseReaderError::TIMEOUT);
    }
    Done(d.lock);
  }
};

}  // namespace ggrpc

#endif
