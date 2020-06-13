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
};

template <class W, class R>
class ClientResponseReader {
 public:
  typedef std::function<std::unique_ptr<grpc::ClientAsyncResponseReader<R>>(
      grpc::ClientContext*, const W&, grpc::CompletionQueue*)>
      RequestFunc;
  typedef std::function<void(R, grpc::Status)> OnResponseFunc;
  typedef std::function<void(ClientResponseReaderError)> OnErrorFunc;

 private:
  detail::ReaderThunk<ClientResponseReader> reader_thunk_;
  friend class detail::ReaderThunk<ClientResponseReader>;

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
    detail::RunCallbackClient(lock, nesting_, std::move(funcname), std::move(f),
                              std::forward<Args>(args)...);
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
                    ClientResponseReaderError::FINISH);
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

}  // namespace ggrpc

#endif
