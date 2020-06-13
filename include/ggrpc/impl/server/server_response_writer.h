#ifndef GGRPC_IMPL_SERVER_RESPONSE_WRITER_H_INCLUDED
#define GGRPC_IMPL_SERVER_RESPONSE_WRITER_H_INCLUDED

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
  };
  ResponseData response_;

  detail::AcceptorThunk<ServerResponseWriterHandler> acceptor_thunk_;
  friend class detail::AcceptorThunk<ServerResponseWriterHandler>;

  detail::WriterThunk<ServerResponseWriterHandler> writer_thunk_;
  friend class detail::WriterThunk<ServerResponseWriterHandler>;

  detail::NotifierThunk<ServerResponseWriterHandler> notifier_thunk_;
  friend class detail::NotifierThunk<ServerResponseWriterHandler>;

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
    detail::RunCallbackServer(lock, nesting_, tmp_context_, context_,
                              std::move(funcname), this, mf,
                              std::forward<Args>(args)...);
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

}  // namespace ggrpc

#endif // GGRPC_SERVER_H_INCLUDED
