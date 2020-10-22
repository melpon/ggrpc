#ifndef GGRPC_IMPL_SERVER_WRITER_H_INCLUDED
#define GGRPC_IMPL_SERVER_WRITER_H_INCLUDED

#include <deque>
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

enum class ServerWriterError {
  WRITE,
};

template <class W, class R>
class ServerWriterHandler;

template <class W, class R>
class ServerWriterContext {
  ServerWriterHandler<W, R>* p_;

  ServerWriterContext(ServerWriterHandler<W, R>* p);
  friend class Server;

 public:
  ~ServerWriterContext();
  Server* GetServer() const;
  void Write(W resp, int64_t id = 0);
  void Finish(grpc::Status status);
  void Close();
};

template <class W, class R>
class ServerWriterHandler {
  mutable std::mutex mutex_;
  Server* server_;
  grpc::ServerCompletionQueue* cq_;

  grpc::ServerContext server_context_;
  grpc::ServerAsyncWriter<W> writer_;

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

  detail::AcceptorThunk<ServerWriterHandler> acceptor_thunk_;
  friend class detail::AcceptorThunk<ServerWriterHandler>;

  detail::WriterThunk<ServerWriterHandler> writer_thunk_;
  friend class detail::WriterThunk<ServerWriterHandler>;

  detail::NotifierThunk<ServerWriterHandler> notifier_thunk_;
  friend class detail::NotifierThunk<ServerWriterHandler>;

  struct DoneThunk : Handler {
    ServerWriterHandler* p;
    DoneThunk(ServerWriterHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToDone(ok); }
  };
  DoneThunk done_thunk_;
  friend class DoneThunk;

  // 状態マシン
  enum class WriteStatus {
    LISTENING,
    WRITING,
    IDLE,
    FINISHING,
    CANCELING,
    FINISHED
  };
  WriteStatus write_status_ = WriteStatus::LISTENING;

  bool release_ = false;
  int nesting_ = 0;

  grpc::Alarm alarm_;
  void Notify() { alarm_.Set(cq_, gpr_time_0(GPR_TIMESPAN), &notifier_thunk_); }

  std::shared_ptr<ServerWriterContext<W, R>> context_;
  std::shared_ptr<ServerWriterContext<W, R>> tmp_context_;

  struct SafeDeleter {
    ServerWriterHandler<W, R>* p;
    std::unique_lock<std::mutex> lock;
    SafeDeleter(ServerWriterHandler<W, R>* p) : p(p), lock(p->mutex_) {}
    ~SafeDeleter() {
      bool del = p->release_ &&
                 p->write_status_ ==
                     ServerWriterHandler<W, R>::WriteStatus::FINISHED &&
                 p->nesting_ == 0;
      lock.unlock();
      if (del) {
        delete p;
      }
    }
  };

 public:
  ServerWriterHandler()
      : writer_(&server_context_),
        acceptor_thunk_(this),
        writer_thunk_(this),
        notifier_thunk_(this),
        done_thunk_(this) {}
  virtual ~ServerWriterHandler() {
    SPDLOG_TRACE("[0x{}] deleted ServerWriterHandler", (void*)this);
  }

 private:
  // ServerWriterContext への参照が消えると自身の Release() が呼ばれるように仕込む
  // Close() や Finish() を呼ぶと自身の持つ context は nullptr になる
  // 誰かが参照してれば引き続き読み書きできる
  void Init(Server* server, grpc::ServerCompletionQueue* cq,
            std::function<void(grpc::ServerCompletionQueue*)> gen_handler,
            std::shared_ptr<ServerWriterContext<W, R>> context) {
    server_ = server;
    cq_ = cq;
    gen_handler_ = std::move(gen_handler);
    context_ = context;
    server_context_.AsyncNotifyWhenDone(&done_thunk_);
    Request(&server_context_, &request_, &writer_, cq_, &acceptor_thunk_);
  }
  typedef W WriteType;
  typedef R ReadType;
  friend class Server;

  void Release() {
    SafeDeleter d(this);

    release_ = true;

    DoClose(d.lock);
  }
  friend class ServerWriterContext<W, R>;

  void Close() {
    SafeDeleter d(this);

    DoClose(d.lock);
  }

 public:
  // Close を呼ぶと context_ が nullptr になってしまうが、
  // コールバック中に Context() が無効になると使いにくいので、
  // コールバック中は tmp_context_ を設定しておいて、それが利用可能な場合はそちらを使う
  std::shared_ptr<ServerWriterContext<W, R>> Context() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return tmp_context_ ? tmp_context_ : context_;
  }

 private:
  void DoClose(std::unique_lock<std::mutex>& lock) {
    server_ = nullptr;

    // 読み書き中ならキャンセルする
    if (write_status_ == WriteStatus::IDLE ||
        write_status_ == WriteStatus::WRITING ||
        write_status_ == WriteStatus::FINISHING) {
      server_context_.TryCancel();
      alarm_.Cancel();
    }

    if (write_status_ == WriteStatus::IDLE ||
        write_status_ == WriteStatus::WRITING ||
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
    if (write_status_ != WriteStatus::FINISHED) {
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
    detail::RunCallbackServer(lock, nesting_, tmp_context_, context_,
                              std::move(funcname), this, mf,
                              std::forward<Args>(args)...);
  }

  void ProceedToAccept(bool ok) {
    SafeDeleter d(this);

    SPDLOG_TRACE("[0x{}] ProceedToAccept: ok={}", (void*)this, ok);

    assert(write_status_ == WriteStatus::LISTENING ||
           write_status_ == WriteStatus::CANCELING);

    if (write_status_ == WriteStatus::CANCELING) {
      // 終了
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    // Accept 失敗
    if (!ok) {
      SPDLOG_ERROR("Accept failed");
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    write_status_ = WriteStatus::IDLE;

    // 次の要求に備える
    d.lock.unlock();
    gen_handler_(cq_);
    d.lock.lock();

    RunCallback(d.lock, "OnAccept", &ServerWriterHandler::OnAccept,
                std::move(request_));
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
      RunCallback(d.lock, "OnError", &ServerWriterHandler::OnError,
                  ServerWriterError::WRITE);
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
          writer_.Write(next.response, &writer_thunk_);
        } else {
          // Finish
          writer_.Finish(next.status, &writer_thunk_);
          write_status_ = WriteStatus::FINISHING;
        }
      }
      RunCallback(d.lock, "OnWrite", &ServerWriterHandler::OnWrite,
                  std::move(resp.response), resp.id);
    } else if (write_status_ == WriteStatus::FINISHING) {
      assert(resp.finish);
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnFinish", &ServerWriterHandler::OnFinish,
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
      RunCallback(d.lock, "OnError", &ServerWriterHandler::OnError,
                  ServerWriterError::WRITE);
      Done(d.lock);
      return;
    }

    auto& data = response_queue_.front();
    if (!data.finish) {
      // Response
      writer_.Write(std::move(data.response), &writer_thunk_);
    } else {
      // Finish
      writer_.Finish(data.status, &writer_thunk_);
    }
  }
  void ProceedToDone(bool ok) {
    SafeDeleter d(this);
    SPDLOG_TRACE("[0x{}] ProceedToDone: ok={}, cancelled={}", (void*)this, ok,
                 server_context_.IsCancelled());
    if (write_status_ == WriteStatus::CANCELING) {
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
    }
  }

 public:
  virtual void Request(grpc::ServerContext* context, R* request,
                       grpc::ServerAsyncWriter<W>* writer,
                       grpc::ServerCompletionQueue* cq, void* tag) = 0;
  virtual void OnAccept(R request) {}
  virtual void OnWrite(W response, int64_t id) {}
  virtual void OnFinish(grpc::Status status) {}
  virtual void OnError(ServerWriterError error) {}
};

template <class W, class R>
ServerWriterContext<W, R>::ServerWriterContext(ServerWriterHandler<W, R>* p)
    : p_(p) {}
template <class W, class R>
ServerWriterContext<W, R>::~ServerWriterContext() {
  p_->Release();
}
template <class W, class R>
Server* ServerWriterContext<W, R>::GetServer() const {
  return p_->GetServer();
}
template <class W, class R>
void ServerWriterContext<W, R>::Write(W resp, int64_t id) {
  p_->Write(std::move(resp), id);
}
template <class W, class R>
void ServerWriterContext<W, R>::Finish(grpc::Status status) {
  p_->Finish(status);
}
template <class W, class R>
void ServerWriterContext<W, R>::Close() {
  p_->Close();
}

}  // namespace ggrpc

#endif
