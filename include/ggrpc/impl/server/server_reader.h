#ifndef GGRPC_IMPL_SERVER_READER_H_INCLUDED
#define GGRPC_IMPL_SERVER_READER_H_INCLUDED

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

namespace ggrpc {

class Server;

enum class ServerReaderError {
  WRITE,
};

template <class W, class R>
class ServerReaderHandler;

template <class W, class R>
class ServerReaderContext {
  ServerReaderHandler<W, R>* p_;

  ServerReaderContext(ServerReaderHandler<W, R>* p);
  friend class Server;

 public:
  ~ServerReaderContext();
  Server* GetServer() const;
  void Finish(W resp, grpc::Status status);
  void FinishWithError(grpc::Status status);
  void Close();
};

template <class W, class R>
class ServerReaderHandler {
  mutable std::mutex mutex_;
  Server* server_;
  grpc::ServerCompletionQueue* cq_;

  grpc::ServerContext server_context_;
  grpc::ServerAsyncReader<W, R> reader_;

  std::function<void(grpc::ServerCompletionQueue*)> gen_handler_;

  R request_;
  struct ResponseData {
    bool error;
    W response;
    grpc::Status status;
  };
  ResponseData response_;

  struct AcceptorThunk : Handler {
    ServerReaderHandler* p;
    AcceptorThunk(ServerReaderHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToAccept(ok); }
  };
  AcceptorThunk acceptor_thunk_;
  friend class AcceptorThunk;

  struct ReaderThunk : Handler {
    ServerReaderHandler* p;
    ReaderThunk(ServerReaderHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToRead(ok); }
  };
  ReaderThunk reader_thunk_;
  friend class ReaderThunk;

  struct WriterThunk : Handler {
    ServerReaderHandler* p;
    WriterThunk(ServerReaderHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToWrite(ok); }
  };
  WriterThunk writer_thunk_;
  friend class WriterThunk;

  struct NotifierThunk : Handler {
    ServerReaderHandler* p;
    NotifierThunk(ServerReaderHandler* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToNotify(ok); }
  };
  NotifierThunk notifier_thunk_;
  friend class NotifierThunk;

  // 状態マシン
  enum class ReadStatus { LISTENING, READING, CANCELING, FINISHED };
  enum class WriteStatus { LISTENING, IDLE, FINISHING, CANCELING, FINISHED };
  ReadStatus read_status_ = ReadStatus::LISTENING;
  WriteStatus write_status_ = WriteStatus::LISTENING;

  bool release_ = false;
  int nesting_ = 0;

  grpc::Alarm alarm_;
  void Notify() { alarm_.Set(cq_, gpr_time_0(GPR_TIMESPAN), &notifier_thunk_); }

  std::shared_ptr<ServerReaderContext<W, R>> context_;
  std::shared_ptr<ServerReaderContext<W, R>> tmp_context_;

  struct SafeDeleter {
    ServerReaderHandler<W, R>* p;
    std::unique_lock<std::mutex> lock;
    SafeDeleter(ServerReaderHandler<W, R>* p) : p(p), lock(p->mutex_) {}
    ~SafeDeleter() {
      bool del =
          p->release_ &&
          p->read_status_ == ServerReaderHandler<W, R>::ReadStatus::FINISHED &&
          p->write_status_ ==
              ServerReaderHandler<W, R>::WriteStatus::FINISHED &&
          p->nesting_ == 0;
      lock.unlock();
      if (del) {
        delete p;
      }
    }
  };

 public:
  ServerReaderHandler()
      : reader_(&server_context_),
        acceptor_thunk_(this),
        reader_thunk_(this),
        writer_thunk_(this),
        notifier_thunk_(this) {}
  virtual ~ServerReaderHandler() {
    SPDLOG_TRACE("[0x{}] deleted ServerReaderHandler", (void*)this);
  }

 private:
  // ServerResponseWriterContext への参照が消えると自身の Release() が呼ばれるように仕込む
  // Close() や Finish() を呼ぶと自身の持つ context は nullptr になる
  // 誰かが参照してれば引き続き読み書きできる
  void Init(Server* server, grpc::ServerCompletionQueue* cq,
            std::function<void(grpc::ServerCompletionQueue*)> gen_handler,
            std::shared_ptr<ServerReaderContext<W, R>> context) {
    server_ = server;
    cq_ = cq;
    gen_handler_ = std::move(gen_handler);
    context_ = context;
    Request(&server_context_, &reader_, cq_, &acceptor_thunk_);
  }
  typedef W WriteType;
  typedef R ReadType;
  friend class Server;

  void Release() {
    SafeDeleter d(this);

    release_ = true;

    DoClose(d.lock);
  }
  friend class ServerReaderContext<W, R>;

  void Close() {
    SafeDeleter d(this);

    DoClose(d.lock);
  }

 public:
  // Close を呼ぶと context_ が nullptr になってしまうが、
  // コールバック中に Context() が無効になると使いにくいので、
  // コールバック中は tmp_context_ を設定しておいて、それが利用可能な場合はそちらを使う
  std::shared_ptr<ServerReaderContext<W, R>> Context() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return tmp_context_ ? tmp_context_ : context_;
  }

 private:
  void DoClose(std::unique_lock<std::mutex>& lock) {
    server_ = nullptr;

    // 読み書き中ならキャンセルする
    if (read_status_ == ReadStatus::READING ||
        write_status_ == WriteStatus::FINISHING) {
      server_context_.TryCancel();
      // 書き込み中ならアラームもキャンセルする
      if (write_status_ == WriteStatus::FINISHING) {
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

    if (write_status_ == WriteStatus::FINISHING ||
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

  void Finish(W resp, grpc::Status status) {
    std::lock_guard<std::mutex> guard(mutex_);
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

    if (write_status_ == WriteStatus::IDLE) {
      response_.error = true;
      response_.status = status;

      write_status_ = WriteStatus::FINISHING;
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
    reader_.Read(&request_, &reader_thunk_);

    write_status_ = WriteStatus::IDLE;

    RunCallback(d.lock, "OnAccept", &ServerReaderHandler::OnAccept);
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

    // 書き込み状態が FINISHING 以降だった場合は、
    // 読み込みに関するコールバックは呼ばずに終了する
    if (write_status_ == WriteStatus::FINISHING ||
        write_status_ == WriteStatus::FINISHED) {
      read_status_ = ReadStatus::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      // 読み込みがすべて完了した（あるいは失敗した）
      // あとは書き込み処理が終わるのを待つだけ
      read_status_ = ReadStatus::FINISHED;
      RunCallback(d.lock, "OnReadDoneOrError",
                  &ServerReaderHandler::OnReadDoneOrError);
      Done(d.lock);
      return;
    }

    // 次の読み込み
    reader_.Read(&request_, &reader_thunk_);

    RunCallback(d.lock, "OnRead", &ServerReaderHandler::OnRead,
                std::move(request_));
  }

  void ProceedToWrite(bool ok) {
    SafeDeleter d(this);

    SPDLOG_TRACE("[0x{}] ProceedToWrite: ok={}", (void*)this, ok);

    assert(write_status_ == WriteStatus::FINISHING ||
           write_status_ == WriteStatus::CANCELING);

    if (write_status_ == WriteStatus::CANCELING) {
      // 終了
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("write failed");
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnError", &ServerReaderHandler::OnError,
                  ServerReaderError::WRITE);
      Done(d.lock);
      return;
    }

    write_status_ = WriteStatus::FINISHED;
    if (read_status_ == ReadStatus::READING) {
      // まだ読込中ならキャンセルする
      read_status_ = ReadStatus::CANCELING;
      server_context_.TryCancel();
    }
    RunCallback(d.lock, "OnFinish", &ServerReaderHandler::OnFinish,
                std::move(response_.response), response_.status);
    Done(d.lock);
  }

  void ProceedToNotify(bool ok) {
    SafeDeleter d(this);

    SPDLOG_TRACE("[0x{}] ProceedToNotify: ok={}", (void*)this, ok);

    assert(write_status_ == WriteStatus::FINISHING ||
           write_status_ == WriteStatus::CANCELING);

    if (write_status_ == WriteStatus::CANCELING) {
      // 終了
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      SPDLOG_ERROR("alarm cancelled");
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnError", &ServerReaderHandler::OnError,
                  ServerReaderError::WRITE);
      Done(d.lock);
      return;
    }

    if (!response_.error) {
      reader_.Finish(response_.response, response_.status, &writer_thunk_);
    } else {
      reader_.FinishWithError(response_.status, &writer_thunk_);
    }
  }

 public:
  virtual void Request(grpc::ServerContext* context,
                       grpc::ServerAsyncReader<W, R>* streamer,
                       grpc::ServerCompletionQueue* cq, void* tag) = 0;
  virtual void OnAccept() {}
  virtual void OnRead(R req) {}
  virtual void OnReadDoneOrError() {}
  virtual void OnFinish(W response, grpc::Status status) {}
  virtual void OnError(ServerReaderError error) {}
};

template <class W, class R>
ServerReaderContext<W, R>::ServerReaderContext(
    ServerReaderHandler<W, R>* p)
    : p_(p) {}
template <class W, class R>
ServerReaderContext<W, R>::~ServerReaderContext() {
  p_->Release();
}
template <class W, class R>
Server* ServerReaderContext<W, R>::GetServer() const {
  return p_->GetServer();
}
template <class W, class R>
void ServerReaderContext<W, R>::Finish(W resp, grpc::Status status) {
  p_->Finish(std::move(resp), status);
}
template <class W, class R>
void ServerReaderContext<W, R>::FinishWithError(grpc::Status status) {
  p_->FinishWithError(status);
}
template <class W, class R>
void ServerReaderContext<W, R>::Close() {
  p_->Close();
}

}  // namespace ggrpc

#endif
