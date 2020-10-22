#ifndef GGRPC_IMPL_SERVER_READER_WRITER_H_INCLUDED
#define GGRPC_IMPL_SERVER_READER_WRITER_H_INCLUDED

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

  detail::AcceptorThunk<ServerReaderWriterHandler> acceptor_thunk_;
  friend class detail::AcceptorThunk<ServerReaderWriterHandler>;

  detail::ReaderThunk<ServerReaderWriterHandler> reader_thunk_;
  friend class detail::ReaderThunk<ServerReaderWriterHandler>;

  detail::WriterThunk<ServerReaderWriterHandler> writer_thunk_;
  friend class detail::WriterThunk<ServerReaderWriterHandler>;

  detail::NotifierThunk<ServerReaderWriterHandler> notifier_thunk_;
  friend class detail::NotifierThunk<ServerReaderWriterHandler>;

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

    SPDLOG_TRACE("[0x{}] DoClose: read_status={}, write_status={}, nesting={}",
                 (void*)this, (int)read_status_, (int)write_status_, nesting_);

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

    //SPDLOG_TRACE("[0x{}] Done start", (void*)this);

    auto context = std::move(context_);
    context_ = nullptr;

    ++nesting_;
    lock.unlock();
    // 誰も参照してなければここで Release() が呼ばれる
    context.reset();
    lock.lock();
    --nesting_;

    //SPDLOG_TRACE("[0x{}] Done end", (void*)this);
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

    SPDLOG_TRACE(
        "[0x{}] ProceedToAccept: ok={}, read_status={}, write_status={}",
        (void*)this, ok, (int)read_status_, (int)write_status_);

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

    read_status_ = ReadStatus::READING;
    write_status_ = WriteStatus::IDLE;

    streamer_.Read(&request_, &reader_thunk_);

    //SPDLOG_TRACE("[0x{}] start gen_handler: read_status={}, write_status={}",
    //             (void*)this, (int)read_status_, (int)write_status_);

    // 次の要求に備える
    d.lock.unlock();
    gen_handler_(cq_);
    d.lock.lock();

    //SPDLOG_TRACE("[0x{}] end gen_handler: read_status={}, write_status={}",
    //             (void*)this, (int)read_status_, (int)write_status_);

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
      if (read_status_ == ReadStatus::READING) {
        // まだ読込中ならキャンセルする
        read_status_ = ReadStatus::CANCELING;
        server_context_.TryCancel();
      }
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

}  // namespace ggrpc

#endif
