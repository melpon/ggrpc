#ifndef GGRPC_IMPL_CLIENT_REDER_H_INCLUDED
#define GGRPC_IMPL_CLIENT_REDER_H_INCLUDED

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

enum class ClientReaderError {
  CONNECT,
  READ,
};

template <class W, class R>
class ClientReader {
 public:
  typedef std::function<std::unique_ptr<grpc::ClientAsyncReader<R>>(
      grpc::ClientContext*, const W&, grpc::CompletionQueue*, void*)>
      ConnectFunc;
  typedef std::function<void()> OnConnectFunc;
  typedef std::function<void(R)> OnReadFunc;
  typedef std::function<void(grpc::Status)> OnReadDoneFunc;
  typedef std::function<void(ClientReaderError)> OnErrorFunc;

 private:
  detail::ConnectorThunk<ClientReader> connector_thunk_;
  friend class detail::ConnectorThunk<ClientReader>;

  detail::ReaderThunk<ClientReader> reader_thunk_;
  friend class detail::ReaderThunk<ClientReader>;

  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientAsyncReader<R>> reader_;

  enum class ReadStatus {
    INIT,
    CONNECTING,
    READING,
    FINISHING,
    CANCELING,
    FINISHED
  };
  ReadStatus read_status_ = ReadStatus::INIT;

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

  struct SafeDeleter {
    ClientReader* p;
    std::unique_lock<std::mutex> lock;
    SafeDeleter(ClientReader* p) : p(p), lock(p->mutex_) {}
    ~SafeDeleter() {
      bool del = p->release_ &&
                 p->read_status_ == ClientReader::ReadStatus::FINISHED &&
                 p->nesting_ == 0;
      lock.unlock();
      if (del) {
        delete p;
      }
    }
  };
  friend struct SafeDeleter;

  ClientReader(grpc::CompletionQueue* cq, ConnectFunc connect)
      : connector_thunk_(this),
        reader_thunk_(this),
        cq_(cq),
        connect_(std::move(connect)) {}

  ~ClientReader() { SPDLOG_TRACE("[0x{}] deleted", (void*)this); }

  // コピー、ムーブ禁止
  ClientReader(const ClientReader&) = delete;
  ClientReader(ClientReader&&) = delete;
  ClientReader& operator=(const ClientReader&) = delete;
  ClientReader& operator=(ClientReader&&) = delete;

 public:
  void SetOnConnect(OnConnectFunc on_connect) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT) {
      return;
    }
    on_connect_ = std::move(on_connect);
  }
  void SetOnRead(OnReadFunc on_read) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT) {
      return;
    }
    on_read_ = std::move(on_read);
  }
  void SetOnReadDone(OnReadDoneFunc on_read_done) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT) {
      return;
    }
    on_read_done_ = std::move(on_read_done);
  }
  void SetOnError(OnErrorFunc on_error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT) {
      return;
    }
    on_error_ = std::move(on_error);
  }

  void Connect(const W& request) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT) {
      return;
    }
    read_status_ = ReadStatus::CONNECTING;
    reader_ = connect_(&context_, request, cq_, &connector_thunk_);
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
    // 読み込み中だったらキャンセルされるまで待つ
    if (read_status_ == ReadStatus::CONNECTING ||
        read_status_ == ReadStatus::READING ||
        read_status_ == ReadStatus::FINISHING) {
      context_.TryCancel();
    }
    if (read_status_ == ReadStatus::CONNECTING ||
        read_status_ == ReadStatus::READING ||
        read_status_ == ReadStatus::FINISHING) {
      read_status_ = ReadStatus::CANCELING;
    } else {
      read_status_ = ReadStatus::FINISHED;
    }
    Done(lock);
  }

  void Done(std::unique_lock<std::mutex>& lock) {
    if (read_status_ != ReadStatus::FINISHED) {
      return;
    }

    auto on_connect = std::move(on_connect_);
    auto on_read = std::move(on_read_);
    auto on_read_done = std::move(on_read_done_);
    auto on_error = std::move(on_error_);

    ++nesting_;
    lock.unlock();
    on_connect = nullptr;
    on_read = nullptr;
    on_read_done = nullptr;
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

  void ProceedToConnect(bool ok) {
    SafeDeleter d(this);
    SPDLOG_TRACE("[0x{}] ProceedToConnect: ok={}", (void*)this, ok);

    // read が CONNECTING か CANCELING になることしか無いはず
    assert(read_status_ == ReadStatus::CONNECTING ||
           read_status_ == ReadStatus::CANCELING);

    if (read_status_ != ReadStatus::CONNECTING) {
      // 既に Close が呼ばれてるので終わる
      Done(d.lock);
      return;
    }

    // 接続失敗
    if (!ok) {
      SPDLOG_ERROR("connection error");

      read_status_ = ReadStatus::FINISHED;
      RunCallback(d.lock, "OnError", on_error_, ClientReaderError::CONNECT);
      Done(d.lock);
      return;
    }

    // 読み込み
    reader_->Read(&response_, &reader_thunk_);
    read_status_ = ReadStatus::READING;

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
        reader_->Finish(&grpc_status_, &reader_thunk_);
        read_status_ = ReadStatus::FINISHING;
      } else if (read_status_ == ReadStatus::FINISHING) {
        SPDLOG_ERROR("reading or finishing error");
        read_status_ = ReadStatus::FINISHED;
        RunCallback(d.lock, "OnError", on_error_, ClientReaderError::READ);
        Done(d.lock);
      }
      return;
    }

    if (read_status_ == ReadStatus::READING) {
      // 結果が取得できた
      auto resp = std::move(response_);

      // 次の読み込み
      reader_->Read(&response_, &reader_thunk_);
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
};

}  // namespace ggrpc

#endif
