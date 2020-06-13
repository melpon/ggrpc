#ifndef GGRPC_IMPL_CLIENT_READER_WRITER_H_INCLUDED
#define GGRPC_IMPL_CLIENT_READER_WRITER_H_INCLUDED

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

class ClientManager;

enum class ClientReaderWriterError {
  CONNECT,
  READ,
  WRITE,
};

template <class W, class R>
class ClientReaderWriter {
 public:
  typedef std::function<std::unique_ptr<grpc::ClientAsyncReaderWriter<W, R>>(
      grpc::ClientContext*, grpc::CompletionQueue*, void*)>
      ConnectFunc;
  typedef std::function<void()> OnConnectFunc;
  typedef std::function<void(R)> OnReadFunc;
  typedef std::function<void(grpc::Status)> OnReadDoneFunc;
  typedef std::function<void(ClientReaderWriterError)> OnErrorFunc;
  typedef std::function<void(W, int64_t)> OnWriteFunc;
  typedef std::function<void()> OnWritesDoneFunc;

 private:
  detail::ConnectorThunk<ClientReaderWriter> connector_thunk_;
  friend class detail::ConnectorThunk<ClientReaderWriter>;

  detail::ReaderThunk<ClientReaderWriter> reader_thunk_;
  friend class detail::ReaderThunk<ClientReaderWriter>;

  detail::WriterThunk<ClientReaderWriter> writer_thunk_;
  friend class detail::WriterThunk<ClientReaderWriter>;

  // ClientAsyncReaderWriter よりも ClientContext
  // の方が寿命が長くなるようにしないといけないので、 必ず streamer_ より上に
  // context_ を定義すること
  grpc::ClientContext context_;
  std::unique_ptr<grpc::ClientAsyncReaderWriter<W, R>> streamer_;

  enum class ReadStatus {
    INIT,
    CONNECTING,
    READING,
    FINISHING,
    CANCELING,
    FINISHED
  };
  ReadStatus read_status_ = ReadStatus::INIT;
  enum class WriteStatus {
    INIT,
    CONNECTING,
    WRITING,
    IDLE,
    FINISHING,
    CANCELING,
    FINISHED
  };
  WriteStatus write_status_ = WriteStatus::INIT;

  struct RequestData {
    bool is_done;
    int64_t id;
    W request;
  };
  std::deque<RequestData> request_queue_;
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
  OnWriteFunc on_write_;
  OnWritesDoneFunc on_writes_done_;

  struct SafeDeleter {
    ClientReaderWriter* p;
    std::unique_lock<std::mutex> lock;
    SafeDeleter(ClientReaderWriter* p) : p(p), lock(p->mutex_) {}
    ~SafeDeleter() {
      bool del =
          p->release_ &&
          p->read_status_ == ClientReaderWriter::ReadStatus::FINISHED &&
          p->write_status_ == ClientReaderWriter::WriteStatus::FINISHED &&
          p->nesting_ == 0;
      lock.unlock();
      if (del) {
        delete p;
      }
    }
  };
  friend struct SafeDeleter;

  ClientReaderWriter(grpc::CompletionQueue* cq, ConnectFunc connect)
      : connector_thunk_(this),
        reader_thunk_(this),
        writer_thunk_(this),
        cq_(cq),
        connect_(std::move(connect)) {}

  ~ClientReaderWriter() { SPDLOG_TRACE("[0x{}] deleted", (void*)this); }

  // コピー、ムーブ禁止
  ClientReaderWriter(const ClientReaderWriter&) = delete;
  ClientReaderWriter(ClientReaderWriter&&) = delete;
  ClientReaderWriter& operator=(const ClientReaderWriter&) = delete;
  ClientReaderWriter& operator=(ClientReaderWriter&&) = delete;

 public:
  void SetOnConnect(OnConnectFunc on_connect) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_connect_ = std::move(on_connect);
  }
  void SetOnRead(OnReadFunc on_read) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_read_ = std::move(on_read);
  }
  void SetOnReadDone(OnReadDoneFunc on_read_done) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_read_done_ = std::move(on_read_done);
  }
  void SetOnWrite(OnWriteFunc on_write) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_write_ = std::move(on_write);
  }
  void SetOnWritesDone(OnWritesDoneFunc on_writes_done) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_writes_done_ = std::move(on_writes_done);
  }
  void SetOnError(OnErrorFunc on_error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    on_error_ = std::move(on_error);
  }

  void Connect() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (read_status_ != ReadStatus::INIT ||
        write_status_ != WriteStatus::INIT) {
      return;
    }
    read_status_ = ReadStatus::CONNECTING;
    write_status_ = WriteStatus::CONNECTING;
    streamer_ = connect_(&context_, cq_, &connector_thunk_);
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
    // 読み書き中だったらキャンセルされるまで待つ
    if (read_status_ == ReadStatus::CONNECTING ||
        read_status_ == ReadStatus::READING ||
        read_status_ == ReadStatus::FINISHING ||
        write_status_ == WriteStatus::CONNECTING ||
        write_status_ == WriteStatus::WRITING ||
        write_status_ == WriteStatus::FINISHING) {
      context_.TryCancel();
    }
    if (read_status_ == ReadStatus::CONNECTING ||
        read_status_ == ReadStatus::READING ||
        read_status_ == ReadStatus::FINISHING) {
      read_status_ = ReadStatus::CANCELING;
    } else {
      read_status_ = ReadStatus::FINISHED;
    }
    if (write_status_ == WriteStatus::CONNECTING ||
        write_status_ == WriteStatus::WRITING ||
        write_status_ == WriteStatus::FINISHING) {
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

    auto on_connect = std::move(on_connect_);
    auto on_read = std::move(on_read_);
    auto on_read_done = std::move(on_read_done_);
    auto on_write = std::move(on_write_);
    auto on_writes_done = std::move(on_writes_done_);
    auto on_error = std::move(on_error_);

    ++nesting_;
    lock.unlock();
    on_connect = nullptr;
    on_read = nullptr;
    on_read_done = nullptr;
    on_write = nullptr;
    on_writes_done = nullptr;
    on_error = nullptr;
    lock.lock();
    --nesting_;
  }

 public:
  void Write(W request, int64_t id = 0) {
    std::lock_guard<std::mutex> guard(mutex_);
    SPDLOG_TRACE("[0x{}] Write: request={}", (void*)this,
                 request.DebugString());

    if (write_status_ == WriteStatus::IDLE ||
        write_status_ == WriteStatus::INIT ||
        write_status_ == WriteStatus::CONNECTING ||
        write_status_ == WriteStatus::WRITING) {
      RequestData req;
      req.is_done = false;
      req.id = id;
      req.request = std::move(request);

      if (write_status_ == WriteStatus::IDLE) {
        streamer_->Write(req.request, &writer_thunk_);
        write_status_ = WriteStatus::WRITING;
      }

      request_queue_.push_back(std::move(req));
    }
  }

  void WritesDone() {
    std::lock_guard<std::mutex> guard(mutex_);

    if (write_status_ == WriteStatus::IDLE ||
        write_status_ == WriteStatus::INIT ||
        write_status_ == WriteStatus::CONNECTING ||
        write_status_ == WriteStatus::WRITING) {
      RequestData req;
      req.is_done = true;
      request_queue_.push_back(std::move(req));

      if (write_status_ == WriteStatus::IDLE) {
        streamer_->WritesDone(&writer_thunk_);
        write_status_ = WriteStatus::FINISHING;
      }
    }
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

    // read/write 両方が CONNECTING か CANCELING になることしか無いはず
    assert(read_status_ == ReadStatus::CONNECTING &&
               write_status_ == WriteStatus::CONNECTING ||
           read_status_ == ReadStatus::CANCELING &&
               write_status_ == WriteStatus::CANCELING);

    if (read_status_ == ReadStatus::CANCELING ||
        write_status_ == WriteStatus::CANCELING) {
      // 既に Close が呼ばれてるので終わる
      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    // 接続失敗
    if (!ok) {
      SPDLOG_ERROR("connection error");

      read_status_ = ReadStatus::FINISHED;
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnError", on_error_,
                  ClientReaderWriterError::CONNECT);
      Done(d.lock);
      return;
    }

    // 読み込み
    streamer_->Read(&response_, &reader_thunk_);
    read_status_ = ReadStatus::READING;

    HandleRequestQueue();

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
        streamer_->Finish(&grpc_status_, &reader_thunk_);
        read_status_ = ReadStatus::FINISHING;
      } else if (read_status_ == ReadStatus::FINISHING) {
        SPDLOG_ERROR("reading or finishing error");
        read_status_ = ReadStatus::FINISHED;
        RunCallback(d.lock, "OnError", on_error_,
                    ClientReaderWriterError::READ);
        Done(d.lock);
      }
      return;
    }

    if (read_status_ == ReadStatus::READING) {
      // 結果が取得できた
      auto resp = std::move(response_);

      // 次の読み込み
      streamer_->Read(&response_, &reader_thunk_);
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
      // まだ書き込み中ならキャンセルする
      if (write_status_ == WriteStatus::WRITING ||
          write_status_ == WriteStatus::FINISHING) {
        context_.TryCancel();
        write_status_ = WriteStatus::CANCELING;
      } else {
        write_status_ = WriteStatus::FINISHED;
      }
      RunCallback(d.lock, "OnReadDone", on_read_done_, grpc_status_);
      Done(d.lock);
    }
  }

  void ProceedToWrite(bool ok) {
    SafeDeleter d(this);
    SPDLOG_TRACE("[0x{}] ProceedToWrite: ok={}", (void*)this, ok);

    assert(write_status_ == WriteStatus::WRITING ||
           write_status_ == WriteStatus::FINISHING ||
           write_status_ == WriteStatus::CANCELING);
    assert(!request_queue_.empty());

    if (write_status_ == WriteStatus::CANCELING) {
      write_status_ = WriteStatus::FINISHED;
      Done(d.lock);
      return;
    }

    if (!ok) {
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnError", on_error_, ClientReaderWriterError::WRITE);
      Done(d.lock);
      return;
    }

    if (write_status_ == WriteStatus::FINISHING) {
      // 書き込み完了。
      // あとは読み込みが全て終了したら終わり。
      request_queue_.pop_front();
      write_status_ = WriteStatus::FINISHED;
      RunCallback(d.lock, "OnWritesDone", on_writes_done_);
      Done(d.lock);
      return;
    }

    auto req = std::move(request_queue_.front());
    request_queue_.pop_front();

    // 書き込みが成功したら次のキューを処理する
    HandleRequestQueue();

    RunCallback(d.lock, "OnWrite", on_write_, std::move(req.request), req.id);
  }

  void HandleRequestQueue() {
    if (request_queue_.empty()) {
      write_status_ = WriteStatus::IDLE;
    } else {
      auto& req = request_queue_.front();
      if (!req.is_done) {
        // 通常の書き込みリクエスト
        streamer_->Write(req.request, &writer_thunk_);
        write_status_ = WriteStatus::WRITING;
      } else {
        // 完了のリクエスト
        streamer_->WritesDone(&writer_thunk_);
        write_status_ = WriteStatus::FINISHING;
      }
    }
  }
};

}  // namespace ggrpc

#endif
