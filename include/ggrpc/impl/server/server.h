#ifndef GGRPC_IMPL_SERVER_H_INCLUDED
#define GGRPC_IMPL_SERVER_H_INCLUDED

#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

// gRPC
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>

// spdlog
#include <spdlog/spdlog.h>

#include "../alarm.h"
#include "../handler.h"
#include "server_reader.h"
#include "server_reader_writer.h"
#include "server_response_writer.h"
#include "server_writer.h"

namespace ggrpc {

namespace detail {

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

}  // namespace detail

class Server {
 public:
  Server() {}
  ~Server() { Shutdown(); }

  template <class H, class... Args>
  void AddResponseWriterHandler(Args... args) {
    typedef typename H::WriteType W;
    typedef typename H::ReadType R;

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

    std::unique_ptr<GenHandler> gh(new GenHandler());
    gh->gen_handler = [this, gh = gh.get(), targs = std::move(targs)](
                          grpc::ServerCompletionQueue* cq) {
      std::lock_guard<std::mutex> guard(mutex_);

      H* handler = detail::new_from_tuple<H>(std::move(targs));
      auto context = std::shared_ptr<ServerResponseWriterContext<W, R>>(
          new ServerResponseWriterContext<W, R>(handler));
      handler->Init(this, cq, gh->gen_handler, context);
      Collect();
      holders_.push_back(
          std::unique_ptr<Holder>(new ResponseWriterHolder<W, R>(context)));
    };

    gen_handlers_.push_back(std::move(gh));
  }

  template <class H, class... Args>
  void AddWriterHandler(Args... args) {
    typedef typename H::WriteType W;
    typedef typename H::ReadType R;

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

    std::unique_ptr<GenHandler> gh(new GenHandler());
    gh->gen_handler = [this, gh = gh.get(), targs = std::move(targs)](
                          grpc::ServerCompletionQueue* cq) {
      std::lock_guard<std::mutex> guard(mutex_);

      H* handler = detail::new_from_tuple<H>(std::move(targs));
      auto context = std::shared_ptr<ServerWriterContext<W, R>>(
          new ServerWriterContext<W, R>(handler));
      handler->Init(this, cq, gh->gen_handler, context);
      Collect();
      holders_.push_back(
          std::unique_ptr<Holder>(new WriterHolder<W, R>(context)));
    };

    gen_handlers_.push_back(std::move(gh));
  }

  template <class H, class... Args>
  void AddReaderHandler(Args... args) {
    typedef typename H::WriteType W;
    typedef typename H::ReadType R;

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

    std::unique_ptr<GenHandler> gh(new GenHandler());
    gh->gen_handler = [this, gh = gh.get(), targs = std::move(targs)](
                          grpc::ServerCompletionQueue* cq) {
      std::lock_guard<std::mutex> guard(mutex_);

      H* handler = detail::new_from_tuple<H>(std::move(targs));
      auto context = std::shared_ptr<ServerReaderContext<W, R>>(
          new ServerReaderContext<W, R>(handler));
      handler->Init(this, cq, gh->gen_handler, context);
      Collect();
      holders_.push_back(
          std::unique_ptr<Holder>(new ReaderHolder<W, R>(context)));
    };

    gen_handlers_.push_back(std::move(gh));
  }

  template <class H, class... Args>
  void AddReaderWriterHandler(Args... args) {
    typedef typename H::WriteType W;
    typedef typename H::ReadType R;

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

    std::unique_ptr<GenHandler> gh(new GenHandler());
    gh->gen_handler = [this, gh = gh.get(), targs = std::move(targs)](
                          grpc::ServerCompletionQueue* cq) {
      std::lock_guard<std::mutex> guard(mutex_);

      H* handler = detail::new_from_tuple<H>(std::move(targs));
      auto context = std::shared_ptr<ServerReaderWriterContext<W, R>>(
          new ServerReaderWriterContext<W, R>(handler));
      handler->Init(this, cq, gh->gen_handler, context);
      Collect();
      holders_.push_back(
          std::unique_ptr<Holder>(new ReaderWriterHolder<W, R>(context)));
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

    // 全ての実行中のハンドラに対して Close() を呼ぶ
    for (auto& holder : holders_) {
      holder->Close();
    }
    holders_.clear();

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
      } catch (...) {
        SPDLOG_ERROR("Server Shutdown thread joining failed");
      }
      lock.lock();
    }

    SPDLOG_INFO("Server Shutdown completed");
  }

  std::shared_ptr<Alarm> CreateAlarm() {
    std::lock_guard<std::mutex> guard(mutex_);

    // Start してない
    if (threads_.size() == 0) {
      return nullptr;
    }
    if (shutdown_) {
      return nullptr;
    }

    Collect();

    auto id = next_id_++;
    auto cq = cqs_[id % cqs_.size()].get();

    std::shared_ptr<Alarm> p(new Alarm(cq), [](Alarm* p) { p->Release(); });
    holders_.push_back(std::unique_ptr<Holder>(new AlarmHolder(p)));
    return p;
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

  void Collect() {
    // expired な要素を削除する
    holders_.erase(std::remove_if(holders_.begin(), holders_.end(),
                                  [](const std::unique_ptr<Holder>& holder) {
                                    return holder->Expired();
                                  }),
                   holders_.end());
  }

  struct Holder {
    virtual ~Holder() {}
    virtual void Close() = 0;
    virtual bool Expired() = 0;
  };
  template <class W, class R>
  struct ResponseWriterHolder : Holder {
    std::weak_ptr<ServerResponseWriterContext<W, R>> wp;
    ResponseWriterHolder(std::shared_ptr<ServerResponseWriterContext<W, R>> p)
        : wp(p) {}
    void Close() override {
      auto sp = wp.lock();
      if (sp) {
        sp->Close();
      }
    }
    bool Expired() override { return wp.expired(); }
  };
  template <class W, class R>
  struct WriterHolder : Holder {
    std::weak_ptr<ServerWriterContext<W, R>> wp;
    WriterHolder(std::shared_ptr<ServerWriterContext<W, R>> p) : wp(p) {}
    void Close() override {
      auto sp = wp.lock();
      if (sp) {
        sp->Close();
      }
    }
    bool Expired() override { return wp.expired(); }
  };
  template <class W, class R>
  struct ReaderHolder : Holder {
    std::weak_ptr<ServerReaderContext<W, R>> wp;
    ReaderHolder(std::shared_ptr<ServerReaderContext<W, R>> p) : wp(p) {}
    void Close() override {
      auto sp = wp.lock();
      if (sp) {
        sp->Close();
      }
    }
    bool Expired() override { return wp.expired(); }
  };
  template <class W, class R>
  struct ReaderWriterHolder : Holder {
    std::weak_ptr<ServerReaderWriterContext<W, R>> wp;
    ReaderWriterHolder(std::shared_ptr<ServerReaderWriterContext<W, R>> p)
        : wp(p) {}
    void Close() override {
      auto sp = wp.lock();
      if (sp) {
        sp->Close();
      }
    }
    bool Expired() override { return wp.expired(); }
  };
  struct AlarmHolder : Holder {
    std::weak_ptr<Alarm> wp;
    AlarmHolder(std::shared_ptr<Alarm> p) : wp(p) {}
    void Close() override {
      auto sp = wp.lock();
      if (sp) {
        sp->Close();
      }
    }
    bool Expired() override { return wp.expired(); }
  };
  std::vector<std::unique_ptr<Holder>> holders_;

  std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
  std::vector<std::unique_ptr<std::thread>> threads_;
  std::unique_ptr<grpc::Server> server_;
  mutable std::mutex mutex_;

  struct GenHandler {
    std::function<void(grpc::ServerCompletionQueue*)> gen_handler;
  };
  std::vector<std::unique_ptr<GenHandler>> gen_handlers_;

  bool shutdown_ = false;
  int next_id_ = 0;
};

}  // namespace ggrpc

#endif
