#ifndef GGRPC_IMPL_CLIENT_MANAGER_H_INCLUDED
#define GGRPC_IMPL_CLIENT_MANAGER_H_INCLUDED

#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

// gRPC
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>

// spdlog
#include <spdlog/spdlog.h>

#include "../alarm.h"
#include "../handler.h"
#include "client_reader.h"
#include "client_reader_writer.h"
#include "client_response_reader.h"
#include "client_writer.h"

namespace ggrpc {

class ClientManager;

class ClientManager {
  std::mutex mutex_;

  struct ThreadData {
    grpc::CompletionQueue cq;
    std::unique_ptr<std::thread> thread;
  };
  std::vector<ThreadData> threads_;

  uint32_t next_client_id_ = 0;
  bool shutdown_ = false;

  struct Holder {
    virtual ~Holder() {}
    virtual void Close() = 0;
    virtual bool Expired() = 0;
    virtual std::shared_ptr<void> Lock() = 0;

   protected:
    template <class T>
    static void CloseWP(std::weak_ptr<T> wp) {
      auto sp = wp.lock();
      if (sp) {
        sp->Close();
      }
    }
    template <class T>
    static std::shared_ptr<void> LockWP(std::weak_ptr<T> wp) {
      return wp.lock();
    }
  };
  template <class W, class R>
  struct ResponseReaderHolder : Holder {
    std::weak_ptr<ClientResponseReader<W, R>> wp;
    ResponseReaderHolder(std::shared_ptr<ClientResponseReader<W, R>> p)
        : wp(p) {}
    void Close() override { CloseWP(wp); }
    bool Expired() override { return wp.expired(); }
    std::shared_ptr<void> Lock() override { return LockWP(wp); }
  };
  template <class W, class R>
  struct ReaderHolder : Holder {
    std::weak_ptr<ClientReader<W, R>> wp;
    ReaderHolder(std::shared_ptr<ClientReader<W, R>> p) : wp(p) {}
    void Close() override { CloseWP(wp); }
    bool Expired() override { return wp.expired(); }
    std::shared_ptr<void> Lock() override { return LockWP(wp); }
  };
  template <class W, class R>
  struct WriterHolder : Holder {
    std::weak_ptr<ClientWriter<W, R>> wp;
    WriterHolder(std::shared_ptr<ClientWriter<W, R>> p) : wp(p) {}
    void Close() override { CloseWP(wp); }
    bool Expired() override { return wp.expired(); }
    std::shared_ptr<void> Lock() override { return LockWP(wp); }
  };
  template <class W, class R>
  struct ReaderWriterHolder : Holder {
    std::weak_ptr<ClientReaderWriter<W, R>> wp;
    ReaderWriterHolder(std::shared_ptr<ClientReaderWriter<W, R>> p) : wp(p) {}
    void Close() override { CloseWP(wp); }
    bool Expired() override { return wp.expired(); }
    std::shared_ptr<void> Lock() override { return LockWP(wp); }
  };
  struct AlarmHolder : Holder {
    std::weak_ptr<Alarm> wp;
    AlarmHolder(std::shared_ptr<Alarm> p) : wp(p) {}
    void Close() override { CloseWP(wp); }
    bool Expired() override { return wp.expired(); }
    std::shared_ptr<void> Lock() override { return LockWP(wp); }
  };
  std::vector<std::unique_ptr<Holder>> holders_;
  std::map<void*, Holder*> holders_map_;

 public:
  ClientManager(int threads) : threads_(threads) {}

  ~ClientManager() { Shutdown(); }

  void Start() {
    for (auto& th : threads_) {
      th.thread.reset(new std::thread([& cq = th.cq]() { ThreadRun(&cq); }));
    }
  }

 private:
  static void ThreadRun(grpc::CompletionQueue* cq) {
    void* got_tag;
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
    for (const auto& holder : holders_) {
      auto it = holders_map_.find(holder->Lock().get());
      if (it == holders_map_.end()) {
        continue;
      }
      it->second = nullptr;
    }
    auto it = holders_map_.begin();
    while (it != holders_map_.end()) {
      if (it->second == nullptr) {
        it = holders_map_.erase(it);
      } else {
        ++it;
      }
    }
  }

 public:
  void Shutdown() {
    std::lock_guard<std::mutex> guard(mutex_);

    if (shutdown_) {
      return;
    }
    shutdown_ = true;

    SPDLOG_TRACE("ClientManager::Shutdown started");

    for (auto&& holder : holders_) {
      holder->Close();
    }
    holders_.clear();
    holders_map_.clear();

    SPDLOG_TRACE("ClientManager::Shutdown all client closed");

    // まず通常のキューを Shutdown して、全てのスレッドが終了するのを待つ
    // コールバックの処理で無限ループしてるとかじゃない限りは終了するはず
    for (auto& th : threads_) {
      th.cq.Shutdown();
    }

    SPDLOG_TRACE("ClientManager::Shutdown cq shutdown completed");

    for (auto& th : threads_) {
      th.thread->join();
      th.thread = nullptr;
    }

    SPDLOG_TRACE("ClientManager::Shutdown finished");
  }

  template <class T>
  using OnStateChangeFunc =
      std::function<void(grpc::Channel*, std::chrono::system_clock::time_point,
                         std::shared_ptr<T>, bool&)>;

  template <class T>
  void NotifyOnStateChange(grpc::Channel* channel,
                           std::chrono::system_clock::time_point deadline,
                           std::shared_ptr<T> target,
                           OnStateChangeFunc<T> on_notify) {
    std::lock_guard<std::mutex> guard(mutex_);

    auto it = holders_map_.find(target.get());
    if (it == holders_map_.end()) {
      return;
    }
    auto client_id = next_client_id_++;
    auto cq = &threads_[client_id % threads_.size()].cq;
    channel->NotifyOnStateChange(
        channel->GetState(false), deadline, cq,
        new NotifyData<T>(this, channel, deadline, target.get(),
                          std::move(on_notify)));
  }

  template <class W, class R>
  std::shared_ptr<ClientResponseReader<W, R>> CreateResponseReader(
      typename ClientResponseReader<W, R>::ConnectFunc connect) {
    return Create<ClientResponseReader<W, R>, ResponseReaderHolder<W, R>>(
        std::move(connect));
  }

  template <class W, class R>
  std::shared_ptr<ClientReader<W, R>> CreateReader(
      typename ClientReader<W, R>::ConnectFunc connect) {
    return Create<ClientReader<W, R>, ReaderHolder<W, R>>(std::move(connect));
  }

  template <class W, class R>
  std::shared_ptr<ClientWriter<W, R>> CreateWriter(
      typename ClientWriter<W, R>::ConnectFunc connect) {
    return Create<ClientWriter<W, R>, WriterHolder<W, R>>(std::move(connect));
  }

  template <class W, class R>
  std::shared_ptr<ClientReaderWriter<W, R>> CreateReaderWriter(
      typename ClientReaderWriter<W, R>::ConnectFunc connect) {
    return Create<ClientReaderWriter<W, R>, ReaderWriterHolder<W, R>>(
        std::move(connect));
  }

  std::shared_ptr<Alarm> CreateAlarm() { return Create<Alarm, AlarmHolder>(); }

 private:
  template <class T, class H, class... Args>
  std::shared_ptr<T> Create(Args... args) {
    std::lock_guard<std::mutex> guard(mutex_);

    Collect();

    auto client_id = next_client_id_++;
    auto cq = &threads_[client_id % threads_.size()].cq;

    std::shared_ptr<T> p(new T(cq, std::forward<Args>(args)...),
                         [](T* p) { p->Release(); });
    auto holder = std::unique_ptr<Holder>(new H(p));
    holders_map_.insert(std::make_pair((void*)p.get(), holder.get()));
    holders_.push_back(std::move(holder));
    return p;
  }

  template <class T>
  struct NotifyData : Handler {
    ClientManager* cm;
    grpc::Channel* channel;
    std::chrono::system_clock::time_point deadline;
    void* p;
    OnStateChangeFunc<T> on_notify;
    NotifyData(ClientManager* cm, grpc::Channel* channel,
               std::chrono::system_clock::time_point deadline, void* p,
               OnStateChangeFunc<T> on_notify)
        : cm(cm),
          channel(channel),
          deadline(deadline),
          p(p),
          on_notify(std::move(on_notify)) {}
    void Proceed(bool ok) override { cm->ProceedToNotify<T>(ok, this); }
  };

  template <class T>
  void ProceedToNotify(bool ok, NotifyData<T>* p) {
    std::lock_guard<std::mutex> guard(mutex_);

    struct SafeDelete {
      NotifyData<T>* p;
      ~SafeDelete() { delete p; }
    } safe_delete = {p};

    auto it = holders_map_.find(p->p);
    if (it == holders_map_.end()) {
      return;
    }
    auto sp = std::static_pointer_cast<T>(it->second->Lock());
    if (sp == nullptr) {
      return;
    }
    bool repeated = false;
    p->on_notify(p->channel, p->deadline, sp, repeated);
    if (repeated) {
      auto client_id = next_client_id_++;
      auto cq = &threads_[client_id % threads_.size()].cq;
      p->channel->NotifyOnStateChange(p->channel->GetState(false), p->deadline,
                                      cq, p);
      safe_delete.p = nullptr;
    }
  }

};

}  // namespace ggrpc

#endif
