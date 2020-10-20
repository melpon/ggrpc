#ifndef GGRPC_ALARM_H_INCLUDED
#define GGRPC_ALARM_H_INCLUDED

#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

// gRPC
#include <grpcpp/alarm.h>

#include "handler.h"

namespace ggrpc {

class ClientManager;
class Server;

class Alarm {
 public:
  typedef std::function<void(bool)> AlarmFunc;

 private:
  struct NotifierThunk : Handler {
    Alarm* p;
    NotifierThunk(Alarm* p) : p(p) {}
    void Proceed(bool ok) override { p->ProceedToNotify(ok); }
  };
  NotifierThunk notifier_thunk_;
  friend class NotifierThunk;

  grpc::CompletionQueue* cq_;

  bool close_ = false;
  bool release_ = false;
  int nesting_ = 0;
  std::mutex mutex_;

  grpc::Alarm alarm_;
  struct AlarmData {
    bool canceled = false;
    std::chrono::milliseconds deadline;
    AlarmFunc alarm;
  };
  std::deque<AlarmData> alarm_queue_;

  struct SafeDeleter {
    Alarm* p;
    std::unique_lock<std::mutex> lock;
    SafeDeleter(Alarm* p) : p(p), lock(p->mutex_) {}
    ~SafeDeleter() {
      bool del = p->release_ && p->alarm_queue_.empty() && p->nesting_ == 0;
      lock.unlock();
      if (del) {
        delete p;
      }
    }
  };
  friend struct SafeDeleter;

  Alarm(grpc::CompletionQueue* cq) : notifier_thunk_(this), cq_(cq) {}
  ~Alarm() { SPDLOG_TRACE("[0x{}] deleted", (void*)this); }

  // コピー、ムーブ禁止
  Alarm(const Alarm&) = delete;
  Alarm(Alarm&&) = delete;
  Alarm& operator=(const Alarm&) = delete;
  Alarm& operator=(Alarm&&) = delete;

 public:
  // アラームを設定する。
  // 既に設定済みのアラームがある場合、そのアラームはキャンセルされ、新しいアラームが設定される。
  bool Set(std::chrono::milliseconds deadline, AlarmFunc alarm) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (close_) {
      return false;
    }

    AlarmData data;
    data.deadline = deadline;
    data.alarm = alarm;

    if (alarm_queue_.empty()) {
      auto timepoint = std::chrono::system_clock::now() + deadline;
      alarm_.Set(cq_, timepoint, &notifier_thunk_);
    } else {
      DoCancel();
    }

    alarm_queue_.push_back(std::move(data));
    return true;
  }

 private:
  void Release() {
    SafeDeleter d(this);

    release_ = true;
    close_ = true;

    DoCancel();
  }
  friend class ClientManager;
  friend class Server;

 public:
  // アラームをキャンセルする
  void Cancel() {
    SafeDeleter d(this);

    DoCancel();
  }

  // 全ての設定済みアラームをキャンセルして、
  // Set を呼び出しても新しくアラームを設定できなくする
  void Close() {
    SafeDeleter d(this);

    SPDLOG_TRACE("Close Alarm");
    close_ = true;

    DoCancel();
  }

 private:
  void DoCancel() {
    if (alarm_queue_.empty()) {
      return;
    }

    // 全てキャンセル状態にする
    for (auto& data : alarm_queue_) {
      data.canceled = true;
    }
    // 現在設定されてるアラームをキャンセルする
    alarm_.Cancel();
  }

 private:
  template <class F, class... Args>
  void RunCallback(std::unique_lock<std::mutex>& lock, std::string funcname,
                   F& f, Args&&... args) {
    // 普通にコールバックするとデッドロックの可能性があるので
    // unlock してからコールバックする。
    // 再度ロックした時に状態が変わってる可能性があるので注意すること。
    if (f) {
      ++nesting_;
      lock.unlock();
      try {
        SPDLOG_TRACE("call {}", funcname);
        f(std::forward<Args>(args)...);
      } catch (std::exception& e) {
        SPDLOG_ERROR("{} error: what={}", funcname, e.what());
      } catch (...) {
        SPDLOG_ERROR("{} error", funcname);
      }
      f = nullptr;
      lock.lock();
      --nesting_;
    }
  }

  void ProceedToNotify(bool ok) {
    SafeDeleter d(this);

    assert(!alarm_queue_.empty());

    auto data = std::move(alarm_queue_.front());
    // タイムアウトかキャンセルかのコールバックを呼び出す
    RunCallback(d.lock, "OnAlarm", data.alarm, !(data.canceled || !ok));
    alarm_queue_.pop_front();
    // キューにあるアラームで、キャンセル済みのがあったらどんどんキャンセルしていく
    // クローズ済みだった場合は全てキャンセルする
    while (!alarm_queue_.empty() && (alarm_queue_.front().canceled || close_)) {
      data = std::move(alarm_queue_.front());
      RunCallback(d.lock, "OnAlarm", data.alarm, false);
      alarm_queue_.pop_front();
    }

    if (alarm_queue_.empty()) {
      return;
    }

    // 有効なアラームがあったので、次のアラームをセット
    auto timepoint =
        std::chrono::system_clock::now() + alarm_queue_.front().deadline;
    alarm_.Set(cq_, timepoint, &notifier_thunk_);
  }
};

}

#endif // GGRPC_ALARM_H_INCLUDED
