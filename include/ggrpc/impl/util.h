#ifndef GGRPC_IMPL_UTIL_H_INCLUDED
#define GGRPC_IMPL_UTIL_H_INCLUDED

#include <memory>
#include <mutex>
#include <string>
#include <utility>

// spdlog
#include <spdlog/spdlog.h>

#include "handler.h"

namespace ggrpc {
namespace detail {

template <class T>
struct AcceptorThunk : Handler {
  T* p;
  AcceptorThunk(T* p) : p(p) {}
  void Proceed(bool ok) override { p->ProceedToAccept(ok); }
};

template <class T>
struct ConnectorThunk : Handler {
  T* p;
  ConnectorThunk(T* p) : p(p) {}
  void Proceed(bool ok) override { p->ProceedToConnect(ok); }
};

template <class T>
struct ReaderThunk : Handler {
  T* p;
  ReaderThunk(T* p) : p(p) {}
  void Proceed(bool ok) override { p->ProceedToRead(ok); }
};

template <class T>
struct WriterThunk : Handler {
  T* p;
  WriterThunk(T* p) : p(p) {}
  void Proceed(bool ok) override { p->ProceedToWrite(ok); }
};

template <class T>
struct NotifierThunk : Handler {
  T* p;
  NotifierThunk(T* p) : p(p) {}
  void Proceed(bool ok) override { p->ProceedToNotify(ok); }
};

template <class F, class... Args>
void RunCallbackClient(std::unique_lock<std::mutex>& lock, int& nesting,
                       std::string funcname, F f, Args&&... args) {
  // 普通にコールバックするとデッドロックの可能性があるので
  // unlock してからコールバックする。
  // 再度ロックした時に状態が変わってる可能性があるので注意すること。
  if (f) {
    ++nesting;
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
    --nesting;
  }
}

template <class Context, class T, class MemF, class... Args>
void RunCallbackServer(std::unique_lock<std::mutex>& lock, int& nesting,
                       std::shared_ptr<Context>& tmp_context,
                       const std::shared_ptr<Context>& context,
                       std::string funcname, T* p, MemF mf, Args&&... args) {
  // コールバック中は tmp_context を有効にする
  if (nesting == 0) {
    tmp_context = context;
  }
  ++nesting;
  lock.unlock();
  try {
    SPDLOG_TRACE("call {}", funcname);
    (p->*mf)(std::forward<Args>(args)...);
  } catch (std::exception& e) {
    SPDLOG_ERROR("{} error: what={}", funcname, e.what());
  } catch (...) {
    SPDLOG_ERROR("{} error", funcname);
  }
  lock.lock();
  --nesting;

  if (nesting == 0) {
    auto ctx = std::move(tmp_context);
    ++nesting;
    lock.unlock();
    ctx.reset();
    lock.lock();
    --nesting;
  }
}

}  // namespace detail
}  // namespace ggrpc

#endif
