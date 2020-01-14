#ifndef GGRPC_HANDLER_H_INCLUDED
#define GGRPC_HANDLER_H_INCLUDED

namespace ggrpc {

struct Handler {
  virtual void Proceed(bool ok) = 0;
};

}  // namespace ggrpc

#endif  // GGRPC_HANDLER_H_INCLUDED
