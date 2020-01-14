#ifndef GGRPC_GRPC_CLIENT
#define GGRPC_GRPC_CLIENT

namespace ggrpc {

struct Handler {
  virtual void Proceed(bool ok) = 0;
};

}  // namespace ggrpc
