#!/bin/bash

set -ex

cd `dirname $0`
INSTALL_DIR="`pwd`/_install"
MODULE_PATH="`pwd`/cmake"
PROJECT_DIR="`pwd`"

export PATH=$INSTALL_DIR/cmake/bin:$PATH

mkdir -p _build/ggrpc
pushd _build/ggrpc
  cmake $PROJECT_DIR \
    -DOPENSSL_ROOT_DIR="$INSTALL_DIR/boringssl" \
    -DSPDLOG_ROOT_DIR="$INSTALL_DIR/spdlog" \
    -DCMAKE_PREFIX_PATH="$INSTALL_DIR/zlib;$INSTALL_DIR/cares;$INSTALL_DIR/protobuf;$INSTALL_DIR/grpc" \
    -DCMAKE_MODULE_PATH=$MODULE_PATH \
    -DCMAKE_INSTALL_PREFIX=$PROJECT_DIR/_install \
    -DCMAKE_BUILD_TYPE=Release \
    "$@"
  make ggrpc_test
  rm -rf /var/crash/*ggrpc_test.*.crash
  rm -rf crash/
  ./ggrpc_test || true
  if [ -e /var/crash/*ggrpc_test.*.crash ]; then
    apport-unpack /var/crash/*ggrpc_test.*.crash crash/
    gdb `cat crash/ExecutablePath` -c crash/CoreDump
  fi
popd
