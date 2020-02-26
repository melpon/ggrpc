#!/bin/bash

set -ex

cd `dirname $0`
INSTALL_DIR="`pwd`/_install"
MODULE_PATH="`pwd`/cmake"
PROJECT_DIR="`pwd`"

BUILD_DIR="_build/ggrpc"
ENABLE_TSAN=OFF
ENABLE_ASAN=OFF

while [ $# -ne 0 ]; do
  case "$1" in
    "--tsan" )
      ENABLE_TSAN=ON
      BUILD_DIR="${BUILD_DIR}-tsan"
      ;;
    "--asan" )
      ENABLE_ASAN=ON
      BUILD_DIR="${BUILD_DIR}-asan"
      ;;
  esac
  shift 1
done

export PATH=$INSTALL_DIR/cmake/bin:$PATH

mkdir -p $BUILD_DIR
pushd $BUILD_DIR
  cmake $PROJECT_DIR \
    -DSPDLOG_ROOT_DIR="$INSTALL_DIR/spdlog" \
    -DCMAKE_PREFIX_PATH="$INSTALL_DIR/grpc" \
    -DCMAKE_MODULE_PATH=$MODULE_PATH \
    -DCMAKE_INSTALL_PREFIX=$PROJECT_DIR/_install \
    -DENABLE_TSAN=$ENABLE_TSAN \
    -DENABLE_ASAN=$ENABLE_ASAN \
    "$@"
  make -j`nproc` ggrpc_test
  rm -rf /var/crash/*ggrpc_test.*.crash
  rm -rf crash/
  ./ggrpc_test || true
  if [ -e /var/crash/*ggrpc_test.*.crash ]; then
    apport-unpack /var/crash/*ggrpc_test.*.crash crash/
    gdb `cat crash/ExecutablePath` -c crash/CoreDump
  fi
popd
