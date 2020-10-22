#!/bin/bash

SOURCE_DIR="`pwd`/_source"
BUILD_DIR="`pwd`/_build"
INSTALL_DIR="`pwd`/_install"

set -ex

mkdir -p $SOURCE_DIR
mkdir -p $BUILD_DIR
mkdir -p $INSTALL_DIR

CMAKE_VERSION="3.16.2"
CMAKE_VERSION_FILE="$INSTALL_DIR/cmake.version"
CMAKE_CHANGED=0
if [ ! -e $CMAKE_VERSION_FILE -o "$CMAKE_VERSION" != "`cat $CMAKE_VERSION_FILE`" ]; then
  CMAKE_CHANGED=1
fi

GRPC_VERSION="1.33.1"
GRPC_VERSION_FILE="$INSTALL_DIR/grpc.version"
GRPC_CHANGED=0
if [ ! -e $GRPC_VERSION_FILE -o "$GRPC_VERSION" != "`cat $GRPC_VERSION_FILE`" ]; then
  GRPC_CHANGED=1
fi

CLI11_VERSION="1.8.0"
CLI11_VERSION_FILE="$INSTALL_DIR/cli11.version"
CLI11_CHANGED=0
if [ ! -e $CLI11_VERSION_FILE -o "$CLI11_VERSION" != "`cat $CLI11_VERSION_FILE`" ]; then
  CLI11_CHANGED=1
fi

SPDLOG_VERSION="1.8.1"
SPDLOG_VERSION_FILE="$INSTALL_DIR/spdlog.version"
SPDLOG_CHANGED=0
if [ ! -e $SPDLOG_VERSION_FILE -o "$SPDLOG_VERSION" != "`cat $SPDLOG_VERSION_FILE`" ]; then
  SPDLOG_CHANGED=1
fi

if [ -z "$JOBS" ]; then
  set +e
  # Linux
  JOBS=`nproc 2>/dev/null`
  if [ -z "$JOBS" ]; then
    # macOS
    JOBS=`sysctl -n hw.logicalcpu_max 2>/dev/null`
    if [ -z "$JOBS" ]; then
      JOBS=1
    fi
  fi
  set -e
fi

# CMake が古いとビルド出来ないので、CMake のバイナリをダウンロードする
if [ $CMAKE_CHANGED -eq 1 -o ! -e $INSTALL_DIR/cmake/bin/cmake ]; then
  if [ "`uname`" = "Darwin" ]; then
    _URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-Darwin-x86_64.tar.gz
    _FILE=$SOURCE_DIR/cmake-${CMAKE_VERSION}-Darwin-x86_64.tar.gz
    _DIR=cmake-${CMAKE_VERSION}-Darwin-x86_64
    _INSTALL=$INSTALL_DIR/CMake.app
  else
    _URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-Linux-x86_64.tar.gz
    _FILE=$SOURCE_DIR/cmake-${CMAKE_VERSION}-Linux-x86_64.tar.gz
    _DIR=cmake-${CMAKE_VERSION}-Linux-x86_64
    _INSTALL=$INSTALL_DIR/cmake
  fi
  if [ ! -e $_FILE ]; then
    echo "file(DOWNLOAD $_URL $_FILE)" > $SOURCE_DIR/tmp.cmake
    cmake -P $SOURCE_DIR/tmp.cmake
    rm $SOURCE_DIR/tmp.cmake
  fi

  pushd $SOURCE_DIR
    rm -rf $_DIR
    cmake -E tar xf $_FILE
  popd

  rm -rf $_INSTALL
  mv $SOURCE_DIR/$_DIR $_INSTALL
fi
echo $CMAKE_VERSION > $CMAKE_VERSION_FILE

if [ "`uname`" = "Darwin" ]; then
  export PATH=$INSTALL_DIR/CMake.app/Contents/bin:$PATH
else
  export PATH=$INSTALL_DIR/cmake/bin:$PATH
fi

# grpc (cmake)
if [ $GRPC_CHANGED -eq 1 -o ! -e $INSTALL_DIR/grpc/lib/libgrpc++.a ]; then
  # gRPC のソース
  if [ ! -e $SOURCE_DIR/grpc/.git ]; then
    git clone https://github.com/grpc/grpc.git $SOURCE_DIR/grpc
  fi
  pushd $SOURCE_DIR/grpc
    git fetch
    git reset --hard v$GRPC_VERSION
    git submodule update -i --recursive
  popd

  for buildtype in release tsan asan debug; do
    case "$buildtype" in
      "release" )
        _POSTFIX=""
        _OPTS="
          -DCMAKE_BUILD_TYPE=Release \
        "
        ;;
      "tsan" )
        _POSTFIX="-tsan"
        _OPTS="
          -DCMAKE_BUILD_TYPE=Debug \
          -DCMAKE_C_FLAGS="-fsanitize=thread" \
          -DCMAKE_CXX_FLAGS="-fsanitize=thread" \
        "
        ;;
      "asan" )
        _POSTFIX="-asan"
        _OPTS="
          -DCMAKE_BUILD_TYPE=Debug \
          -DCMAKE_C_FLAGS="-fsanitize=address" \
          -DCMAKE_CXX_FLAGS="-fsanitize=address" \
        "
        ;;
      "debug" )
        _POSTFIX="-debug"
        _OPTS="
          -DCMAKE_BUILD_TYPE=Debug \
        "
        ;;
    esac

    rm -rf $BUILD_DIR/grpc-build$_POSTFIX
    mkdir -p $BUILD_DIR/grpc-build$_POSTFIX
    pushd $BUILD_DIR/grpc-build$_POSTFIX
      cmake $SOURCE_DIR/grpc \
        -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR/grpc$_POSTFIX \
        -DgRPC_BUILD_CSHARP_EXT=OFF \
        -DBENCHMARK_ENABLE_TESTING=0 \
        $_OPTS
      make -j$JOBS
      make install
    popd
  done
fi
echo $GRPC_VERSION > $GRPC_VERSION_FILE

# CLI11
if [ $CLI11_CHANGED -eq 1 -o  ! -e $INSTALL_DIR/CLI11/include ]; then
  rm -rf $INSTALL_DIR/CLI11
  git clone --branch v$CLI11_VERSION --depth 1 https://github.com/CLIUtils/CLI11.git $INSTALL_DIR/CLI11
fi
echo $CLI11_VERSION > $CLI11_VERSION_FILE

# spdlog
if [ $SPDLOG_CHANGED -eq 1 -o  ! -e $INSTALL_DIR/spdlog/include ]; then
  rm -rf $INSTALL_DIR/spdlog
  git clone --branch v$SPDLOG_VERSION --depth 1 https://github.com/gabime/spdlog.git $INSTALL_DIR/spdlog
fi
echo $SPDLOG_VERSION > $SPDLOG_VERSION_FILE
