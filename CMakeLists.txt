cmake_minimum_required(VERSION 3.16)

set(_INSTALL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/_install")

if (ENABLE_TSAN)
  set(CMAKE_PREFIX_PATH "${_INSTALL_DIR}/grpc-tsan")
endif()

if (ENABLE_ASAN)
  set(CMAKE_PREFIX_PATH "${_INSTALL_DIR}/grpc-asan")
endif()

if (NOT ENABLE_TSAN AND NOT ENABLE_ASAN)
  if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_PREFIX_PATH "${_INSTALL_DIR}/grpc-debug")
  else()
    set(CMAKE_PREFIX_PATH "${_INSTALL_DIR}/grpc")
  endif()
endif()

project(ggrpc C CXX)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(utf8_range REQUIRED)
find_package(CLI11 REQUIRED)
find_package(Spdlog REQUIRED)

# --- ggrpc.pb.cc などの生成

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/proto")
add_custom_command(
  OUTPUT
    "${CMAKE_CURRENT_BINARY_DIR}/proto/ggrpc.pb.cc"
    "${CMAKE_CURRENT_BINARY_DIR}/proto/ggrpc.pb.h"
    "${CMAKE_CURRENT_BINARY_DIR}/proto/ggrpc.grpc.pb.cc"
    "${CMAKE_CURRENT_BINARY_DIR}/proto/ggrpc.grpc.pb.h"
  COMMAND $<TARGET_FILE:protobuf::protoc>
  ARGS
    --grpc_out "${CMAKE_CURRENT_BINARY_DIR}/proto"
    --cpp_out "${CMAKE_CURRENT_BINARY_DIR}/proto"
    -I "${CMAKE_CURRENT_SOURCE_DIR}/proto"
    --plugin=protoc-gen-grpc="$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    "${CMAKE_CURRENT_SOURCE_DIR}/proto/ggrpc.proto"
  DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/proto/ggrpc.proto")

set(GGRPC_PROTO
  "${CMAKE_CURRENT_BINARY_DIR}/proto/ggrpc.pb.cc"
  "${CMAKE_CURRENT_BINARY_DIR}/proto/ggrpc.grpc.pb.cc")

# ---- テスト生成

enable_testing()
add_executable(ggrpc_test test/ggrpc_test.cpp ${GGRPC_PROTO})

# tsan
if (ENABLE_TSAN)
  target_compile_options(ggrpc_test PRIVATE -g -fsanitize=thread -fsanitize=undefined)
  target_link_options(ggrpc_test PRIVATE -g -fsanitize=thread -fsanitize=undefined)
endif()

# asan
if (ENABLE_ASAN)
  target_compile_options(ggrpc_test PRIVATE -g -fsanitize=address -fsanitize=undefined)
  target_link_options(ggrpc_test PRIVATE -g -fsanitize=address -fsanitize=undefined)
endif()

add_test(NAME ggrpc_test COMMAND $<TARGET_FILE:ggrpc_test>)
target_compile_definitions(ggrpc_test PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)
set_target_properties(ggrpc_test PROPERTIES CXX_STANDARD 17 C_STANDARD 99)
target_include_directories(ggrpc_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_include_directories(ggrpc_test PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/proto")
target_link_libraries(ggrpc_test
  gRPC::grpc++
  protobuf::libprotobuf
  gRPC::utf8_range_lib
  utf8_range::utf8_validity
  absl::cord
  absl::cord_internal
  Spdlog::Spdlog)
