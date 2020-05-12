cmake_minimum_required(VERSION 3.16)

set(_INSTALL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../_install")
list(APPEND CMAKE_PREFIX_PATH "${_INSTALL_DIR}/grpc")

project(helloworld C CXX)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# --- ggrpc.pb.cc などの生成

add_custom_command(
  OUTPUT
    "${CMAKE_CURRENT_BINARY_DIR}/helloworld.pb.cc"
    "${CMAKE_CURRENT_BINARY_DIR}/helloworld.pb.h"
    "${CMAKE_CURRENT_BINARY_DIR}/helloworld.grpc.pb.cc"
    "${CMAKE_CURRENT_BINARY_DIR}/helloworld.grpc.pb.h"
  COMMAND $<TARGET_FILE:protobuf::protoc>
  ARGS
    --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
    --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
    -I "${CMAKE_CURRENT_SOURCE_DIR}"
    --plugin=protoc-gen-grpc="$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    "${CMAKE_CURRENT_SOURCE_DIR}/helloworld.proto"
  DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/helloworld.proto")

set(HELLOWORLD_PROTO
  "${CMAKE_CURRENT_BINARY_DIR}/helloworld.pb.cc"
  "${CMAKE_CURRENT_BINARY_DIR}/helloworld.grpc.pb.cc")

add_executable(helloworld-client client.cpp ${HELLOWORLD_PROTO})
target_compile_definitions(helloworld-client PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)
set_target_properties(helloworld-client PROPERTIES CXX_STANDARD 17 C_STANDARD 99)
target_include_directories(helloworld-client PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../include")
target_include_directories(helloworld-client PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_include_directories(helloworld-client PRIVATE "${_INSTALL_DIR}/spdlog/include")
target_link_libraries(helloworld-client gRPC::grpc++)

add_executable(helloworld-server server.cpp ${HELLOWORLD_PROTO})
target_compile_definitions(helloworld-server PRIVATE SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE)
set_target_properties(helloworld-server PROPERTIES CXX_STANDARD 17 C_STANDARD 99)
target_include_directories(helloworld-server PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../include")
target_include_directories(helloworld-server PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_include_directories(helloworld-server PRIVATE "${_INSTALL_DIR}/spdlog/include")
target_link_libraries(helloworld-server gRPC::grpc++)
