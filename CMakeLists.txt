cmake_minimum_required(VERSION 3.16)
project(aggregator_server CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(AGG_REAL_FEEDS "Build real CEX WebSocket connectors (requires Boost.Beast + Boost.Json)" OFF)

# --- gRPC / Protobuf ---------------------------------------------------------
# Only find_package(gRPC): its config already pulls Protobuf in as a dependency.
# Calling find_package(Protobuf) first makes gRPC's internal
# find_dependency(Protobuf) re-load the same export set, which fails with
# "Some (but not all) targets in this export set were already defined"
# (seen with Protobuf >= 4.x / recent CMake).
find_package(gRPC REQUIRED)
find_program(GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin REQUIRED)

# Locate protoc. Prefer PATH (works in the grpc/cpp image and with Homebrew);
# fall back to the imported target so the version always matches the library.
find_program(PROTOC_EXECUTABLE NAMES protoc)
if(NOT PROTOC_EXECUTABLE)
    if(TARGET protobuf::protoc)
        set(PROTOC_EXECUTABLE $<TARGET_FILE:protobuf::protoc>)
    else()
        message(FATAL_ERROR "protoc not found on PATH and no protobuf::protoc target")
    endif()
endif()

set(PROTO_DIR  ${CMAKE_SOURCE_DIR}/proto)
set(PROTO_FILE ${PROTO_DIR}/aggregator.proto)
set(GEN_DIR   ${CMAKE_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GEN_DIR})

set(PB_SRCS  ${GEN_DIR}/aggregator.pb.cc)
set(PB_HDRS  ${GEN_DIR}/aggregator.pb.h)
set(GRPC_SRCS ${GEN_DIR}/aggregator.grpc.pb.cc)
set(GRPC_HDRS ${GEN_DIR}/aggregator.grpc.pb.h)

add_custom_command(
    OUTPUT  ${PB_SRCS} ${PB_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND ${PROTOC_EXECUTABLE}
    ARGS    --grpc_out=${GEN_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_EXECUTABLE}
            --cpp_out=${GEN_DIR}
            -I ${PROTO_DIR}
            ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating gRPC/protobuf sources"
)

add_library(proto_lib ${PB_SRCS} ${GRPC_SRCS})
target_include_directories(proto_lib PUBLIC ${GEN_DIR} ${Protobuf_INCLUDE_DIRS})
target_link_libraries(proto_lib PUBLIC gRPC::grpc++ protobuf::libprotobuf)

include_directories(${CMAKE_SOURCE_DIR}/include)

# --- Aggregator server -------------------------------------------------------
add_executable(aggregator_server
    src/aggregator/server_main.cpp
    src/aggregator/aggregator_service.cpp
    src/aggregator/aggregator_engine.cpp
    src/aggregator/consolidated_book.cpp
    src/feed/order_book.cpp
    src/feed/simulated_exchange.cpp
)
target_compile_options(aggregator_server PRIVATE -O2)
target_link_libraries(aggregator_server PRIVATE proto_lib)

if(AGG_REAL_FEEDS)
    target_sources(aggregator_server PRIVATE src/feed/real_exchanges.cpp)
    find_package(Boost REQUIRED COMPONENTS system)
    find_package(OpenSSL REQUIRED)
    target_link_libraries(aggregator_server PRIVATE Boost::system OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(aggregator_server PRIVATE AGG_REAL_FEEDS)
endif()

# --- Client library + publisher binaries ------------------------------------
add_library(agg_client STATIC src/clients/aggregator_client.cpp)
target_link_libraries(agg_client PUBLIC proto_lib)

foreach(pub bbo_publisher volume_bands_publisher price_bands_publisher)
    add_executable(${pub} src/clients/${pub}.cpp)
    target_link_libraries(${pub} PRIVATE agg_client)
endforeach()

# --- Unit tests (pure C++, no gRPC dependency) ------------------------------
# The ring tests spawn real producer/consumer threads.
find_package(Threads REQUIRED)
add_executable(run_tests
    src/tests/main.cpp
    src/tests/test_order_book.cpp
    src/tests/test_consolidated_book.cpp
    src/tests/test_publishers.cpp
    src/tests/test_ring.cpp
    src/tests/test_market_state.cpp
    src/tests/test_types.cpp
    src/tests/test_simulated_feed.cpp
    src/feed/order_book.cpp
    src/feed/simulated_exchange.cpp
    src/aggregator/consolidated_book.cpp
)
target_link_libraries(run_tests PRIVATE Threads::Threads)
# No proto/grpc link needed; logic is validated independently of transport.

enable_testing()
add_test(NAME unit_tests COMMAND run_tests)
