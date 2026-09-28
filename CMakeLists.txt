cmake_minimum_required(VERSION 3.16)
project(shardkv_fault_isolation LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-O2 -Wall -Wextra -Wpedantic)

add_executable(
        shardkv_backend
        src/backend.cpp
)

add_executable(
        shardkv_gateway_baseline
        src/gateway_baseline.cpp
)

add_executable(
        shardkv_gateway_isolated
        src/gateway_isolated.cpp
)

target_link_libraries(shardkv_backend PRIVATE pthread)
target_link_libraries(shardkv_gateway_baseline PRIVATE pthread)
target_link_libraries(shardkv_gateway_isolated PRIVATE pthread)
