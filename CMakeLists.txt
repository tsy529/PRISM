cmake_minimum_required(VERSION 3.18)

# 指定C编译器版本
set(CMAKE_C_COMPILER gcc-11)
set(CMAKE_CXX_COMPILER g++-11)

project(PRISM VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE "Debug")

# 编译文件存放位置 ./build/
set(CMAKE_BINARY_DIR "${PROJECT_SOURCE_DIR}/build")

# 可执行文件存放位置 ./bin/
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${PROJECT_SOURCE_DIR}/bin")

# 设置编译器标志，-pthread 选项用于启用 POSIX 线程库支持
set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-pthread")

# 添加源码存放位置 ./src/
add_subdirectory(src)
