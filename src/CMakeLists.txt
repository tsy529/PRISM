# 查找 GLPK 头文件和库
find_path(GLPK_INCLUDE_DIR
    NAMES glpk.h
    PATHS /usr/include /usr/local/include
)

find_library(GLPK_LIBRARY
    NAMES glpk libglpk
    PATHS /usr/include /usr/local/include
)

# 查找OpenMP
find_package(OpenMP REQUIRED)

if (NOT GLPK_INCLUDE_DIR)
    message(FATAL_ERROR "GLPK headers not found.")
endif()

if (NOT GLPK_LIBRARY)
    message(FATAL_ERROR "GLPK library not found.")
endif()

# 递归地搜索 src 目录及其子目录中的所有 .cpp 文件
file(GLOB_RECURSE SRCS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

# 添加可执行文件，并包含找到的所有源文件
add_executable(main ${SRCS})

# 包含 GLPK 头文件
target_include_directories(main PRIVATE ${GLPK_INCLUDE_DIR})

# 连接 GLPK 库
target_link_libraries(main PRIVATE ${GLPK_LIBRARY})

# 链接 OpenMP
if(OpenMP_CXX_FOUND)
    target_link_libraries(main PRIVATE OpenMP::OpenMP_CXX)
endif()

# 定义项目路径
target_compile_definitions(main PRIVATE 
    PROJECT_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
)