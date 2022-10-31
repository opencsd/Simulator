cmake_minimum_required(VERSION 3.16)
project(Simulator
        LANGUAGES    CXX
        VERSION     0.1
)

set(CMAKE_CXX_STANDARD 17)

find_package(Threads)
find_package(Boost 1.65.1 REQUIRED COMPONENTS system thread filesystem)


add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(thirdparty)

