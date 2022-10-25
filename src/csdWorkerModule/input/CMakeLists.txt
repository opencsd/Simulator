project(input
        LANGUAGES    CXX
        VERSION     0.1
)

set(CMAKE_TOOLCHAIN_FILE /ngd-toolchain/toolchain.ngd.cmake)
link_libraries(
    pthread
)

add_library(csdInput
        "input.cc"
)

target_include_directories(csdInput
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
        ${CMAKE_SOURCE_DIR}/include/rocksdb
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
        ${CMAKE_SOURCE_DIR}/include/rocksdb
)

target_link_libraries(csdInput
        ${LIBROCKSDB}
)