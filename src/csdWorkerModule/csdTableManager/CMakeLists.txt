project(csdTableManager
        LANGUAGES    CXX
        VERSION     0.1
)

set(CMAKE_TOOLCHAIN_FILE /ngd-toolchain/toolchain.ngd.cmake)
link_libraries(
    pthread
)

add_library(csdTableManager
        "csd_table_manager.cc"
)

target_include_directories(csdTableManager
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
        ${CMAKE_SOURCE_DIR}/include/rocksdb
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
        ${CMAKE_SOURCE_DIR}/include/rocksdb

)

target_link_libraries(csdTableManager
        ${LIBROCKSDB}
)