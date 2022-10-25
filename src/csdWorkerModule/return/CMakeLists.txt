project(return
        LANGUAGES    CXX
        VERSION     0.1
)

set(CMAKE_TOOLCHAIN_FILE /ngd-toolchain/toolchain.ngd.cmake)

link_libraries(
    pthread
)

add_library(csdReturn
        "return.cc"
)

target_include_directories(csdReturn
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)

target_link_libraries(csdReturn
        csdInput
)