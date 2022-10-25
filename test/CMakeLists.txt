project(TestModule
        LANGUAGES    CXX
        VERSION     0.1
)


add_library(TestModule
        "testmodule.cc"
)
target_include_directories(TestModule
PUBLIC
        ${CMAKE_SOURCE_DIR}/include/test
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include/test
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)