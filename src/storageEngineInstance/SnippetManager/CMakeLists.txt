project(SnippetManager
        LANGUAGES    CXX
        VERSION     0.1
)


link_libraries(
    pthread
)
add_library(SnippetManager
        "SnippetManager.cc"
)

target_include_directories(SnippetManager
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/test
        ${CMAKE_SOURCE_DIR}/include/grpc
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/test
        ${CMAKE_SOURCE_DIR}/include/grpc
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)

target_link_libraries(SnippetManager
        MergeQueryManager
        grpc_modules
        TestModule
        boost_thread
        boost_system
)