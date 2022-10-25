project(StorageEngineInputInterface
        LANGUAGES    CXX
        VERSION     0.1
)

link_libraries(
    pthread
)

add_library(StorageEngineInputInterface
        "StorageEngineInputInterface.cc"
)

target_include_directories(StorageEngineInputInterface
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/grpc
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include/grpc
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)

target_link_libraries(StorageEngineInputInterface
        SnippetManager
        WALManager
        grpc_modules
        stdc++fs
)

