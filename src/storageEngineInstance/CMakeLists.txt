project(storageEngineInstance
        LANGUAGES    CXX
        VERSION     0.1
)

set(storageEngineInstance_modules
        "StorageEngineInputInterface"
)

add_subdirectory(buffer_manager)
add_subdirectory(CSDManager)
add_subdirectory(SnippetScheduler)
add_subdirectory(MergeQueryManager)
add_subdirectory(QueryExecutor)
add_subdirectory(SnippetManager)
add_subdirectory(StorageEngineInputInterface)
add_subdirectory(TableAccess)
add_subdirectory(TableManager)
add_subdirectory(WALManager)



add_dependencies(QueryExecutor TableAccess TableManager)

add_dependencies(SnippetScheduler CSDManager)
add_dependencies(buffer_manager TableManager SnippetScheduler)
add_dependencies(MergeQueryManager buffer_manager)
add_dependencies(SnippetManager MergeQueryManager)
add_dependencies(StorageEngineInputInterface SnippetManager WALManager)


add_executable(EngineInstance 
        "EngineInstance.cc"
)

add_dependencies(EngineInstance StorageEngineInputInterface)
include_directories(${Boost_INCLUDE_DIRS})
target_link_libraries(EngineInstance
        grpc_modules
        boost_system
        stdc++fs
        ${storageEngineInstance_modules}
)


target_include_directories(EngineInstance
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)

