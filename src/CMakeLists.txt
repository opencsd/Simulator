project(src
        LANGUAGES    CXX
        VERSION     0.1
)

add_subdirectory(csdWorkerModule)
add_subdirectory(grpc)
add_subdirectory(storageEngineInstance)