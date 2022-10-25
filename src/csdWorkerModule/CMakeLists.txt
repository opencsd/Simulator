project(csdWorkerModule
        LANGUAGES    CXX
        VERSION     0.1
)

set(csd_worker_modules
        "csdScan"
)
SET(CMAKE_CXX_FLAGS "-O2")

set(LIBROCKSDB "${CMAKE_SOURCE_DIR}/thirdparty/rocksdb/librocksdb.a")
link_libraries(
    pthread
)
add_subdirectory(compress)
add_subdirectory(csdTableManager)
add_subdirectory(filter)
add_subdirectory(input)
add_subdirectory(merge)
add_subdirectory(return)
add_subdirectory(scan)

add_dependencies(csdReturn csdInput)
add_dependencies(csdMerge csdReturn)
add_dependencies(csdFilter csdMerge)
add_dependencies(csdScan csdFilter csdTableManager)


add_executable(CSDInstance 
        "CSDInstance.cc"
)

add_dependencies(CSDInstance csdScan)

target_link_libraries(CSDInstance
        csdScan
        ${LIBROCKSDB}
        dl
        z
        snappy
)


target_include_directories(CSDInstance
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)

