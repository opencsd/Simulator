project(QueryExecutor
        LANGUAGES    CXX
        VERSION     0.1
)

link_libraries(
    pthread
)

add_library(QueryExecutor
        "QueryExecutor.cc"
)

target_include_directories(QueryExecutor
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)
target_link_libraries(QueryExecutor
        TableAccess
        TableManager
)