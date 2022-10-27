project(MergeQueryManager
        LANGUAGES    CXX
        VERSION     0.1
)


add_library(MergeQueryManager
        "MergeQueryManager.cc"
)

target_include_directories(MergeQueryManager
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)

target_link_libraries(MergeQueryManager
        buffer_manager
        
)