project(SnippetScheduler
        LANGUAGES    CXX
        VERSION     0.1
)


link_libraries(
    pthread
)
add_library(SnippetScheduler
        "CSDScheduler.cc"
)

target_include_directories(SnippetScheduler
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)
target_link_libraries(SnippetScheduler
        CSDManager
)