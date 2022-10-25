project(buffer_manager
        LANGUAGES    CXX
        VERSION     0.1
)

link_libraries(
    pthread
)

add_library(buffer_manager
        "buffer_manager.cc"
)

target_include_directories(buffer_manager
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)

target_link_libraries(buffer_manager
        TableManager
        SnippetScheduler
        boost_thread
        boost_system
)