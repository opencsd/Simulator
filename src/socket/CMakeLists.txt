project(socket
        LANGUAGES    CXX
        VERSION     0.1
)


link_libraries(
    pthread
)
target_link_libraries(WALManager
        cpprest
        boost_system
        crypto
        grpc_modules
)

add_library(socket_module
       "socket.cc"
)

target_include_directories(socket_module
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)