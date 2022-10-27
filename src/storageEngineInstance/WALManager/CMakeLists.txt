project(WALManager
        LANGUAGES    CXX
        VERSION     0.1
)


add_library(WALManager
        "WalManager.cc"        
)
target_link_libraries(WALManager
        cpprest
        boost_system
        crypto
        grpc_modules
)

target_include_directories(WALManager
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)
