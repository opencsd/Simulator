project(CSDManager
        LANGUAGES    CXX
        VERSION     0.1
)


add_library(CSDManager
        "CSDManager.cc"
)

target_include_directories(CSDManager
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)
