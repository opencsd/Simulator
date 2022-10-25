project(TableManager
        LANGUAGES    CXX
        VERSION     0.1
)


add_library(TableManager
        "TableManager.cc"        
)

target_include_directories(TableManager
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)
