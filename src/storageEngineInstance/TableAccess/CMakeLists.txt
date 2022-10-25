project(TableAccess
        LANGUAGES    CXX
        VERSION     0.1
)


add_library(TableAccess
        "TableAccess.cc"
)
target_include_directories(TableAccess
PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
PRIVATE
        ${CMAKE_SOURCE_DIR}/include
        ${CMAKE_SOURCE_DIR}/include/rapidjson
)