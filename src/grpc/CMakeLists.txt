project(grpc
        LANGUAGES    CXX
        VERSION     0.1
)

get_filename_component(instanceProto "${CMAKE_SOURCE_DIR}/protos/snippet_sample.proto" ABSOLUTE)
get_filename_component(instanceProtoPath "${instanceProto}" PATH)

set(instanceProtoSources "${CMAKE_SOURCE_DIR}/src/grpc/snippet_sample.pb.cc")
set(instanceProtoHeaders "${CMAKE_SOURCE_DIR}/include/grpc/snippet_sample.pb.h")
set(instanceGrpcSources "${CMAKE_SOURCE_DIR}/src/grpc/snippet_sample.grpc.pb.cc")
set(instanceGrpcHeaders "${CMAKE_SOURCE_DIR}/include/grpc/snippet_sample.grpc.pb.h")
add_custom_command(
        OUTPUT "${instanceProtoSources}" "${instanceProtoHeaders}" "${instanceGrpcSources}" "${instanceGrpcHeaders}"
        COMMAND ${_PROTOBUF_PROTOC}
        ARGS --grpc_out "${CMAKE_SOURCE_DIR}/include/grpc" --cpp_out "${CMAKE_SOURCE_DIR}/src/grpc"
        -I "${instanceProtoPath}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${instanceProto}"
        DEPENDS "${instanceProto}")

include_directories("${CMAKE_CURRENT_BINARY_DIR}" "${CMAKE_SOURCE_DIR}/include/grpc")

add_library(grpc_modules
        ${instanceProtoSources}
        ${instanceProtoHeaders}
        ${instanceGrpcSources}
        ${instanceGrpcHeaders}
)


target_link_libraries(grpc_modules
        ${_PROTOBUF_LIBPROTOBUF}
        ${_REFLECTION}
        ${_GRPC_GRPCPP})