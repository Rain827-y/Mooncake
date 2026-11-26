add_library(
  mooncake_memory_engine SHARED
  src/client/memory_engine_client.cpp
  src/common/memory_engine_common.cpp
  src/common/memory_engine_rpc.cpp
  src/server/memory_engine_server.cpp)
target_compile_features(mooncake_memory_engine PUBLIC cxx_std_20)
target_include_directories(
  mooncake_memory_engine PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/common)
target_link_libraries(mooncake_memory_engine PUBLIC tent_shared
                      PRIVATE glog::glog yalantinglibs::yalantinglibs)
set_target_properties(mooncake_memory_engine PROPERTIES INSTALL_RPATH "$ORIGIN")
install(TARGETS mooncake_memory_engine LIBRARY DESTINATION lib)
install(FILES include/memory_engine_client.h include/memory_engine_server.h DESTINATION include)
install(FILES ${PROJECT_SOURCE_DIR}/mooncake-transfer-engine/tent/include/tent/common/status.h
        DESTINATION include/tent/common)
