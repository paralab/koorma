add_executable(koorma_bench bench_main.cpp)
target_link_libraries(koorma_bench PRIVATE koorma_core)
target_include_directories(koorma_bench PRIVATE
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/src)
