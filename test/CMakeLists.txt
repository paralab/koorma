include(GoogleTest)

function(koorma_add_test name)
  add_executable(${name} ${ARGN})
  target_link_libraries(${name} PRIVATE koorma_core GTest::gtest GTest::gtest_main)
  target_include_directories(${name} PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src)
  gtest_discover_tests(${name})
endfunction()

koorma_add_test(format_test format_test.cpp)
koorma_add_test(value_view_test value_view_test.cpp)
koorma_add_test(manifest_test manifest_test.cpp)
koorma_add_test(leaf_roundtrip_test leaf_roundtrip_test.cpp)
koorma_add_test(walker_test walker_test.cpp)
