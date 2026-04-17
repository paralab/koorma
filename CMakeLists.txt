cmake_minimum_required(VERSION 3.25)
project(koorma LANGUAGES CXX VERSION 0.0.1)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

add_compile_options(-Wall -Wextra -Wpedantic -Wshadow -Wconversion
                    -Wno-unused-parameter -fno-omit-frame-pointer)

option(KOORMA_BUILD_TESTS "Build gtest suites" ON)
option(KOORMA_USE_BLOOM_FILTER "Use Bloom filters (default)" ON)
option(KOORMA_USE_QUOTIENT_FILTER "Use VQF quotient filters" OFF)

if(KOORMA_USE_BLOOM_FILTER AND KOORMA_USE_QUOTIENT_FILTER)
  message(FATAL_ERROR "Choose exactly one of KOORMA_USE_BLOOM_FILTER or KOORMA_USE_QUOTIENT_FILTER")
endif()

find_package(absl REQUIRED)
find_package(liburing REQUIRED)
find_package(spdlog REQUIRED)
find_package(fmt REQUIRED)
# vqf is vendored or found via Conan; wired in src/mem/CMakeLists.txt

add_library(koorma_core STATIC)
target_include_directories(koorma_core PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>)
target_link_libraries(koorma_core PUBLIC
  absl::base absl::btree absl::crc32c absl::flat_hash_map absl::hash
  absl::strings absl::synchronization absl::status absl::statusor
  spdlog::spdlog fmt::fmt)
target_link_libraries(koorma_core PRIVATE
  liburing::liburing)

if(KOORMA_USE_BLOOM_FILTER)
  target_compile_definitions(koorma_core PUBLIC KOORMA_USE_BLOOM_FILTER=1)
endif()
if(KOORMA_USE_QUOTIENT_FILTER)
  target_compile_definitions(koorma_core PUBLIC KOORMA_USE_QUOTIENT_FILTER=1)
endif()

add_subdirectory(src)

if(KOORMA_BUILD_TESTS)
  enable_testing()
  find_package(GTest REQUIRED)
  add_subdirectory(test)
endif()
