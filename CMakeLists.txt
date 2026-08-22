cmake_minimum_required(VERSION 3.20)
project(weir VERSION 0.1.0 LANGUAGES CXX C)

option(WEIR_SANITIZERS "Enable ASan and UBSan" OFF)
option(WEIR_LINUX_INTEGRATION_TESTS "Enable Linux server integration tests" OFF)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
add_compile_options(-Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror)
if(WEIR_SANITIZERS)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

add_library(weir src/weir.cpp src/transport/uring_ring.cpp src/transport/epoll_transport.cpp src/transport/uring_transport.cpp src/coroutine_server.cpp)
target_include_directories(weir PUBLIC include)
find_package(Threads REQUIRED)
target_link_libraries(weir PUBLIC Threads::Threads)

add_executable(weir-server src/server_main.cpp)
add_executable(weir-producer src/producer_main.cpp)
add_executable(weir-replay src/replay_main.cpp)
add_executable(weir-inspect-log src/inspect_log_main.cpp)
foreach(bin IN ITEMS weir-server weir-producer weir-replay weir-inspect-log)
  target_link_libraries(${bin} PRIVATE weir)
endforeach()

# Vendored HdrHistogram_c (public domain / CC0), pinned to tag 0.11.10.
add_library(hdr_histogram STATIC bench/vendor/hdr_histogram/hdr_histogram.c)
target_include_directories(hdr_histogram PUBLIC bench/vendor/hdr_histogram)
set_source_files_properties(bench/vendor/hdr_histogram/hdr_histogram.c
  PROPERTIES COMPILE_OPTIONS "-w")
add_executable(weir_bench bench/weir_bench.cpp)
target_include_directories(weir_bench PRIVATE src)
target_link_libraries(weir_bench PRIVATE weir hdr_histogram)

enable_testing()
add_executable(weir-tests tests/test_main.cpp)
target_link_libraries(weir-tests PRIVATE weir)
add_test(NAME weir-tests COMMAND weir-tests)
set_tests_properties(weir-tests PROPERTIES TIMEOUT 10)
add_executable(weir-transport-tests tests/transport_test.cpp)
target_link_libraries(weir-transport-tests PRIVATE weir)
add_test(NAME weir-transport-tests COMMAND weir-transport-tests)
set_tests_properties(weir-transport-tests PROPERTIES TIMEOUT 10)
if(WEIR_LINUX_INTEGRATION_TESTS)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "WEIR_LINUX_INTEGRATION_TESTS requires Linux runtime support")
  endif()
  find_program(PYTHON3 python3)
  if(PYTHON3)
    add_test(NAME weir-transport-integration
      COMMAND ${PYTHON3} ${CMAKE_SOURCE_DIR}/tests/integration/transport_test.py
              --server $<TARGET_FILE:weir-server>
              --inspect $<TARGET_FILE:weir-inspect-log>)
    set_tests_properties(weir-transport-integration PROPERTIES TIMEOUT 120)
    add_test(NAME weir-coroutine-transport-integration
      COMMAND ${CMAKE_COMMAND} -E env WEIR_BACKEND=coroutine
              ${PYTHON3} ${CMAKE_SOURCE_DIR}/tests/integration/transport_test.py
              --server $<TARGET_FILE:weir-server>
              --inspect $<TARGET_FILE:weir-inspect-log>)
    set_tests_properties(weir-coroutine-transport-integration PROPERTIES TIMEOUT 180)
    add_test(NAME weir-uring-transport-integration
      COMMAND ${CMAKE_COMMAND} -E env WEIR_BACKEND=io_uring
              ${PYTHON3} ${CMAKE_SOURCE_DIR}/tests/integration/transport_test.py
              --server $<TARGET_FILE:weir-server>
              --inspect $<TARGET_FILE:weir-inspect-log>)
    set_tests_properties(weir-uring-transport-integration PROPERTIES TIMEOUT 180)
  endif()
  add_test(NAME weir-crash-recovery
    COMMAND ${CMAKE_COMMAND} -E env WEIR_BUILD_DIR=${CMAKE_BINARY_DIR}
            ${CMAKE_SOURCE_DIR}/scripts/crash-recovery.sh)
  set_tests_properties(weir-crash-recovery PROPERTIES TIMEOUT 20)
endif()
