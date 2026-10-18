cmake_minimum_required(VERSION 3.21)

project(shuttle_async)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(ASYNC_BUILD_TESTS "Build async module tests" OFF)
set(PLANTUML_EXECUTABLE "" CACHE FILEPATH "PlantUML command-line executable")
set(PLANTUML_JAR "" CACHE FILEPATH "Path to plantuml.jar")
find_program(PLANTUML_DISCOVERED_EXECUTABLE NAMES plantuml)
if(NOT PLANTUML_EXECUTABLE AND PLANTUML_DISCOVERED_EXECUTABLE)
    set(PLANTUML_EXECUTABLE "${PLANTUML_DISCOVERED_EXECUTABLE}")
endif()
find_package(Java COMPONENTS Runtime QUIET)
set(JAVA_EXECUTABLE "" CACHE FILEPATH "Java runtime used to launch plantuml.jar")
if(NOT JAVA_EXECUTABLE AND Java_JAVA_EXECUTABLE)
    set(JAVA_EXECUTABLE "${Java_JAVA_EXECUTABLE}")
endif()

add_subdirectory(external/mimalloc)

if(MINGW)
    target_compile_definitions(mimalloc-static PRIVATE MI_WIN_INIT_USE_RAW_DLLMAIN)
endif()

add_library(async STATIC
        src/awaiters.cpp
    src/coro_scope.cpp
    src/mi_allocator.cpp
    src/one_thread_executor.cpp
    src/thread_pool_executor/thread_pool_dispatcher_task.cpp
    src/thread_pool_executor/thread_pool_executor.cpp
)

add_library(async::async ALIAS async)

if(ASYNC_BUILD_TESTS)
    set(ASYNC_SMOKE_TARGET async_smoke_test)
else()
    set(ASYNC_SMOKE_TARGET test)
endif()
add_executable(${ASYNC_SMOKE_TARGET} tests/test.cpp)

add_executable(thread_pool_stress_test tests/thread_pool_stress_test.cpp)
target_link_libraries(thread_pool_stress_test PRIVATE async::async)

add_executable(thread_pool_work_stealing_test tests/thread_pool_work_stealing_test.cpp)
target_link_libraries(thread_pool_work_stealing_test PRIVATE async::async)

add_executable(thread_pool_scheduling_overhead_test
        tests/thread_pool_scheduling_overhead_test.cpp)
target_link_libraries(thread_pool_scheduling_overhead_test PRIVATE async::async)

add_executable(thread_pool_wakeup_stress_test
        tests/thread_pool_wakeup_stress_test.cpp)
target_link_libraries(thread_pool_wakeup_stress_test PRIVATE async::async)

add_executable(async_shared_state_test tests/async_shared_state_test.cpp)
target_link_libraries(async_shared_state_test PRIVATE async::async)

add_executable(async_shared_state_stress_test tests/async_shared_state_stress_test.cpp)
target_link_libraries(async_shared_state_stress_test PRIVATE async::async)

option(ASYNC_BUILD_EXAMPLES "Build shuttle_async usage examples" ON)

if(ASYNC_BUILD_EXAMPLES)
    add_executable(async_thread_pool_example examples/thread_pool_tasks.cpp)
    target_link_libraries(async_thread_pool_example PRIVATE async::async)

    add_executable(async_shared_state_example examples/shared_state_custom_allocator.cpp)
    target_link_libraries(async_shared_state_example PRIVATE async::async)
endif()

target_compile_features(async PUBLIC cxx_std_23)

target_include_directories(async
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/external/concurrentqueue
    PRIVATE
        ${PROJECT_SOURCE_DIR}/external/mimalloc/include
)

target_include_directories(${ASYNC_SMOKE_TARGET}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/external/concurrentqueue

)

target_link_libraries(${ASYNC_SMOKE_TARGET} PRIVATE async::async)

target_link_libraries(async PUBLIC mimalloc-static)

add_custom_target(uml_png
    COMMAND ${CMAKE_COMMAND}
        -DUML_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/uml/en
        -DUML_OUTPUT_DIR=${CMAKE_CURRENT_BINARY_DIR}/uml/en
        -DPLANTUML_EXECUTABLE=${PLANTUML_EXECUTABLE}
        -DPLANTUML_JAR=${PLANTUML_JAR}
        -DJAVA_EXECUTABLE=${JAVA_EXECUTABLE}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/scripts/render_uml.cmake
    COMMAND ${CMAKE_COMMAND}
        -DUML_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/uml/ru
        -DUML_OUTPUT_DIR=${CMAKE_CURRENT_BINARY_DIR}/uml/ru
        -DPLANTUML_EXECUTABLE=${PLANTUML_EXECUTABLE}
        -DPLANTUML_JAR=${PLANTUML_JAR}
        -DJAVA_EXECUTABLE=${JAVA_EXECUTABLE}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/scripts/render_uml.cmake
    COMMENT "Rendering English and Russian PlantUML diagrams to PNG"
    VERBATIM
)

if(ASYNC_BUILD_TESTS)
    enable_testing()

    add_test(NAME thread_pool_stress COMMAND thread_pool_stress_test)
    set_tests_properties(thread_pool_stress PROPERTIES TIMEOUT 900)

    add_test(NAME thread_pool_work_stealing COMMAND thread_pool_work_stealing_test)
    set_tests_properties(thread_pool_work_stealing PROPERTIES TIMEOUT 300)

    add_test(NAME thread_pool_scheduling_overhead
        COMMAND thread_pool_scheduling_overhead_test)
    set_tests_properties(thread_pool_scheduling_overhead PROPERTIES TIMEOUT 300)

    add_test(NAME thread_pool_wakeup_stress COMMAND thread_pool_wakeup_stress_test)
    set_tests_properties(thread_pool_wakeup_stress PROPERTIES TIMEOUT 30)

    add_test(NAME async_shared_state COMMAND async_shared_state_test)
    set_tests_properties(async_shared_state PROPERTIES TIMEOUT 10)

    add_test(NAME async_shared_state_stress COMMAND async_shared_state_stress_test)
    set_tests_properties(async_shared_state_stress PROPERTIES TIMEOUT 120)
endif()
