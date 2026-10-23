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

# ==============================================================================
# EXTERNAL DEPENDENCIES
# ==============================================================================
add_subdirectory(external/mimalloc)

if(MINGW)
    target_compile_definitions(mimalloc-static PRIVATE MI_WIN_INIT_USE_RAW_DLLMAIN)
endif()

# ==============================================================================
# CORE LIBRARY TARGET
# ==============================================================================
add_library(async STATIC
        src/awaiters.cpp
        src/coro_scope.cpp
        src/mi_allocator.cpp
        src/one_thread_executor.cpp
        src/thread_pool_executor/thread_pool_dispatcher_task.cpp
        src/thread_pool_executor/thread_pool_executor.cpp
        include/async/async_io.hpp
        include/async/platform/threadpool_win32/async_file.hpp
)

add_library(async::async ALIAS async)

# ==============================================================================
# TEST SUITE TARGETS
# ==============================================================================
if(ASYNC_BUILD_TESTS)
    set(ASYNC_SMOKE_TARGET async_smoke_test)
else()
    set(ASYNC_SMOKE_TARGET test)
endif()
add_executable(${ASYNC_SMOKE_TARGET} tests/test.cpp)

add_executable(thread_pool_stress_test tests/thread_pool_stress_test.cpp)
target_link_libraries(thread_pool_stress_test PRIVATE async::async)

add_executable(thread_pool_completion_test tests/thread_pool_completion_test.cpp)
target_link_libraries(thread_pool_completion_test PRIVATE async::async)

add_executable(thread_pool_work_stealing_test tests/thread_pool_work_stealing_test.cpp)
target_link_libraries(thread_pool_work_stealing_test PRIVATE async::async)

add_executable(thread_pool_scheduling_overhead_test tests/thread_pool_scheduling_overhead_test.cpp)
target_link_libraries(thread_pool_scheduling_overhead_test PRIVATE async::async)

add_executable(thread_pool_wakeup_stress_test tests/thread_pool_wakeup_stress_test.cpp)
target_link_libraries(thread_pool_wakeup_stress_test PRIVATE async::async)

add_executable(async_shared_state_test tests/async_shared_state_test.cpp)
target_link_libraries(async_shared_state_test PRIVATE async::async)

add_executable(async_shared_state_stress_test tests/async_shared_state_stress_test.cpp)
target_link_libraries(async_shared_state_stress_test PRIVATE async::async)

# ==============================================================================
# EXAMPLES TARGETS
# ==============================================================================
option(ASYNC_BUILD_EXAMPLES "Build shuttle_async usage examples" ON)

if(ASYNC_BUILD_EXAMPLES)
    add_executable(async_thread_pool_example examples/thread_pool_tasks.cpp)
    target_link_libraries(async_thread_pool_example PRIVATE async::async)

    add_executable(async_shared_state_example examples/shared_state_custom_allocator.cpp)
    target_link_libraries(async_shared_state_example PRIVATE async::async)
endif()

# ==============================================================================
# COMPILER CONFIGURATIONS & INCLUDES
# ==============================================================================
target_compile_features(async PUBLIC cxx_std_23)

target_compile_options(async PUBLIC
        # Ваше текущее правило для GCC / Clang
        "$<$<AND:$<CONFIG:Debug>,$<CXX_COMPILER_ID:GNU,Clang>>:-foptimize-sibling-calls>"

        # НОВОЕ ПРАВИЛО ДЛЯ MSVC (Visual Studio)
        "$<$<AND:$<CONFIG:Debug>,$<CXX_COMPILER_ID:MSVC>>:/Ob1>"
)


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

# ==============================================================================
# PLANTUML DIAGRAMS BUILD CONFIGURATION (Outputs directly to docs/uml)
# ==============================================================================

# Automatically locate the fallback jar inside the project tools directory if not specified
if(NOT PLANTUML_JAR)
    set(PLANTUML_JAR "${CMAKE_CURRENT_SOURCE_DIR}/tools/plantuml.jar" CACHE FILEPATH "Path to plantuml.jar" FORCE)
endif()

# Validate and select the active diagram renderer backend
if(PLANTUML_EXECUTABLE AND EXISTS "${PLANTUML_EXECUTABLE}")
    set(PLANTUML_BASE_CMD "${PLANTUML_EXECUTABLE}")
elseif(JAVA_EXECUTABLE AND EXISTS "${JAVA_EXECUTABLE}" AND PLANTUML_JAR AND EXISTS "${PLANTUML_JAR}")
    set(PLANTUML_BASE_CMD "${JAVA_EXECUTABLE}" -jar "${PLANTUML_JAR}")
else()
    message(WARNING "PlantUML or Java paths are invalid. 'uml_png' target will not be fully functional.")
endif()

if(PLANTUML_BASE_CMD)
    set(ALL_GENERATED_PNGS "")
    set(UML_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/uml")
    set(UML_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/docs/uml")

    # Enforce directory existence prior to active target compilation
    file(MAKE_DIRECTORY "${UML_OUT_DIR}")

    # Discover architecture source diagram files
    file(GLOB PUML_FILES "${UML_SRC_DIR}/*.puml")

    foreach(SRC_DIAGRAM IN LISTS PUML_FILES)
        get_filename_component(DIAGRAM_NAME ${SRC_DIAGRAM} NAME_WE)
        set(OUT_PNG "${UML_OUT_DIR}/${DIAGRAM_NAME}.png")

        add_custom_command(
                OUTPUT "${OUT_PNG}"
                # The -o flag forces PlantUML to output directly to the unified docs/uml folder
                COMMAND ${PLANTUML_BASE_CMD} -tpng -charset UTF-8 -o "${UML_OUT_DIR}" "${SRC_DIAGRAM}"
                DEPENDS "${SRC_DIAGRAM}"
                COMMENT "Rendering diagram: docs/uml/${DIAGRAM_NAME}.png"
                VERBATIM
        )
        list(APPEND ALL_GENERATED_PNGS "${OUT_PNG}")
    endforeach()

    if(ALL_GENERATED_PNGS)
        add_custom_target(uml_png DEPENDS ${ALL_GENERATED_PNGS})
    else()
        add_custom_target(uml_png COMMAND ${CMAKE_COMMAND} -E echo "No .puml files discovered inside the uml/ folder")
    endif()
endif()

# ==============================================================================
# AUTOMATIC API DOCUMENTATION GENERATION (Pure English Doxygen)
# ==============================================================================
find_package(Doxygen QUIET)

if(DOXYGEN_FOUND)
    set(DOXYFILE "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile")

    if(EXISTS "${DOXYFILE}")
        # Unified documentation target for pure English references
        add_custom_target(docs
                COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_CURRENT_SOURCE_DIR}/docs/api"
                COMMAND ${DOXYGEN_EXECUTABLE} "${DOXYFILE}"
                WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
                COMMENT "Building clean English documentation from Doxyfile"
                VERBATIM
        )
        add_dependencies(docs uml_png)
    endif()
else()
    message(STATUS "Doxygen executable not found. Documentation targets will not be available.")
endif()


if(ASYNC_BUILD_TESTS)
    enable_testing()

    add_test(NAME thread_pool_stress COMMAND thread_pool_stress_test)
    set_tests_properties(thread_pool_stress PROPERTIES TIMEOUT 900)

    add_test(NAME thread_pool_completion COMMAND thread_pool_completion_test)
    set_tests_properties(thread_pool_completion PROPERTIES TIMEOUT 30)

    add_test(NAME thread_pool_work_stealing COMMAND thread_pool_work_stealing_test)
    set_tests_properties(thread_pool_work_stealing PROPERTIES TIMEOUT 300)

    add_test(NAME thread_pool_scheduling_overhead COMMAND thread_pool_scheduling_overhead_test)
    set_tests_properties(thread_pool_scheduling_overhead PROPERTIES TIMEOUT 300)

    add_test(NAME thread_pool_wakeup_stress COMMAND thread_pool_wakeup_stress_test)
    set_tests_properties(thread_pool_wakeup_stress PROPERTIES TIMEOUT 30)

    add_test(NAME async_shared_state COMMAND async_shared_state_test)
    set_tests_properties(async_shared_state PROPERTIES TIMEOUT 10)

    add_test(NAME async_shared_state_stress COMMAND async_shared_state_stress_test)
    set_tests_properties(async_shared_state_stress PROPERTIES TIMEOUT 120)
endif()
