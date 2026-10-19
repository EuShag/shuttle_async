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

add_executable(thread_pool_completion_test tests/thread_pool_completion_test.cpp)
target_link_libraries(thread_pool_completion_test PRIVATE async::async)

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

target_compile_options(async PUBLIC
        "$<$<AND:$<CONFIG:Debug>,$<CXX_COMPILER_ID:GNU>>:-foptimize-sibling-calls>"
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
# СЕКЦИЯ PLANTUML СБОРКИ (Генерация в docs/uml/en и docs/uml/ru)
# ==============================================================================

# Автоматический поиск jar-ника в папке tools проекта, если не задан извне
if(NOT PLANTUML_JAR)
    set(PLANTUML_JAR "${CMAKE_CURRENT_SOURCE_DIR}/tools/plantuml.jar" CACHE FILEPATH "Path to plantuml.jar" FORCE)
endif()

# Валидация и выбор рендерера
if(PLANTUML_EXECUTABLE AND EXISTS "${PLANTUML_EXECUTABLE}")
    set(PLANTUML_BASE_CMD "${PLANTUML_EXECUTABLE}")
elseif(JAVA_EXECUTABLE AND EXISTS "${JAVA_EXECUTABLE}" AND PLANTUML_JAR AND EXISTS "${PLANTUML_JAR}")
    set(PLANTUML_BASE_CMD "${JAVA_EXECUTABLE}" -jar "${PLANTUML_JAR}")
else()
    message(WARNING "PlantUML or Java paths are invalid. 'uml_png' target will not be fully functional.")
endif()

if(PLANTUML_BASE_CMD)
    set(UML_LANGUAGES "en" "ru")
    set(ALL_GENERATED_PNGS "")

    foreach(LANG IN LISTS UML_LANGUAGES)
        set(LANG_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/uml/${LANG}")
        set(LANG_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/docs/uml/${LANG}")

        # Гарантируем, что папки docs/uml/en и docs/uml/ru существуют до сборки
        file(MAKE_DIRECTORY "${LANG_OUT_DIR}")

        # Находим исходные .puml файлы
        file(GLOB PUML_FILES "${LANG_SRC_DIR}/*.puml")

        foreach(SRC_DIAGRAM IN LISTS PUML_FILES)
            get_filename_component(DIAGRAM_NAME ${SRC_DIAGRAM} NAME_WE)

            # Ожидаемый путь к готовой картинке в папке docs/
            set(OUT_PNG "${LANG_OUT_DIR}/${DIAGRAM_NAME}.png")

            add_custom_command(
                    OUTPUT "${OUT_PNG}"
                    # Флаг -o указывает PlantUML выходную папку для сохранения PNG
                    COMMAND ${PLANTUML_BASE_CMD} -tpng -charset UTF-8 -o "${LANG_OUT_DIR}" "${SRC_DIAGRAM}"
                    DEPENDS "${SRC_DIAGRAM}"
                    COMMENT "Rendering diagram: docs/uml/${LANG}/${DIAGRAM_NAME}.png"
                    VERBATIM
            )
            list(APPEND ALL_GENERATED_PNGS "${OUT_PNG}")
        endforeach()
    endforeach()

    if(ALL_GENERATED_PNGS)
        add_custom_target(uml_png DEPENDS ${ALL_GENERATED_PNGS})
    else()
        add_custom_target(uml_png COMMAND ${CMAKE_COMMAND} -E echo "No .puml files found in uml/en or uml/ru")
    endif()
endif()

# ==============================================================================
# АВТОМАТИЧЕСКАЯ СБОРКА API-ДОКУМЕНТАЦИИ (ЧЕРЕЗ ВАШИ DOXYFILE)
# ==============================================================================
find_package(Doxygen QUIET)

if(DOXYGEN_FOUND)
    set(DOXYFILE_EN "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile")
    set(DOXYFILE_RU "${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.ru")

    # Цель для английской документации
    if(EXISTS "${DOXYFILE_EN}")
        add_custom_target(docs_en
                COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_CURRENT_SOURCE_DIR}/docs/api/en"
                COMMAND ${DOXYGEN_EXECUTABLE} "${DOXYFILE_EN}"
                WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
                COMMENT "Building English documentation from Doxyfile"
                VERBATIM
        )
        list(APPEND ALL_DOC_TARGETS docs_en)
    endif()

    # Цель для русской документации
    if(EXISTS "${DOXYFILE_RU}")
        add_custom_target(docs_ru
                COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_CURRENT_SOURCE_DIR}/docs/api/ru"
                COMMAND ${DOXYGEN_EXECUTABLE} "${DOXYFILE_RU}"
                WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
                COMMENT "Building Russian documentation from Doxyfile.ru"
                VERBATIM
        )
        list(APPEND ALL_DOC_TARGETS docs_ru)
    endif()

    # Общая корневая цель 'docs', которая объединяет сборку языков
    if(ALL_DOC_TARGETS)
        add_custom_target(docs)
        add_dependencies(docs ${ALL_DOC_TARGETS})
        add_dependencies(docs uml_png)
    endif()
else()
    message(STATUS "Doxygen executable not found. Documentation targets will not be available.")
endif()

# ==============================================================================

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
