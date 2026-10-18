if(NOT DEFINED UML_SOURCE_DIR OR NOT DEFINED UML_OUTPUT_DIR)
    message(FATAL_ERROR "UML_SOURCE_DIR and UML_OUTPUT_DIR are required")
endif()

file(GLOB_RECURSE diagram_files
    "${UML_SOURCE_DIR}/*.puml")
if(NOT diagram_files)
    message(FATAL_ERROR "No PlantUML diagrams found in ${UML_SOURCE_DIR}")
endif()

if(DEFINED PLANTUML_EXECUTABLE AND EXISTS "${PLANTUML_EXECUTABLE}")
    set(renderer "${PLANTUML_EXECUTABLE}")
    set(renderer_mode "executable")
elseif(DEFINED JAVA_EXECUTABLE AND EXISTS "${JAVA_EXECUTABLE}"
        AND DEFINED PLANTUML_JAR AND EXISTS "${PLANTUML_JAR}")
    set(renderer "${JAVA_EXECUTABLE}")
    set(renderer_mode "jar")
else()
    message(FATAL_ERROR
        "PNG rendering requires PlantUML and Java. Set PLANTUML_EXECUTABLE, "
        "or set JAVA_EXECUTABLE and PLANTUML_JAR when configuring CMake.")
endif()

file(MAKE_DIRECTORY "${UML_OUTPUT_DIR}")
foreach(diagram IN LISTS diagram_files)
    get_filename_component(diagram_name "${diagram}" NAME)
    if(renderer_mode STREQUAL "executable")
        execute_process(
            COMMAND "${renderer}" -tpng -charset UTF-8
                -o "${UML_OUTPUT_DIR}" "${diagram}"
            RESULT_VARIABLE result
            OUTPUT_VARIABLE stdout
            ERROR_VARIABLE stderr)
    else()
        execute_process(
            COMMAND "${renderer}" -jar "${PLANTUML_JAR}" -tpng -charset UTF-8
                -o "${UML_OUTPUT_DIR}" "${diagram}"
            RESULT_VARIABLE result
            OUTPUT_VARIABLE stdout
            ERROR_VARIABLE stderr)
    endif()

    if(NOT result EQUAL 0)
        message(FATAL_ERROR
            "PlantUML failed for ${diagram_name}:\n${stdout}\n${stderr}")
    endif()
endforeach()

message(STATUS "Rendered ${diagram_files} diagrams to ${UML_OUTPUT_DIR}")
