cmake_minimum_required(VERSION 2.8)

SET(LIB_FILE ${CMAKE_CURRENT_SOURCE_DIR}/src/libsnappyc.a)
SET(LIB_HEADER_FOLDER ${CMAKE_CURRENT_SOURCE_DIR}/src/)

# how to build the result of the library
add_custom_command(OUTPUT  ${LIB_FILE}
                   COMMAND make 
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# create a target out of the library compilation result
add_custom_target(snappy_target DEPENDS ${LIB_FILE})

# create an library target out of the library compilation result
add_library(snappy STATIC IMPORTED GLOBAL)
add_dependencies(snappy snappy_target)

# specify where the library is and where to find the headers
set_target_properties(snappy
    PROPERTIES
    IMPORTED_LOCATION ${LIB_FILE}
    INTERFACE_INCLUDE_DIRECTORIES ${LIB_HEADER_FOLDER})