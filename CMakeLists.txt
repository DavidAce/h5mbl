cmake_minimum_required(VERSION 3.12)
project(h5mbl VERSION 0.1.0 DESCRIPTION "Merges MBL batch data in HDF5 format using h5mbl")
option(H5MBL_PRINT_INFO "Print CMake target information"                               ON)
option(H5MBL_ENABLE_ASAN "Enable runtime address sanitizer -fsanitize=address"         OFF)
option(H5MBL_ENABLE_CCACHE "Enable runtime address sanitizer -fsanitize=address"       ON)

################################################################
### Get git version number                                   ###
### Generates a header gitversion/gitversion.h               ###
### Include it using #include <gitversion.h>                 ###
### Gives a namespace GIT:: with several git version numbers.###
################################################################
include(cmake/gitversion.cmake)


# Create an executable
add_executable(${PROJECT_NAME}
        source/main.cpp
        source/io/find.cpp
        source/io/logger.cpp
        source/io/hash.cpp
        source/io/h5db.cpp
        source/io/h5io.cpp
        source/io/id.cpp
        source/io/h5dbg.cpp
        source/general/prof.cpp
        source/general/text.cpp
        source/general/class_tic_toc.cpp
        source/general/human.cpp
        source/debug/stacktrace.cpp
        source/tid/tid.cpp
        source/tid/token.cpp
        source/tid/ur.cpp
        source/mpi/mpi-tools.cpp
        )
target_include_directories(${PROJECT_NAME} PRIVATE source)

# Enable C++17
target_compile_features(${PROJECT_NAME} PRIVATE cxx_std_17)

find_package(MPI COMPONENTS CXX REQUIRED)
target_link_libraries(${PROJECT_NAME} PRIVATE MPI::MPI_CXX)

# Set some compiler flags
include(cmake/CompilerFlags.cmake)

#######################################
# Settings for sanitizers           ###
#######################################
if(H5MBL_ENABLE_ASAN)
    target_compile_options(${PROJECT_NAME} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
    target_link_libraries(${PROJECT_NAME} PRIVATE -fsanitize=address)
endif()


#######################################
# Settings for ccache               ###
#######################################
if(H5MBL_ENABLE_CCACHE)
    find_program(CCACHE_PROGRAM ccache)
    if(CCACHE_PROGRAM)
        set_target_properties(${PROJECT_NAME} PROPERTIES CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
        message(STATUS "Using ccache ${CCACHE_PROGRAM}")
        if(H5MBL_ENABLE_PCH AND COMMAND target_precompile_headers)
            message(STATUS "Detected ccache + pch: Remember to set --> sloppiness = include_file_mtime,pch_defines,time_macros <-- in your ccache.conf")
        endif()
    else()
        message(STATUS "ccache program could not be found")
    endif()
endif()




##################################################################
### Install dependencies from conanfile.txt                    ###
##################################################################
unset(CONAN_COMMAND CACHE)
find_program (CONAN_COMMAND conan
        HINTS ${CONAN_PREFIX} $ENV{CONAN_PREFIX} ${CONDA_PREFIX} $ENV{CONDA_PREFIX}
        PATHS $ENV{HOME}/anaconda3 $ENV{HOME}/miniconda3 $ENV{HOME}/anaconda $ENV{HOME}/miniconda $ENV{HOME}/.conda
        PATH_SUFFIXES bin envs/dmrg/bin)

if(NOT CONAN_COMMAND)
    message(FATAL_ERROR "Could not find conan program executable")
else()
    message(STATUS "Found conan: ${CONAN_COMMAND}")
endif()

# Download cmake-conan integrator
if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan/conan.cmake")
    message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
    file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/release/0.17/conan.cmake"
            "${CMAKE_BINARY_DIR}/conan/conan.cmake"
            EXPECTED_HASH MD5=52a255a933397fdce3d0937f9c737e98
            TLS_VERIFY ON)
endif()
include(${CMAKE_BINARY_DIR}/conan/conan.cmake)

conan_cmake_autodetect(CONAN_AUTODETECT)
conan_cmake_install(
        CONAN_COMMAND ${CONAN_COMMAND}
        BUILD missing outdated cascade
        GENERATOR cmake_find_package_multi
        SETTINGS ${CONAN_AUTODETECT}
        INSTALL_FOLDER ${CMAKE_BINARY_DIR}/conan
        PATH_OR_REFERENCE ${CMAKE_SOURCE_DIR}
)

##################################################################
### Find all the things!                                       ###
##################################################################
if(NOT CONAN_CMAKE_SILENT_OUTPUT)
    set(CONAN_CMAKE_SILENT_OUTPUT OFF) # Default is off
endif()
list(PREPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR}/conan)
list(PREPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR}/conan)
# Use CONFIG to avoid MODULE mode. This is recommended for the cmake_find_package_multi generator

find_package(CLI11        2.1.1  REQUIRED CONFIG)
find_package(h5pp         1.9.1  REQUIRED CONFIG)
find_package(OpenSSL      1.1.1  REQUIRED CONFIG)
find_package(Backward     1.6    REQUIRED CONFIG)


target_link_libraries(h5mbl PUBLIC CLI11::CLI11)
target_link_libraries(h5mbl PUBLIC h5pp::h5pp)
target_link_libraries(h5mbl PUBLIC OpenSSL::OpenSSL)
target_link_libraries(h5mbl PUBLIC Backward::Backward)


# Print summary of CMake configuration
include(cmake/PrintTargetInfo.cmake)
print_project_summary(h5mbl)



