cmake_minimum_required(VERSION 3.12)
project(h5mbl VERSION 0.1.0 DESCRIPTION "Merges MBL batch data in HDF5 format using h5pp")
option(H5MBL_PRINT_INFO "Print CMake target information" ON)

# Create an executable
add_executable(${PROJECT_NAME} source/main.cpp source/io/nmspc_logger.cpp source/general/class_tic_toc.cpp)

# Enable C++17
target_compile_features(${PROJECT_NAME} PRIVATE cxx_std_17)


find_file(CONAN_BUILD_INFO
        conanbuildinfo.cmake
        HINTS ${CMAKE_BINARY_DIR} ${CMAKE_CURRENT_LIST_DIR}
        NO_DEFAULT_PATH)

if(CONAN_BUILD_INFO)
    ##################################################################
    ### Use pre-existing conanbuildinfo.cmake                      ###
    ### This avoids having to run conan again                      ###
    ##################################################################
    message(STATUS "Detected Conan build info: ${CONAN_BUILD_INFO}")
    include(${CONAN_BUILD_INFO})
    conan_basic_setup(TARGETS)
else()

    # Download Conan-CMake integration automatically, you can also just copy the conan.cmake file
    if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
        message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
        file(DOWNLOAD "https://github.com/conan-io/cmake-conan/raw/v0.15/conan.cmake"
                "${CMAKE_BINARY_DIR}/conan.cmake")
    endif()


    find_program (
            CONAN_COMMAND
            conan
            HINTS ${CONAN_PREFIX} $ENV{CONAN_PREFIX} ${CONDA_PREFIX} $ENV{CONDA_PREFIX}
            PATHS $ENV{HOME}/anaconda3  $ENV{HOME}/miniconda3 $ENV{HOME}/anaconda $ENV{HOME}/miniconda $ENV{HOME}/.conda
            PATH_SUFFIXES bin envs/dmrg/bin
    )
    if(NOT CONAN_COMMAND)
        message(FATAL_ERROR "Could not find conan program executable")
    else()
        message(STATUS "Found conan: ${CONAN_COMMAND}")
    endif()


    include(${CMAKE_BINARY_DIR}/conan.cmake)
    conan_cmake_run(
            CONAN_COMMAND ${CONAN_COMMAND}
            CONANFILE conanfile.txt
            BUILD_TYPE ${CMAKE_BUILD_TYPE}
            BASIC_SETUP CMAKE_TARGETS
            BUILD missing)
endif()


target_link_libraries(${PROJECT_NAME} PRIVATE ${CONAN_TARGETS})



# Print summary of CMake configuration
if (H5MBL_PRINT_INFO)
    include(cmake/PrintTargetInfo.cmake)
    include(cmake/getExpandedTarget.cmake)
    foreach(tgt ${CONAN_TARGETS})
        expand_target_all_targets(${tgt} expanded_target)
        list(APPEND H5MBL_TARGETS_EXPANDED ${expanded_target} )
    endforeach()
    list(REMOVE_DUPLICATES H5MBL_TARGETS_EXPANDED)
    message(STATUS "| H5MBL TARGET SUMMARY")
    message(STATUS "|--------------------")
    foreach(tgt ${H5MBL_TARGETS_EXPANDED})
        print_target_info(${tgt} "| ")
    endforeach()
endif()
