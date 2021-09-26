cmake_minimum_required(VERSION 3.15)

if(NOT WIN32)
    if("$ENV{CLICOLOR}" OR "$ENV{CLICOLOR_FORCE}")
        string(ASCII 27 Esc)
        set(ColourReset "${Esc}[m")
        set(ColourBold  "${Esc}[1m")
        set(Red         "${Esc}[31m")
        set(Green       "${Esc}[32m")
        set(Yellow      "${Esc}[33m")
        set(Blue        "${Esc}[34m")
        set(Magenta     "${Esc}[35m")
        set(Cyan        "${Esc}[36m")
        set(White       "${Esc}[37m")
        set(BoldRed     "${Esc}[1;31m")
        set(BoldGreen   "${Esc}[1;32m")
        set(BoldYellow  "${Esc}[1;33m")
        set(BoldBlue    "${Esc}[1;34m")
        set(BoldMagenta "${Esc}[1;35m")
        set(BoldCyan    "${Esc}[1;36m")
        set(BoldWhite   "${Esc}[1;37m")
    endif()
endif()


function(pad_string OUT_VARIABLE DESIRED_LENGTH FILL_CHAR VALUE)
    string(LENGTH "${VALUE}" VALUE_LENGTH)
    math(EXPR REQUIRED_PADS "${DESIRED_LENGTH} - ${VALUE_LENGTH}")
    set(PAD ${VALUE})
    if(REQUIRED_PADS GREATER 0)
        math(EXPR REQUIRED_MINUS_ONE "${REQUIRED_PADS} - 1")
        foreach(FOO RANGE ${REQUIRED_MINUS_ONE})
            set(PAD "${PAD}${FILL_CHAR}")
        endforeach()
    endif()
    set(${OUT_VARIABLE} "${ColourBold}${PAD}${ColourReset}" PARENT_SCOPE)
endfunction()

function(remove_empty_genexpr list_data)
    foreach(elem ${${list_data}})
#            message(${elem})
        if(elem MATCHES "-|/")
            list(APPEND match_list ${elem})
        elseif(elem MATCHES "$<" OR elem MATCHES ">")
#            message("Discarding: ${elem}")
        else()
            list(APPEND match_list ${elem})
        endif()
    endforeach()
    set(${list_data} ${match_list} PARENT_SCOPE)
endfunction()

function(print_target_info target_name prefix)
    if(TARGET ${target_name})
        get_target_property(PROP_INC  ${target_name} INTERFACE_INCLUDE_DIRECTORIES)
        get_target_property(PROP_LIB  ${target_name} INTERFACE_LINK_LIBRARIES)
        get_target_property(PROP_OPT  ${target_name} INTERFACE_COMPILE_OPTIONS)
        get_target_property(PROP_DEF  ${target_name} INTERFACE_COMPILE_DEFINITIONS)
        get_target_property(PROP_FTR  ${target_name} INTERFACE_COMPILE_FEATURES)
        get_target_property(PROP_TYP  ${target_name} TYPE)
        get_target_property(PROP_IMP  ${target_name} IMPORTED)
        if(PROP_IMP)
            if(NOT PROP_TYP MATCHES "INTERFACE" OR CMAKE_VERSION VERSION_GREATER_EQUAL 3.19)
                get_target_property(PROP_LOC  ${target_name} LOCATION)
            endif()
        endif()

        remove_empty_genexpr(PROP_INC)
        remove_empty_genexpr(PROP_LIB)
        remove_empty_genexpr(PROP_OPT)
        remove_empty_genexpr(PROP_DEF)
        remove_empty_genexpr(PROP_FTR)

        pad_string(padded_target "40" " " "${prefix}[${target_name}]" )
        if(PROP_LIB)
            list(REMOVE_DUPLICATES PROP_LIB)
            message(VERBOSE "${padded_target} LIBRARY : ${PROP_LIB}" )
        endif()
        if(PROP_INC)
            list(REMOVE_DUPLICATES PROP_INC)
            message(VERBOSE "${padded_target} INCLUDE : ${PROP_INC}" )
        endif()
        if(PROP_OPT)
            list(REMOVE_DUPLICATES PROP_OPT)
            message(VERBOSE "${padded_target} OPTIONS : ${PROP_OPT}" )
        endif()
        if(PROP_DEF)
            list(REMOVE_DUPLICATES PROP_DEF)
            message(VERBOSE "${padded_target} DEFINES : ${PROP_DEF}" )
        endif()
        if(PROP_FTR)
            message(VERBOSE "${padded_target} FEATURE : ${PROP_FTR}" )
        endif()
        if(PROP_LOC)
            message(VERBOSE "${padded_target} IMPORTS : ${PROP_LOC}" )
        endif()
    endif()
endfunction()


function(print_compiler_info prefix)
    pad_string(padded_string "40" " " "${prefix}[${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}]" )
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        message(VERBOSE "${padded_string} OPTIONS : ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE}" )
    endif()
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        message(VERBOSE "${padded_string} OPTIONS : ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG}" )
    endif()
    if(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
        message(VERBOSE "${padded_string} OPTIONS : ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}" )
    endif()
    if(CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
        message(VERBOSE "${padded_string} OPTIONS : ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_MINSIZEREL}" )
    endif()
endfunction()

# Print summary of project targets
function(print_project_summary prj)
    include(cmake/getExpandedTarget.cmake)
    if(NOT TARGET ${prj})
        message(FATAL_ERROR "${prj} is not a valid target")
    endif()
    message(VERBOSE "| PROJECT SUMMARY [${prj}]")
    message(VERBOSE "|--------------------------")
    print_compiler_info("| ")
    expand_target_all_targets(${prj} TARGET_EXPANDED)
    foreach (t ${TARGET_EXPANDED})
        print_target_info(${t} "| ")
    endforeach ()
    message(VERBOSE "|--------------------------")
endfunction()