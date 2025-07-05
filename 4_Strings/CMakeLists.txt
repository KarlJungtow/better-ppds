cmake_minimum_required(VERSION 3.16.0)
project(PPDS_Strings VERSION 1.0.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set Optimization Flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fPIC")
set(CMAKE_CXX_FLAGS_RELEASE "-g -O2 -march=native -mtune=native -fPIC")

set(PPDS_PROJECT_DIR "${CMAKE_SOURCE_DIR}/..")

include_directories(${PPDS_PROJECT_DIR}/Util/include)
if(NOT DEFINED DATA_DIRECTORY)
    set(DATA_DIRECTORY "${PPDS_PROJECT_DIR}/../data/")
endif()

if(NOT DEFINED SOURCE_DIRECTORY)
    set(SOURCE_DIRECTORY "${CMAKE_SOURCE_DIR}/")
endif()

# Manuelle GTest-Integration mit korrektem Tag
include(ExternalProject)

ExternalProject_Add(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest
        GIT_TAG v1.14.0  # Korrigierter Tag
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/gtest
        CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release -DBUILD_GMOCK=ON -DINSTALL_GTEST=OFF -DBUILD_TESTING=OFF
        BUILD_BYPRODUCTS
        ${CMAKE_CURRENT_BINARY_DIR}/gtest/src/googletest-build/googlemock/libgmock.a
        ${CMAKE_CURRENT_BINARY_DIR}/gtest/src/googletest-build/googlemock/libgmock_main.a
        ${CMAKE_CURRENT_BINARY_DIR}/gtest/src/googletest-build/googletest/libgtest.a
        ${CMAKE_CURRENT_BINARY_DIR}/gtest/src/googletest-build/googletest/libgtest_main.a
        INSTALL_COMMAND ""
)

# Include-Verzeichnisse
set(GTEST_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/gtest/src/googletest/googletest/include)
set(GMOCK_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/gtest/src/googletest/googlemock/include)

# Bibliothekspfade
set(GTEST_LIBRARY ${CMAKE_CURRENT_BINARY_DIR}/gtest/src/googletest-build/lib/libgtest.a)
set(GTEST_MAIN_LIBRARY ${CMAKE_CURRENT_BINARY_DIR}/gtest/src/googletest-build/lib/libgtest_main.a)
set(GMOCK_LIBRARY ${CMAKE_CURRENT_BINARY_DIR}/gtest/src/googletest-build/lib/libgmock.a)
set(GMOCK_MAIN_LIBRARY ${CMAKE_CURRENT_BINARY_DIR}/gtest/src/googletest-build/lib/libgmock_main.a)

# Definiere die Bibliotheken
add_library(gtest STATIC IMPORTED)
set_property(TARGET gtest PROPERTY IMPORTED_LOCATION ${GTEST_LIBRARY})
add_dependencies(gtest googletest)

add_library(gtest_main STATIC IMPORTED)
set_property(TARGET gtest_main PROPERTY IMPORTED_LOCATION ${GTEST_MAIN_LIBRARY})
add_dependencies(gtest_main googletest)

add_library(gmock STATIC IMPORTED)
set_property(TARGET gmock PROPERTY IMPORTED_LOCATION ${GMOCK_LIBRARY})
add_dependencies(gmock googletest)

add_library(gmock_main STATIC IMPORTED)
set_property(TARGET gmock_main PROPERTY IMPORTED_LOCATION ${GMOCK_MAIN_LIBRARY})
add_dependencies(gmock_main googletest)

# Finde OpenMP
find_package(OpenMP REQUIRED)

# Hauptbibliothek
add_library(JoinUtils SHARED Join.cpp JoinUtils.hpp)

# Test executable
add_executable(JoinTest string_tests.cpp)
target_include_directories(JoinTest PRIVATE ${GTEST_INCLUDE_DIRS} ${GMOCK_INCLUDE_DIRS})
target_link_libraries(JoinTest PRIVATE
        JoinUtils
        gtest
        gtest_main
        gmock
        gmock_main
        OpenMP::OpenMP_CXX
)

# Füge Compiler-Definitionen hinzu
target_compile_definitions(JoinUtils PRIVATE
        DATA_DIRECTORY="${DATA_DIRECTORY}"
        SOURCE_DIRECTORY="${SOURCE_DIRECTORY}"
)

target_compile_definitions(JoinTest PRIVATE
        DATA_DIRECTORY="${DATA_DIRECTORY}"
        SOURCE_DIRECTORY="${SOURCE_DIRECTORY}"
)