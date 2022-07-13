cmake_minimum_required (VERSION 3.12 FATAL_ERROR)
project(dblib_tests)

find_package(Boost 1.65 REQUIRED COMPONENTS unit_test_framework)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

add_compile_definitions(DBLIB_TESTS_FB=0)
add_compile_definitions(DBLIB_TESTS_SQLITE=0)

add_executable(dblib_tests
    tests/dblib_tests.cpp
    src/dblib_consts.cpp
    src/dblib_sqlite.cpp
    src/dblib.cpp
    src/dblib_exception.cpp
    src/dblib_stmt_tools.cpp
    src/dblib_cvt_utils.cpp
    src/dblib_firebird.cpp
    src/dblib_dyn.cpp
    src/dblib_postgresql.cpp
)

message(STATUS "Boost_LIBRARIES = ${Boost_LIBRARIES}")

target_link_libraries(dblib_tests ${Boost_LIBRARIES})
