# CMAKE generated file: DO NOT EDIT!
# Generated by CMake Version 3.19
cmake_policy(SET CMP0009 NEW)

# PROJECT_SRCS at CMakeLists.txt:16 (file)
file(GLOB_RECURSE NEW_GLOB LIST_DIRECTORIES false "/Users/christophercheung/Desktop/6.830/830final/src/*.cpp")
set(OLD_GLOB
  "/Users/christophercheung/Desktop/6.830/830final/src/joiner.cpp"
  "/Users/christophercheung/Desktop/6.830/830final/src/main/harness.cpp"
  "/Users/christophercheung/Desktop/6.830/830final/src/main/main.cpp"
  "/Users/christophercheung/Desktop/6.830/830final/src/main/query2SQL.cpp"
  "/Users/christophercheung/Desktop/6.830/830final/src/operators.cpp"
  "/Users/christophercheung/Desktop/6.830/830final/src/parser.cpp"
  "/Users/christophercheung/Desktop/6.830/830final/src/relation.cpp"
  "/Users/christophercheung/Desktop/6.830/830final/src/utils.cpp"
  )
if(NOT "${NEW_GLOB}" STREQUAL "${OLD_GLOB}")
  message("-- GLOB mismatch!")
  file(TOUCH_NOCREATE "/Users/christophercheung/Desktop/6.830/830final/build/CMakeFiles/cmake.verify_globs")
endif()

# PROJECT_SRCS at CMakeLists.txt:16 (file)
file(GLOB_RECURSE NEW_GLOB LIST_DIRECTORIES false "/Users/christophercheung/Desktop/6.830/830final/src/include/*.h")
set(OLD_GLOB
  "/Users/christophercheung/Desktop/6.830/830final/src/include/joiner.h"
  "/Users/christophercheung/Desktop/6.830/830final/src/include/operators.h"
  "/Users/christophercheung/Desktop/6.830/830final/src/include/parser.h"
  "/Users/christophercheung/Desktop/6.830/830final/src/include/relation.h"
  "/Users/christophercheung/Desktop/6.830/830final/src/include/utils.h"
  )
if(NOT "${NEW_GLOB}" STREQUAL "${OLD_GLOB}")
  message("-- GLOB mismatch!")
  file(TOUCH_NOCREATE "/Users/christophercheung/Desktop/6.830/830final/build/CMakeFiles/cmake.verify_globs")
endif()

# TEST_SOURCE_FILES at test/CMakeLists.txt:33 (file)
file(GLOB_RECURSE NEW_GLOB LIST_DIRECTORIES false "/Users/christophercheung/Desktop/6.830/830final/test/*.cpp")
set(OLD_GLOB
  "/Users/christophercheung/Desktop/6.830/830final/test/main.cpp"
  "/Users/christophercheung/Desktop/6.830/830final/test/operators_test.cpp"
  "/Users/christophercheung/Desktop/6.830/830final/test/parser_test.cpp"
  "/Users/christophercheung/Desktop/6.830/830final/test/relation_test.cpp"
  )
if(NOT "${NEW_GLOB}" STREQUAL "${OLD_GLOB}")
  message("-- GLOB mismatch!")
  file(TOUCH_NOCREATE "/Users/christophercheung/Desktop/6.830/830final/build/CMakeFiles/cmake.verify_globs")
endif()
