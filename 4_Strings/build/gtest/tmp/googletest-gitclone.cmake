
if(NOT "/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest-stamp/googletest-gitinfo.txt" IS_NEWER_THAN "/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest-stamp/googletest-gitclone-lastrun.txt")
  message(STATUS "Avoiding repeated git clone, stamp file is up to date: '/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest-stamp/googletest-gitclone-lastrun.txt'")
  return()
endif()

execute_process(
  COMMAND ${CMAKE_COMMAND} -E remove_directory "/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to remove directory: '/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest'")
endif()

# try the clone 3 times in case there is an odd git clone issue
set(error_code 1)
set(number_of_tries 0)
while(error_code AND number_of_tries LESS 3)
  execute_process(
    COMMAND "/usr/bin/git"  clone --no-checkout "https://github.com/google/googletest" "googletest"
    WORKING_DIRECTORY "/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src"
    RESULT_VARIABLE error_code
    )
  math(EXPR number_of_tries "${number_of_tries} + 1")
endwhile()
if(number_of_tries GREATER 1)
  message(STATUS "Had to git clone more than once:
          ${number_of_tries} times.")
endif()
if(error_code)
  message(FATAL_ERROR "Failed to clone repository: 'https://github.com/google/googletest'")
endif()

execute_process(
  COMMAND "/usr/bin/git"  checkout v1.14.0 --
  WORKING_DIRECTORY "/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to checkout tag: 'v1.14.0'")
endif()

set(init_submodules TRUE)
if(init_submodules)
  execute_process(
    COMMAND "/usr/bin/git"  submodule update --recursive --init 
    WORKING_DIRECTORY "/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest"
    RESULT_VARIABLE error_code
    )
endif()
if(error_code)
  message(FATAL_ERROR "Failed to update submodules in: '/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest'")
endif()

# Complete success, update the script-last-run stamp file:
#
execute_process(
  COMMAND ${CMAKE_COMMAND} -E copy
    "/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest-stamp/googletest-gitinfo.txt"
    "/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest-stamp/googletest-gitclone-lastrun.txt"
  RESULT_VARIABLE error_code
  )
if(error_code)
  message(FATAL_ERROR "Failed to copy script-last-run stamp file: '/home/karl/Desktop/better-ppds/4_Strings/build/gtest/src/googletest-stamp/googletest-gitclone-lastrun.txt'")
endif()

