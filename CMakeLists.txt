cmake_minimum_required(VERSION 2.8)
project(cmetrics C)

# CMetrics Version
set(CMT_VERSION_MAJOR  0)
set(CMT_VERSION_MINOR  1)
set(CMT_VERSION_PATCH  0)
set(CMT_VERSION_STR "${CMT_VERSION_MAJOR}.${CMT_VERSION_MINOR}.${CMT_VERSION_PATCH}")

# Include helpers
include(cmake/macros.cmake)
include(CheckCSourceCompiles)

# Include headers and dependency headers
include_directories(
  include
  lib/monkey/include
  lib/xxHash-0.8.0/
  )

# FIXME: MessagePack support
check_c_source_compiles("
  #include \"../../../lib/msgpack-c/include/msgpack.h\"
  int main() {
     msgpack_packer pck;
     msgpack_sbuffer sbuf;
     return 0;
  }" CMT_HAVE_MSGPACK)

# xxHash
if(NOT TARGET xxhash)
  # Do something when target found
  set(XXHASH_BUILD_ENABLE_INLINE_API OFF)
  set(XXHASH_BUILD_XXHSUM OFF)
  set(BUILD_SHARED_LIBS OFF)
  add_subdirectory(lib/xxHash-0.8.0/cmake_unofficial EXCLUDE_FROM_ALL)
endif()

# Source code
add_subdirectory(src)

# Tests
enable_testing()
add_subdirectory(tests)
