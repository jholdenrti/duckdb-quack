cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME quack)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

#set(CMAKE_CXX_STANDARD 17)

include_directories(src/include duckdb/third_party/httplib duckdb/extension/autocomplete/include)
add_subdirectory(src)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Force the loadable extension to link the SHARED C++ runtime. Some toolchains
# (observed: gcc 16.1 on Arch-family hosts, whose compiler-private lib dir ships
# only libstdc++.a) resolve the driver's implicit -lstdc++ to the static archive
# when linking a shared object, silently embedding a second C++ EH runtime in
# the extension. Exceptions that cross the host/extension boundary then
# unbalance the per-thread std::uncaught_exceptions() counter of one of the two
# runtimes, and DuckDB consults Exception::UncaughtException() on shutdown paths
# (AttachedDatabase::Close TRY_CHECKPOINT, ~ClientContext): a poisoned thread
# silently skips the close-time WAL checkpoint of attached databases and can
# skip transaction cleanup. Naming libstdc++.so.6 explicitly restores the single
# shared runtime that hosts (duckdb CLI, go-duckdb) link dynamically.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_libraries(${LOADABLE_EXTENSION_NAME} -l:libstdc++.so.6)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
