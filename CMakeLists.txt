cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME quack)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

# set(CMAKE_CXX_STANDARD 17)

# OpenSSL: the server can listen over HTTPS (httplib's SSLServer) and generates
# its own self-signed keys. Comes from vcpkg in CI (see vcpkg.json), like it
# does for httpfs.
if(MINGW)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
endif()
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
# Every TU must agree on this: it selects the duckdb_httplib_openssl namespace
# that quack_server.hpp uses
add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)

include_directories(
  src/include duckdb/third_party/httplib duckdb/extension/autocomplete/include
  duckdb/third_party/mbedtls/include)

add_subdirectory(src)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdb_mbedtls ${OPENSSL_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_mbedtls
                      ${OPENSSL_LIBRARIES})

# On macOS, httplib (with OpenSSL support) loads root certificates from the
# system keychain by default, which references CoreFoundation and Security
# symbols. Link the frameworks explicitly, like httpfs does, or the loadable
# extension fails with "Undefined symbols" for _CFArrayGetCount & co.
if(APPLE)
  find_library(COREFOUNDATION_FRAMEWORK CoreFoundation)
  find_library(SECURITY_FRAMEWORK Security)
  target_link_libraries(${EXTENSION_NAME} ${COREFOUNDATION_FRAMEWORK}
                        ${SECURITY_FRAMEWORK})
  target_link_libraries(${LOADABLE_EXTENSION_NAME} ${COREFOUNDATION_FRAMEWORK}
                        ${SECURITY_FRAMEWORK})
endif()

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
