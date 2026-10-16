# Loaded as CMAKE_TOOLCHAIN_FILE for the rdkafka-sys cmake build on Windows only
# (wired up in .cargo/config.toml). It adds nothing toolchain-related.
#
# Why: on Windows librdkafka's CMake build uses WITHOUT_WIN32_CONFIG=ON, so the
# codec macros come from src/CMakeLists.txt, where WITH_SNAPPY=1 is emitted only
# when the cache variable WITH_SNAPPY is set. Nothing in librdkafka or in
# rdkafka-sys's build.rs (4.10.0+2.12.1) sets it, so Windows builds came out
# without snappy ("snappy not enabled at build time"). On macOS/Linux the
# generated config.h hard-codes WITH_SNAPPY 1. snappy.c is always compiled in,
# so turning the flag on needs no extra library.
#
# Checked by kafka::producer::tests::librdkafka_is_built_with_every_compression_codec.
set(WITH_SNAPPY ON CACHE BOOL "With Snappy (set by kafka-explorer)" FORCE)
