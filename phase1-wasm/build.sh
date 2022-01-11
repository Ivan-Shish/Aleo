#!/bin/sh

set -ex

# A couple of steps are necessary to get this build working which makes it slightly
# nonstandard compared to most other builds.
#
# * First, the Rust standard library needs to be recompiled with atomics
#   enabled. to do that we use Cargo's unstable `-Zbuild-std` feature.
#
# * Next we need to compile everything with the `atomics` and `bulk-memory`
#   features enabled, ensuring that LLVM will generate atomic instructions,
#   shared memory, passive segments, etc.
# Note the usage of `--target no-modules` here which is required for passing
# the memory import to each wasm module.
RUSTFLAGS='-C target-feature=+atomics,+bulk-memory,+mutable-globals -C codegen-units=1' \
wasm-pack build --release --target no-modules -- --features wasm --features parallel -Z build-std=std,panic_abort
