#!/bin/bash

echo "Building all crates"

cd aleo-setup/setup1-verifier || exit
cargo build --release
cd ../setup1-contributor || exit
cargo build --release
cd ../../aleo-setup-coordinator || exit
cargo build --release

echo "Build success"
