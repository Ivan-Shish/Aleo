# Install Basics

sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y \
    build-essential \
    clang \
    gcc \
    git \
    libssl-dev \
    llvm \
    make \
    pkg-config \
    tmux \
    xz-utils

# Install Rust

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

rustup default nightly
rustup component add rust-src

# Start Server
cargo run --release
