# Cli tools for Aleo setup 1

Some handy utilities for testing purposes

## Build Guide

```bash
# Requires specific rustc version
rustup default 1.48
# After setting the toolchain, you can build with:
cargo build --release
```

~~Works on stable and nightly! (tested on 1.50 stable and 1.52 nightly)~~

Alternatively, to add the binaries to the PATH run:

```bash
cargo install --path .
```

This will add the `public-key-extractor` and `view-key` binaries to `.cargo/bin` folder

## Usage

```bash
# To generate a view key:
view-key > view_key.txt

# To produce a public key out of a private key:
public-key-extractor --path keys.json
```
