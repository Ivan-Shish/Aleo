# Cli tools for Aleo setup 1

Some handy utilities for testing purposes

## Build Guide

Use stable Rust to build (at the moment of writing 1.54)

```bash
cargo build --release
```

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
