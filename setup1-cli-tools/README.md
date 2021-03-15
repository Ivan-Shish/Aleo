# Cli tools for Aleo setup 1

Some handy utilities for testing purposes

## Build Guide

```bash
cargo build --release
```

Works on stable and nightly! (tested on 1.50 stable and 1.52 nightly)

## Usage

```bash
# To generate a view key:
./view_key

# To produce a public key out of a private key:
./public_key_extractor --path keys.json
```
