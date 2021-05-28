# setup1-verifier

## Build Guide

To build the verifier run:

```bash
# Requires specific rustc version
rustup default 1.48

# Build the binary
cargo build --release
```

To add the binary to the PATH run:

```bash
cargo install --path .
```

This will add the `setup1-verifier` binary to `.cargo/bin` folder

## Usage

Generate a view key using [cli tools](../setup1-cli-tools)

```bash
view-key > view_key.txt
```

Run the verifier:
```bash
setup1-verifier --api-url http://localhost:9000 --view-key view_key.txt
```
where `--api-url` is a coordinator api address

## Log levels

The `setup1-verifier` binary is set up to read the `RUST_LOG` environment
variable to control the logs. Here are some examples:
```bash
# INFO, WARN and ERROR messages will be logged
RUST_LOG=info setup1-verifier ...

# We can also control logs in the dependencies. The following command
# will log TRACE, DEBUG, INFO, WARN and ERROR from all the sources
# but only WARN and ERROR from the hyper dependency
RUST_LOG="trace,hyper=warn" setup1-verifier ...
```
