# aleo-setup-backend

Verifier IP
```
157.245.162.104
```

Verifier Websocket address
```
ws://157.245.162.104:8080/ws
```

## Setup

Create a `.env` file that contains the following variables:
```
COORDINATOR_API_URL=
VIEW_KEY=
```

## Build Guide

### Installation 

#### Execute the install script (ubuntu)
```
./install.sh
```

#### Install manually
```
# Install rust

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

rustup default nightly
rustup target add wasm32-unknown-unknown
rustup component add rust-src
cargo install wasm-bindgen-cli
```

### Running the backend + coordinator

#### Execute the run script
```
./run.sh
```

#### Run manually

```
cargo run --release
```