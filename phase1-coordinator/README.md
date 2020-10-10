# phase1-coordinator

## Build Guide

To start the coordinator, run:
```
cargo run --release
```

## Testing

### Logging

Logging is enabled by default during tests. To silence all logs, run:
```
cargo test --features silent
```

### Serial Test Execution

By convention, all tests execute serially to minimize possible risk of writing over test storage.
