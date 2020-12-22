# phase1-verifier

## Build Guide

To install the verifier, run:
```
cargo build --release
```

To start the verifier, run the following command:
```
aleo-setup-verifier { 'development', 'inner', 'outer', 'universal' } { COORDINATOR_API_URL } { VERIFIER_VIEW_KEY } { (optional) TRACE }
```
