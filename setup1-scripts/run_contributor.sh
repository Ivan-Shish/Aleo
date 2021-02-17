#!/bin/bash

# Generate the keys
# Currently requires user interaction to enter the passphrase
aleo-setup/target/release/aleo-setup-contributor generate keys.json

# Run the contributor
# Currently requires user interaction to enter the passphrase
aleo-setup/target/release/aleo-setup-contributor contribute development http://localhost:8000 keys.json &> contributor_logs.txt
