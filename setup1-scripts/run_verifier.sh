#!/bin/bash

# Generate view key
aleo-setup/target/release/view-key > view_key.txt

# Run the verifier using the view key
# and writing the logs to verifier_logs.txt
cat view_key.txt | xargs aleo-setup/target/release/setup1-verifier development http://localhost:8000 &> verifier_logs.txt
