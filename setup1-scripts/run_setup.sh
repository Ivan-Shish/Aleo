#!/bin/bash

./run_coordinator.sh &
./run_verifier.sh &
./run_contributor.sh &

wait
echo "Setup completed"
