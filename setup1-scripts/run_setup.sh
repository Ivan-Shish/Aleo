#!/bin/bash

./run-coordinator.sh &
./run-verifier.sh &
./run-contributor.sh &

wait
echo "Setup completed"
