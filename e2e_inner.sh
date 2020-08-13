#!/bin/bash -e

rm -f challenge* response* new_challenge* processed*

POWER=19
BATCH=10000
CURVE="bls12_377"

phase1="cargo run --bin phase1 -- --curve-kind $CURVE --batch-size $BATCH --power $POWER"
phase2="cargo run --release --bin prepare_phase2 -- --curve-kind $CURVE --batch-size $BATCH --power $POWER --phase2-size $POWER"
snark="cargo run --release --bin aleo-setup2 --"

####### Phase 1

$phase1 new --challenge-fname challenge
yes | $phase1 contribute --challenge-fname challenge --response-fname response
$phase1 verify-and-transform --challenge-fname challenge --response-fname response --new-challenge-fname new_challenge
rm challenge new_challenge # no longer needed

###### Prepare Phase 2

$phase2 --response-fname response --phase2-fname processed --phase2-size $POWER

###### Phase 2

$snark new --phase1 processed --output initial_ceremony --phase1-size $POWER --is-inner

cp initial_ceremony contribution1
yes | $snark contribute --data contribution1 --is-inner
$snark verify --before initial_ceremony --after contribution1 --is-inner

# a new contributor contributes
cp contribution1 contribution2
yes | $snark contribute --data contribution2 --is-inner
$snark verify --before contribution1 --after contribution2 --is-inner
$snark verify --before initial_ceremony --after contribution2 --is-inner

# done! since `verify` passed, you can be sure that this will work
# as shown in the `mpc.rs` example
