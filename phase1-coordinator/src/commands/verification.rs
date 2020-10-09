use crate::{
    environment::Environment,
    storage::{Locator, Object, StorageWrite},
    CoordinatorError,
};
use phase1::{helpers::CurveKind, Phase1, Phase1Parameters, PublicKey};
use setup_utils::{calculate_hash, CheckForCorrectness, GenericArray, U64};

use std::io::{Read, Write};
use tracing::{debug, error, info, trace};
use zexe_algebra::{Bls12_377, PairingEngine as Engine, BW6_761};

pub(crate) struct Verification;

impl Verification {
    ///
    /// Runs chunk verification for a given environment, round height, and chunk ID.
    ///
    /// Executes the round verification on a given chunk ID.
    ///
    pub(crate) fn run(
        environment: &Environment,
        storage: &mut StorageWrite,
        round_height: u64,
        chunk_id: u64,
        current_contribution_id: u64,
        is_final_contribution: bool,
    ) -> anyhow::Result<()> {
        info!(
            "Starting verification of round {} chunk {} contribution {}",
            round_height, chunk_id, current_contribution_id
        );

        // Check that this is not the initial contribution.
        if (round_height == 0 || round_height == 1) && current_contribution_id == 0 {
            return Err(CoordinatorError::VerificationOnContributionIdZero.into());
        }

        // Fetch the contribution locators for `Verification`.
        let previous = Locator::ContributionFile(round_height, chunk_id, current_contribution_id - 1, true);
        let current = Locator::ContributionFile(round_height, chunk_id, current_contribution_id, false);
        let next = match is_final_contribution {
            true => Locator::ContributionFile(round_height + 1, chunk_id, 0, true),
            false => Locator::ContributionFile(round_height, chunk_id, current_contribution_id, true),
        };

        trace!("Previous contribution locator is {}", storage.to_path(&previous)?);
        trace!("Current contribution locator is {}", storage.to_path(&current)?);
        trace!("Next contribution locator is {}", storage.to_path(&next)?);

        Self::execute(environment, storage, chunk_id, previous, current, next)?;

        info!(
            "Completed verification of round {} chunk {} contribution {}",
            round_height, chunk_id, current_contribution_id
        );
        Ok(())
    }

    fn execute(
        environment: &Environment,
        storage: &mut StorageWrite,
        chunk_id: u64,
        challenge_file: Locator,
        response_file: Locator,
        next_challenge_file: Locator,
    ) -> Result<(), CoordinatorError> {
        // Check that the previous and current locators exist in storage.
        if !storage.exists(&challenge_file) || !storage.exists(&response_file) {
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // Execute ceremony verification on chunk.
        let settings = environment.to_settings();
        let (_, _, curve, _, _, _) = settings.clone();
        let result = match curve {
            CurveKind::Bls12_377 => Self::transform_pok_and_correctness(
                environment,
                storage.reader(&challenge_file)?.as_ref(),
                storage.reader(&response_file)?.as_ref(),
                &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
            ),
            CurveKind::BW6 => Self::transform_pok_and_correctness(
                environment,
                storage.reader(&challenge_file)?.as_ref(),
                storage.reader(&response_file)?.as_ref(),
                &phase1_chunked_parameters!(BW6_761, settings, chunk_id),
            ),
        };
        let response_hash = match result {
            Ok(response_hash) => response_hash,
            Err(error) => return Err(CoordinatorError::VerificationFailed.into()),
        };

        // Fetch the compression settings.
        let response_is_compressed = environment.compressed_outputs();
        let next_challenge_is_compressed = environment.compressed_inputs();

        // Create the next challenge file.
        if response_is_compressed == next_challenge_is_compressed {
            trace!("Don't need to recompress the contribution, copying the file without the public key");
            storage.copy(&response_file, &next_challenge_file)?;

            let hash = calculate_hash(&storage.reader(&next_challenge_file)?);
            debug!("Here's the BLAKE2b hash of the decompressed participant's response as new_challenge file:");
            Self::log_hash(&hash);
        } else {
            trace!("Verification succeeded! Writing to new challenge file...");

            // Initialize the next contribution locator so the output is saved,
            // and fetch the writer for the next locator.
            storage.initialize(
                next_challenge_file.clone(),
                Object::contribution_file_size(environment, chunk_id, true),
            )?;
            let mut next_challenge_writer = storage.writer(&next_challenge_file)?;

            // Fetch a response file reader.
            let response_reader = storage.reader(&response_file)?;

            let (_, _, curve, _, _, _) = settings.clone();
            match curve {
                CurveKind::Bls12_377 => Self::decompress(
                    response_reader.as_ref(),
                    next_challenge_writer.as_mut(),
                    response_hash.as_ref(),
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                )?,
                CurveKind::BW6 => Self::decompress(
                    response_reader.as_ref(),
                    next_challenge_writer.as_mut(),
                    response_hash.as_ref(),
                    &phase1_chunked_parameters!(Bls12_377, settings, chunk_id),
                )?,
            };
            drop(next_challenge_writer);

            let next_challenge_reader = storage.reader(&next_challenge_file)?;
            let recompressed_hash = calculate_hash(next_challenge_reader.as_ref());
            debug!("Here's the BLAKE2b hash of the decompressed participant's response as new_challenge file:");
            Self::log_hash(&recompressed_hash);
        }

        Ok(())
    }

    pub fn transform_pok_and_correctness<T: Engine + Sync>(
        environment: &Environment,
        challenge_reader: &[u8],
        response_reader: &[u8],
        parameters: &Phase1Parameters<T>,
    ) -> Result<GenericArray<u8, U64>, CoordinatorError> {
        // Fetch the compression settings.
        let challenge_is_compressed = environment.compressed_inputs();
        let response_is_compressed = environment.compressed_outputs();

        debug!(
            "Verifying a contribution to accumulator for 2^{} powers of tau",
            parameters.total_size_in_log2
        );

        // Check that contribution is correct
        trace!("Calculating previous challenge hash...");
        let current_accumulator_hash = calculate_hash(challenge_reader);
        debug!("Hash of the `challenge` file for verification:");
        Self::log_hash(&current_accumulator_hash);

        // Check the hash chain - a new response must be based on the previous challenge!
        {
            let mut response_challenge_hash = [0; 64];
            let mut memory_slice = response_reader.get(0..64).expect("must read point data from file");
            memory_slice
                .read_exact(&mut response_challenge_hash)
                .expect("couldn't read hash of challenge file from response file");

            debug!("`response` was based on the hash:");
            Self::log_hash(&response_challenge_hash);

            if &response_challenge_hash[..] != current_accumulator_hash.as_slice() {
                error!("Hash chain failure. This is not the right response.");
                return Err(CoordinatorError::VerificationFailed);
            }
        }

        let response_hash = calculate_hash(response_reader);
        debug!("Hash of the response file for verification:");
        Self::log_hash(&response_hash);

        // Fetch the contributor's public key.
        let public_key = PublicKey::read(response_reader, response_is_compressed, &parameters)?;

        trace!("Verifying a contribution to contain proper powers and correspond to the public key...");
        Phase1::verification(
            challenge_reader,
            response_reader,
            &public_key,
            current_accumulator_hash.as_slice(),
            challenge_is_compressed,
            response_is_compressed,
            CheckForCorrectness::No,
            CheckForCorrectness::Full,
            &parameters,
        )?;
        trace!("Verification succeeded!");

        Ok(response_hash)
    }

    fn decompress<'a, T: Engine + Sync>(
        response_reader: &[u8],
        mut next_challenge_writer: &mut [u8],
        response_hash: &[u8],
        parameters: &Phase1Parameters<T>,
    ) -> Result<(), CoordinatorError> {
        {
            (&mut next_challenge_writer[0..]).write_all(response_hash)?;
            next_challenge_writer.flush()?;
        }

        trace!("Decompressing the response file for the next challenge");
        Phase1::decompress(
            response_reader,
            next_challenge_writer,
            CheckForCorrectness::No,
            &parameters,
        )?;
        next_challenge_writer.flush()?;
        trace!("Decompressed the response file for the next challenge");

        Ok(())
    }

    /// Logs the hash.
    fn log_hash(hash: &[u8]) {
        let mut output = format!("\n\n");
        for line in hash.chunks(16) {
            output += "\t";
            for section in line.chunks(4) {
                for b in section {
                    output += &format!("{:02x}", b);
                }
                output += " ";
            }
            output += "\n";
        }
        debug!("{}", output);
    }
}

// info!("Sanity checking contribution 0 in round {}", round_height);
//
// // Open the transcript file.
// let transcript = environment.contribution_locator(round_height, chunk_id, contribution_id);
// let file = OpenOptions::new().read(true).open(&transcript)?;
// let reader = unsafe { MmapOptions::new().map(&file)? };
//
// // Check that the contribution chunk was generated based on the blank hash.
// let hash = blank_hash();
// for (expected, candidate) in hash.iter().zip(reader.chunks(64).next().unwrap()) {
//     if expected != candidate {
//         return Err(CoordinatorError::ChunkVerificationFailed.into());
//     }
// }
//
// // Compute the contribution hash to ensure it works.
// let contribution_hash = calculate_hash(&reader);
//
// if round_height == 0 {
//     Self::copy_initial(environment, round_height, chunk_id, contribution_hash)?;
// }
//
// info!("Completed sanity checking of contribution 0 in round {}", round_height);
// return Ok(());
