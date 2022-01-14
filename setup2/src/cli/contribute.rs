use crate::cli::ContributeOpts;
use phase2::{chunked_groth16::contribute as chunked_contribute, keypair::PublicKey};
use setup_utils::Result;

use snarkvm_curves::{bls12_377::Bls12_377, bw6_761::BW6_761};

use fs_err::OpenOptions;
use memmap::MmapOptions;
use rand::{CryptoRng, Rng};

pub fn contribute(opts: &ContributeOpts, rng: &mut R) -> Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&opts.data)
        .expect("could not open file for writing the new MPC parameters ");
    let metadata = file.metadata()?;
    // extend the file by 1 pubkey
    if opts.is_inner {
        file.set_len(metadata.len() + PublicKey::<Bls12_377>::size() as u64)?;
    } else {
        file.set_len(metadata.len() + PublicKey::<BW6_761>::size() as u64)?;
    }
    let mut file = unsafe {
        MmapOptions::new()
            .map_mut(file.file())
            .expect("unable to create a memory map for input")
    };

    if opts.is_inner {
        chunked_contribute::<Bls12_377, _>(&mut file, rng, opts.batch)?;
    } else {
        chunked_contribute::<BW6_761, _>(&mut file, rng, opts.batch)?;
    }

    Ok(())
}

pub fn contribute<E: PairingEngine>(
    challenge_filename: &str,
    challenge_hash_filename: &str,
    response_filename: &str,
    response_hash_filename: &str,
    check_input_correctness: CheckForCorrectness,
    batch_exp_mode: BatchExpMode,
    mut rng: impl CryptoRng,
) {
    info!("Contributing to phase 2");

    let challenge_contents = std::fs::read(challenge_filename).expect("should have read challenge");
    let challenge_hash = calculate_hash(&challenge_contents);
    std::fs::File::create(challenge_hash_filename)
        .expect("unable to open current accumulator hash file")
        .write_all(&challenge_hash)
        .expect("unable to write current accumulator hash");

    info!("`challenge` file contains decompressed points and has a hash:");
    print_hash(&challenge_hash);

    let mut parameters = MPCParameters::<E>::read_fast(
        challenge_contents.as_slice(),
        COMPRESS_CONTRIBUTE_INPUT,
        check_input_correctness,
        false,
        SubgroupCheckMode::Auto,
    )
    .expect("should have read parameters");
    parameters
        .contribute(batch_exp_mode, &mut rng)
        .expect("should have successfully contributed");
    let mut serialized_response = vec![];
    parameters
        .write(&mut serialized_response, COMPRESS_CONTRIBUTE_OUTPUT)
        .expect("should have written input");
    std::fs::File::create(response_filename)
        .expect("unable to create response")
        .write_all(&serialized_response)
        .expect("unable to write the response");
    let response_hash = calculate_hash(&serialized_response);
    std::fs::File::create(response_hash_filename)
        .expect("unable to create response hash")
        .write_all(&response_hash)
        .expect("unable to write the response hash");
    info!(
        "Done!\n\n\
              Your contribution has been written to response file\n\n\
              The BLAKE2b hash of response file is:\n"
    );
    print_hash(&response_hash);
}
