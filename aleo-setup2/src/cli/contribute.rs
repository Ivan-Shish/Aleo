use phase2::{chunked_groth16::contribute as chunked_contribute, keypair::PublicKey};
use setup_utils::Result;

use zexe_algebra::{Bls12_377, BW6_761};

use gumdrop::Options;
use memmap::MmapOptions;
use rand::Rng;
use std::fs::OpenOptions;

#[derive(Debug, Options, Clone)]
pub struct ContributeOpts {
    help: bool,
    #[options(
        help = "the previous contribution - the action will happen in place",
        default = "challenge"
    )]
    pub data: String,
    #[options(help = "the batches which can be loaded in memory", default = "50000")]
    pub batch: usize,
    #[options(help = "setup the inner or the outer circuit?")]
    pub is_inner: bool,
}

pub fn contribute<R: Rng>(opts: &ContributeOpts, rng: &mut R) -> Result<()> {
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
            .map_mut(&file)
            .expect("unable to create a memory map for input")
    };

    if opts.is_inner {
        chunked_contribute::<Bls12_377, _>(&mut file, rng, opts.batch)?;
    } else {
        chunked_contribute::<BW6_761, _>(&mut file, rng, opts.batch)?;
    }

    Ok(())
}
