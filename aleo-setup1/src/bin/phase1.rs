use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(not(feature = "wasm"))] {
        #[macro_use]
        extern crate hex_literal;

        use aleo_setup1::{CurveKind, cli::{contribute, new_challenge, transform, Command, Phase1Opts}};
        use phase1::Phase1Parameters;
        use setup_utils::{beacon_randomness, get_rng, user_system_randomness};

        use zexe_algebra::{Bls12_377, PairingEngine as Engine, BW6_761};

        use gumdrop::Options;
        use std::{process, time::Instant};
        use tracing_subscriber::{
            filter::EnvFilter,
            fmt::{time::ChronoUtc, Subscriber},
        };

        fn execute_cmd<E: Engine>(opts: Phase1Opts) {
            let parameters = Phase1Parameters::<E>::new(opts.proving_system, opts.power, opts.batch_size);

            let command = opts.clone().command.unwrap_or_else(|| {
                eprintln!("No command was provided.");
                eprintln!("{}", Phase1Opts::usage());
                process::exit(2)
            });

            let now = Instant::now();
            match command {
                Command::New(opt) => {
                    new_challenge(&opt.challenge_fname, &parameters);
                }
                Command::Contribute(opt) => {
                    // contribute to the randomness
                    let rng = get_rng(&user_system_randomness());
                    contribute(&opt.challenge_fname, &opt.response_fname, &parameters, rng);
                }
                Command::Beacon(opt) => {
                    // use the beacon's randomness
                    // Place block hash here (block number #564321)
                    let beacon_hash: [u8; 32] = hex!("0000000000000000000a558a61ddc8ee4e488d647a747fe4dcc362fe2026c620");
                    let rng = get_rng(&beacon_randomness(beacon_hash));
                    contribute(&opt.challenge_fname, &opt.response_fname, &parameters, rng);
                }
                Command::VerifyAndTransform(opt) => {
                    // we receive a previous participation, verify it, and generate a new challenge from it
                    transform(
                        &opt.challenge_fname,
                        &opt.response_fname,
                        &opt.new_challenge_fname,
                        &parameters,
                    );
                }
            };

            let new_now = Instant::now();
            println!("Executing {:?} took: {:?}", opts, new_now.duration_since(now));
        }

        fn main() {
            Subscriber::builder()
                .with_target(false)
                .with_timer(ChronoUtc::rfc3339())
                .with_env_filter(EnvFilter::from_default_env())
                .init();

            let opts: Phase1Opts = Phase1Opts::parse_args_default_or_exit();

            match opts.curve_kind {
                CurveKind::Bls12_377 => execute_cmd::<Bls12_377>(opts),
                CurveKind::BW6 => execute_cmd::<BW6_761>(opts),
            };
        }
    }
}
