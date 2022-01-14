use cfg_if::cfg_if;

cfg_if! {
    if #[cfg(not(feature = "wasm"))] {
        mod cli;
        use cli::*;

        use gumdrop::Options;
        use std::{process, time::Instant};
        use tracing_subscriber::{
            filter::EnvFilter,
            fmt::{time::ChronoUtc, Subscriber},
        };
        use tracing::{error, info};

        fn execute_cmd(opts: Phase2Opts) {
            let command = opts.clone().command.unwrap_or_else(|| {
                error!("No command was provided.");
                error!("{}", Phase2Opts::usage());
                process::exit(2)
            });

            let now = Instant::now();

            match command {
                Command::New(opt) => {
                    new(
                        &opt.challenge_fname,
                        &opt.challenge_hash_fname,
                        &opt.challenge_list_fname,
                        opts.chunk_size,
                        &opt.phase1_fname,
                        opt.phase1_powers,
                        &opt.circuit_fname,
                        opts.is_inner,
                    );
                }
                Command::Contribute(opt) => {
                    let seed = hex::decode(&std::fs::read_to_string(&opts.seed).expect("should have read seed").trim())
                    .expect("seed should be a hex string");
                    let rng = setup_utils::derive_rng_from_seed(&seed);
                    contribute(
                        &opt.challenge_fname,
                        &opt.challenge_hash_fname,
                        &opt.response_fname,
                        &opt.response_hash_fname,
                        upgrade_correctness_check_config(
                            DEFAULT_CONTRIBUTE_CHECK_INPUT_CORRECTNESS,
                            opts.force_correctness_checks,
                        ),
                        opts.is_inner,
                        rng,
                    );
                }
                Command::Verify(opt) => {
                    verify(
                        &opt.challenge_fname,
                        &opt.challenge_hash_fname,
                        DEFAULT_VERIFY_CHECK_INPUT_CORRECTNESS,
                        &opt.response_fname,
                        &opt.response_hash_fname,
                        CheckForCorrectness::OnlyNonZero,
                        &opt.new_challenge_fname,
                        &opt.new_challenge_hash_fname,
                        opts.is_inner,
                    );
                }
                Command::Combine(opt) => {
                    combine(
                        &opt.initial_query_fname,
                        &opt.initial_full_fname,
                        &opt.response_list_fname,
                        &opt.combined_fname,
                        opts.is_inner,
                    );
                }
            };

            let new_now = Instant::now();
            info!("Executing {:?} took: {:?}", opts, new_now.duration_since(now));
        }

        fn main() {
            Subscriber::builder()
                .with_timer(ChronoUtc::rfc3339())
                .with_env_filter(EnvFilter::from_default_env())
                .init();
            let opts = SNARKOpts::parse_args_default_or_exit();

            let opts: Phase2Opts = Phase2Opts::parse_args_default_or_exit();

            match opts.curve_kind {
                CurveKind::Bls12_377 => execute_cmd(opts),
                CurveKind::BW6 => execute_cmd(opts),
            };
        }
    }
}
