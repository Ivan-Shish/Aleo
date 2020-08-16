mod cli;
use cli::*;

use setup_utils::{get_rng, user_system_randomness};

use gumdrop::Options;
use std::{process, time::Instant};
use tracing_subscriber::{
    filter::EnvFilter,
    fmt::{time::ChronoUtc, Subscriber},
};

fn main() {
    Subscriber::builder()
        .with_timer(ChronoUtc::rfc3339())
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let opts = SNARKOpts::parse_args_default_or_exit();

    let command = opts.clone().command.unwrap_or_else(|| {
        eprintln!("No command was provided.");
        eprintln!("{}", SNARKOpts::usage());
        process::exit(2)
    });

    let now = Instant::now();
    let res = match command {
        Command::New(ref opt) => new(&opt).unwrap(),
        Command::Contribute(ref opt) => {
            // contribute to the randomness
            let mut rng = get_rng(&user_system_randomness());
            contribute(&opt, &mut rng).unwrap()
        }
        Command::Verify(ref opt) => verify(&opt).unwrap(),
    };

    let new_now = Instant::now();
    println!(
        "Executing {:?} took: {:?}. Result {:?}",
        opts,
        new_now.duration_since(now),
        res,
    );
}
