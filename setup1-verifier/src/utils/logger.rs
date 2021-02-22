use tracing_subscriber::{FmtSubscriber, EnvFilter};

/// Initialize logger from RUST_LOG environment variable
pub fn init_logger() {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
