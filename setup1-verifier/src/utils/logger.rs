use tracing_subscriber::{filter::Directive, EnvFilter, FmtSubscriber};

fn directive(text: &str) -> Directive {
    text.parse()
        .unwrap_or_else(|_| panic!("Failed to parse log filter directive: {}", text))
}

/// Initialize logger from RUST_LOG environment variable
pub fn init_logger() {
    let filter = EnvFilter::from_default_env().add_directive(directive("hyper=off"));
    let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
