use tracing::subscriber;
use tracing_subscriber::{fmt::format::Format, FmtSubscriber};

/// Initialize logger with custom format and verbosity.
pub fn init_logger(verbosity: &str) {
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(match verbosity {
            "ERROR" => tracing::Level::ERROR,
            "WARN" => tracing::Level::WARN,
            "INFO" => tracing::Level::INFO,
            "DEBUG" => tracing::Level::DEBUG,
            "TRACE" => tracing::Level::TRACE,
            _ => tracing::Level::INFO,
        })
        // .without_time()
        .with_target(false)
        .event_format(Format::default())
        .finish();

    subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
