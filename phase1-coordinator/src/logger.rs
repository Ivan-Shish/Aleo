use crate::environment::Environment;

use once_cell::sync::OnceCell;

#[cfg(not(feature = "log_file"))]
pub struct LogGuard;
#[cfg(feature = "log_file")]
pub struct LogGuard(tracing_appender::non_blocking::WorkerGuard);

pub(crate) static LOGGER: OnceCell<LogGuard> = OnceCell::new();

/// Initialize logger with custom format and verbosity.
pub(crate) fn initialize_logger(environment: &Environment) {
    #[cfg(not(feature = "log_file"))]
    LOGGER.get_or_init(|| {
        use tracing_subscriber::{fmt::format::Format, FmtSubscriber};

        let verbosity = environment.verbosity();

        let subscriber = FmtSubscriber::builder()
            // All spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
            // will be written to stdout.
            .with_max_level(*verbosity)
            .with_target(false)
            .event_format(Format::default())
            .finish();

        tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

        LogGuard {}
    });

    #[cfg(feature = "log_file")]
    LOGGER.get_or_init(|| {
        use tracing_subscriber::{fmt, fmt::format::Format, layer::SubscriberExt, FmtSubscriber};

        let local_base_directory = environment.local_base_directory();
        let verbosity = environment.verbosity();

        let file_appender = tracing_appender::rolling::hourly(format!("{}/logs", local_base_directory).as_str(), "log");
        let (file_writer, guard) = tracing_appender::non_blocking(file_appender);
        let file_output = fmt::Layer::default().with_writer(file_writer);

        let console_output = FmtSubscriber::builder()
            // All spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
            // will be written to stdout.
            .with_max_level(*verbosity)
            .with_target(false)
            .event_format(Format::default())
            .finish();

        tracing::subscriber::set_global_default(console_output.with(file_output))
            .expect("Failed to initialize logger. Unable to set the global tracing subscriber");

        // Hold onto `guard` for as long as you'd like logs to be written to a file.
        // If `guard` is dropped at the end of the scope of `initialize_logger`,
        // nothing will be written to a file.
        //
        // Be sure to return the guard and hold onto it in a static LOGGER or fn main()!
        LogGuard(guard)
    });
}
