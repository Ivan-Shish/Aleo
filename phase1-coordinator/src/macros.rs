/// Returns an instance of `Phase1Parameters` given an instantiation of `PairingEngine`,
/// an instance of `Settings`, and a chunk ID.
#[macro_export]
macro_rules! phase1_parameters {
    ($curve:ident, $settings:ident, $chunk_id:ident) => {{
        let (contribution_mode, proving_system, _, power, batch_size, chunk_size) = $settings;
        Phase1Parameters::<$curve>::new(
            contribution_mode,
            $chunk_id as usize,
            chunk_size,
            CurveParameters::new(),
            proving_system,
            power,
            batch_size,
        )
    }};
}

/// Returns an error logging message using `tracing`,
/// then returns the error itself.
#[macro_export]
macro_rules! return_error {
    ($error:ident, $message:ident) => {{
        error!($message);
        return $error;
    }};
}
