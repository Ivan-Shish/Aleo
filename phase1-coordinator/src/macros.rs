/// Returns an instance for chunked `Phase1Parameters` given an instantiation of `PairingEngine`,
/// an instance of `Settings`, and a chunk ID.
#[macro_export]
macro_rules! phase1_chunked_parameters {
    ($curve:ident, $settings:ident, $chunk_id:ident) => {{
        let (contribution_mode, proving_system, _, power, batch_size, chunk_size) = $settings;
        Phase1Parameters::<$curve>::new_chunk(
            contribution_mode,
            $chunk_id as usize,
            chunk_size,
            proving_system,
            power,
            batch_size,
        )
    }};
}

/// Returns an instance for full `Phase1Parameters` given an instantiation of `PairingEngine`,
/// an instance of `Settings`.
#[macro_export]
macro_rules! phase1_full_parameters {
    ($curve:ident, $settings:ident) => {{
        let (_, proving_system, _, power, batch_size, _) = $settings;
        Phase1Parameters::<$curve>::new_full(proving_system, power, batch_size)
    }};
}

/// Returns the contribution filesize given an instantiation of `PairingEngine`,
/// an instance of `Settings`, a chunk ID, a compressed setting, and whether
/// this is the initialization round.
#[macro_export]
macro_rules! contribution_filesize {
    ($curve:ident, $settings:ident, $chunk_id:ident, $compressed:ident, $init:ident) => {{
        use setup_utils::UseCompression;

        let parameters = phase1_chunked_parameters!($curve, $settings, $chunk_id);
        match ($compressed, $init) {
            (UseCompression::Yes, true) => (parameters.contribution_size - parameters.public_key_size) as u64,
            (UseCompression::Yes, false) => parameters.contribution_size as u64,
            (UseCompression::No, _) => parameters.accumulator_size as u64,
        }
    }};
}

/// Returns the final round filesize given an instantiation of `PairingEngine`,
/// an instance of `Settings`, a chunk ID, a compressed setting, and whether
/// this is the initialization round.
#[macro_export]
macro_rules! round_filesize {
    ($curve:ident, $settings:ident, $chunk_id:ident, $compressed:ident, $init:ident) => {{
        let full_parameters = phase1_full_parameters!($curve, $settings);
        let parameters = Phase1Parameters::<$curve>::new(
            full_parameters.contribution_mode,
            0,
            full_parameters.powers_g1_length, // <- do not change this
            full_parameters.curve.clone(),
            full_parameters.proving_system,
            full_parameters.total_size_in_log2,
            full_parameters.batch_size,
        );
        match ($compressed, $init) {
            (UseCompression::Yes, true) => (parameters.contribution_size - parameters.public_key_size) as u64,
            (UseCompression::Yes, false) => parameters.contribution_size as u64,
            (UseCompression::No, _) => parameters.accumulator_size as u64,
        }
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
