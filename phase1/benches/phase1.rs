use phase1::{
    helpers::testing::{generate_input, setup_verify},
    Phase1,
    Phase1Parameters,
};
use setup_utils::*;

use zexe_algebra::Bls12_377;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use rand::thread_rng;

// Benchmark comparing the generation of the iterator in parallel chunks
// Parallel generation is strictly better
fn benchmark_initialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("initialization");

    for compression in &[UseCompression::Yes, UseCompression::No] {
        for power in 10..14 {
            // large powers take too long
            if power > 8 {
                group.sample_size(10);
            }
            let parameters = Phase1Parameters::<Bls12_377>::new(power, power);
            let expected_challenge_length = parameters.get_length(*compression);

            // count in `other` powers (G1 will be 2x that)
            group.throughput(Throughput::Elements(power as u64));
            group.bench_with_input(format!("{}", compression), &power, |b, _power| {
                let mut output = vec![0; expected_challenge_length];
                b.iter(|| Phase1::initialization(&mut output, *compression, &parameters))
            });
        }
    }
}

// Benchmark comparing contributing to the ceremony
fn benchmark_computation(c: &mut Criterion) {
    let batch = 256;
    let mut group = c.benchmark_group(format!("computation_{}", batch));
    group.sample_size(10);

    let compressed_input = UseCompression::No;
    let compressed_output = UseCompression::Yes;

    // We gather data on various sizes.
    for power in 4..8 {
        let parameters = Phase1Parameters::<Bls12_377>::new(power, batch);
        let (input, _) = generate_input(&parameters, compressed_input);
        let mut output = vec![0; parameters.get_length(compressed_output)];
        let current_accumulator_hash = blank_hash();
        let mut rng = thread_rng();
        // Generate the private key.
        let (_, private_key) =
            Phase1::key_generation(&mut rng, current_accumulator_hash.as_ref()).expect("could not generate keypair");

        group.throughput(Throughput::Elements(power as u64));
        group.bench_with_input(
            format!("{}_{}", compressed_input, compressed_output),
            &power,
            |b, _size| {
                b.iter(|| {
                    Phase1::computation(
                        &input,
                        &mut output,
                        compressed_input,
                        compressed_output,
                        CheckForCorrectness::Yes,
                        &private_key,
                        &parameters,
                    )
                    .unwrap()
                })
            },
        );
    }
}

// Benchmark comparing contributing to the ceremony for various sizes and input/output
// compressed situations. Parallel verification is consistently faster by 10-15% in all
// modes of operation
fn benchmark_verification(c: &mut Criterion) {
    let correctness = CheckForCorrectness::No;

    // Iterate over all combinations of the following parameters
    let compression = &[
        (UseCompression::Yes, UseCompression::Yes),
        (UseCompression::No, UseCompression::Yes),
        (UseCompression::Yes, UseCompression::No),
        (UseCompression::No, UseCompression::No),
    ];
    let powers = (4..12).map(|i| 2u32.pow(i) as usize);
    let batch = 256;

    let mut group = c.benchmark_group(format!("verification_{}", batch));
    group.sample_size(10); // these would take way too long otherwise

    // Test the benchmark for everything in the parameter space
    for power in powers {
        for (compressed_input, compressed_output) in compression {
            let parameters = Phase1Parameters::<Bls12_377>::new(power, batch);

            let (input, output, pubkey, current_accumulator_hash) =
                setup_verify(*compressed_input, *compressed_output, &parameters);

            group.throughput(Throughput::Elements(power as u64));
            group.bench_with_input(
                format!("{}_{}", compressed_input, compressed_output),
                &power,
                |b, _power| {
                    b.iter(|| {
                        Phase1::verification(
                            &input,
                            &output,
                            &pubkey,
                            &current_accumulator_hash,
                            *compressed_input,
                            *compressed_output,
                            correctness,
                            correctness,
                            &parameters,
                        )
                        .unwrap()
                    })
                },
            );
        }
    }
}

criterion_group!(
    benches,
    benchmark_initialization,
    benchmark_computation,
    benchmark_verification
);
criterion_main!(benches);
