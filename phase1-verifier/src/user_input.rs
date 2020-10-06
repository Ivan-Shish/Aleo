use crate::verifier::Verifier;

use std::io::stdin;

use tokio::task;
use tracing::{error, info};

///
/// This listens to terminal inputs for manual dispatch of lock and verify operations to the
/// coordinator. These terminal inputs are constructed in the format:
///     <method> <chunk_id>
///     e.g. "verify 4" or "lock 2"
///
pub async fn input_listener(verifier: Verifier) {
    task::spawn(async move {
        loop {
            {
                let mut input = String::new();
                match stdin().read_line(&mut input) {
                    Ok(_message_size) => {
                        let decoded_input: Vec<&str> = input.split(&[' ', '\n'][..]).collect();

                        if decoded_input.len() > 2 && (decoded_input[0] == "lock" || decoded_input[0] == "verify") {
                            let request_method = decoded_input[0].to_string();
                            let chunk_id = match decoded_input[1].parse::<u64>() {
                                Ok(chunk_id) => chunk_id,
                                Err(err) => {
                                    println!("decoded_input[1]: {:?}", decoded_input[1]);
                                    println!("err: {:?}", err);
                                    error!("Please specify a valid chunk_id");
                                    continue;
                                }
                            };

                            match request_method.to_lowercase().as_str() {
                                "lock" => {
                                    info!("Attempting to lock chunk {:?}", chunk_id);

                                    match verifier.lock_chunk(chunk_id).await {
                                        Ok(response) => info!("Coordinator response {}", response),
                                        Err(err) => error!("Failed to lock chunk {} (error {})", chunk_id, err),
                                    }
                                }
                                "verify" => {
                                    info!("Attempting to verify chunk {:?}", chunk_id);

                                    // Spawn a task to verify the chunk
                                    match verifier.verify_contribution(chunk_id).await {
                                        Ok(response) => info!("Coordinator response {}", response),
                                        Err(err) => error!("Failed to verify chunk {} (error {})", chunk_id, err),
                                    }
                                }
                                _ => error!("Please specify a valid method: lock or verify"),
                            }
                        } else {
                            error!(
                                "Please provide an input with the format: \"lock <chunk_id>\" or \"verify <chunk_id>\""
                            );
                        }
                    }
                    Err(error) => error!("error reading message: {}", error),
                }
            }
        }
    });
}
