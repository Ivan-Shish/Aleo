use crate::objects::AleoSetupKeys;

use snarkos_toolkit::account::{Address, PrivateKey};

use age::{
    armor::{ArmoredWriter, Format},
    cli_common::{self, Passphrase},
    EncryptError,
    Encryptor,
};
use anyhow::Result;
use rand::{rngs::OsRng, RngCore};
use secrecy::{ExposeSecret, SecretVec};
use std::io::Write;

fn encrypt(encryptor: Encryptor, secret: &[u8]) -> Result<String> {
    let mut encrypted_output = vec![];
    let mut writer = encryptor
        .wrap_output(ArmoredWriter::wrap_output(&mut encrypted_output, Format::Binary)?)
        .map_err(|e| match e {
            EncryptError::Io(e) => e,
        })?;
    std::io::copy(&mut std::io::Cursor::new(secret), &mut writer)?;
    writer.finish()?;
    let encrypted_secret = hex::encode(&encrypted_output);
    Ok(encrypted_secret)
}

pub fn generate_keys(file_path: &str) {
    let mut file = std::fs::File::create(&file_path).expect("Should have created keys file");
    let (aleo_encryptor, private_key_encryptor) = match cli_common::read_or_generate_passphrase() {
        Ok(Passphrase::Typed(passphrase)) => (
            Encryptor::with_user_passphrase(passphrase.clone()),
            Encryptor::with_user_passphrase(passphrase),
        ),
        Ok(Passphrase::Generated(new_passphrase)) => {
            println!("\n\nGenerated new passphrase: {}", new_passphrase.expose_secret());
            (
                Encryptor::with_user_passphrase(new_passphrase.clone()),
                Encryptor::with_user_passphrase(new_passphrase),
            )
        }
        Err(_) => panic!("Should have read or generated passphrase"),
    };

    println!("\nDO NOT FORGET YOUR PASSPHRASE!\n\nYou will need your passphrase to access your keys.\n\n");

    let mut rng = OsRng;
    let mut aleo_seed = vec![0u8; 64];
    rng.fill_bytes(&mut aleo_seed[..]);
    let aleo_seed = SecretVec::new(aleo_seed);
    let private_key = PrivateKey::new(&mut rng).expect("Should have generated an Aleo private key");
    let address = Address::from(&private_key).expect("Should have derived an Aleo address");
    let private_key = private_key.to_string();

    let encrypted_aleo_seed =
        encrypt(aleo_encryptor, aleo_seed.expose_secret()).expect("Should have encrypted Aleo seed");
    let encrypted_aleo_private_key =
        encrypt(private_key_encryptor, private_key.as_bytes()).expect("Should have encrypted private key");

    let aleo_setup_keys = AleoSetupKeys {
        encrypted_seed: encrypted_aleo_seed,
        encrypted_private_key: encrypted_aleo_private_key,
        address: address.to_string(),
    };
    file.write_all(&serde_json::to_vec(&aleo_setup_keys).expect("Should have converted setup keys to vector"))
        .expect("Should have written setup keys successfully to file");
    println!("Done! Your keys are ready in {}.", &file_path);
}
