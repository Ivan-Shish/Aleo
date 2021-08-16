use clap::AppSettings;
use secrecy::SecretString;
use structopt::StructOpt;

use std::path::PathBuf;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "Generate",
    about = "Generate a seed and an Aleo private key for contribution",
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DisableVersion)
)]
pub struct GenerateOptions {
    /// The passphrase to use for encrypting the private key. If
    /// unspecified, the passphrase will be requested via tty or
    /// pinentry dialog.
    #[structopt(long)]
    pub passphrase: Option<SecretString>,
    /// Store the seed and private key at the given path.
    /// For example: --keys-path keys.json
    #[structopt(long)]
    pub keys_path: PathBuf,
}
