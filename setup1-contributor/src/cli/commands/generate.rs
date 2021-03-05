use clap::AppSettings;
use secrecy::SecretString;
use structopt::StructOpt;

use std::path::PathBuf;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "Generate",
    about = "Generate a seed and an Aleo private key for contribution",
    rename_all = "snake-case",
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DisableHelpSubcommand),
    setting(AppSettings::DisableVersion)
)]
pub struct GenerateOptions {
    /// The passphrase to use for encrypting the private key. If
    /// unspecified, the passphrase will be requested via tty or
    /// pinentry dialog.
    #[structopt(long)]
    pub passphrase: Option<SecretString>,
    /// Store the seed and private key at the given path.
    #[structopt(rename_all = "screaming-snake-case", parse(from_os_str))]
    pub keys_path: PathBuf,
}
