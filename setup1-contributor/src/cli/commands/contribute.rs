use crate::utils::{environment_variants, parse_environment, UploadMode};

use clap::AppSettings;
use phase1_coordinator::environment::Environment;
use secrecy::SecretString;
use structopt::StructOpt;
use url::Url;

use std::path::PathBuf;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "Contribute",
    about = "Contribute to Aleo Setup I",
    rename_all = "snake-case",
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DisableHelpSubcommand),
    setting(AppSettings::DisableVersion)
)]
pub struct ContributeOptions {
    /// Specify how the responses are uploaded.
    #[structopt(
        long,
        possible_values = &UploadMode::variants(),
        default_value = "auto",
    )]
    pub upload_mode: UploadMode,

    /// The passphrase to use for decrypting the private key. If
    /// unspecified, the passphrase will be requested via tty or
    /// pinentry dialog.
    #[structopt(long)]
    pub passphrase: Option<SecretString>,

    /// Specify the contribution environment.
    #[structopt(
        rename_all = "screaming-snake-case",
        possible_values = environment_variants(),
        parse(try_from_str = parse_environment),
    )]
    pub environment: Environment,

    /// Specify the URL of the ceremony coordinator.
    #[structopt(rename_all = "screaming-snake-case")]
    pub coordinator_api_url: Url,

    /// Read seed and private key at the given path.
    #[structopt(rename_all = "screaming-snake-case", parse(from_os_str))]
    pub keys_path: PathBuf,
}
