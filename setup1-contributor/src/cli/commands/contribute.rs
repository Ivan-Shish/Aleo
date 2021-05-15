use crate::utils::UploadMode;

use clap::AppSettings;
use secrecy::SecretString;
use structopt::StructOpt;
use url::Url;

use std::path::PathBuf;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "Contribute",
    about = "Contribute to Aleo Setup I",
    setting(AppSettings::ColoredHelp),
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

    /// Specify the URL of the ceremony coordinator.
    #[structopt(long, help = "Coordinator api url")]
    pub api_url: Url,

    /// Read seed and private key at the given path.
    #[structopt(long, help = "Path to a file containing seed and private key")]
    pub keys_path: PathBuf,
}
