use crate::utils::{environment_variants, parse_environment, UploadMode};
use std::{path::PathBuf, str::FromStr};

use clap::AppSettings;
use phase1_coordinator::environment::{Development, Environment, Parameters, Production};
use structopt::StructOpt;
use url::Url;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "Contribute",
    about = "Contribute to Aleo Setup I",
    rename_all = "screaming-snake",
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DisableHelpSubcommand),
    setting(AppSettings::DisableVersion)
)]
pub struct ContributeOptions {
    /// Specify how the responses are uploaded.
    #[structopt(
        name("upload_mode"),
        long = "upload_mode",
        possible_values = &UploadMode::variants(),
        default_value = "auto",
    )]
    pub upload_mode: UploadMode,

    /// Specify the contribution environment.
    #[structopt(
        possible_values = environment_variants(),
        parse(try_from_str = parse_environment),
    )]
    pub environment: Environment,

    /// Specify the URL of the ceremony coordinator.
    pub coordinator_api_url: Url,

    /// Read seed and private key at the given path.
    #[structopt(parse(from_os_str))]
    pub keys_path: PathBuf,
}
