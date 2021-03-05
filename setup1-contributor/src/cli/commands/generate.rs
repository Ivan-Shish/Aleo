use clap::AppSettings;
use structopt::StructOpt;

use std::path::PathBuf;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "Generate",
    about = "Generate a seed and an Aleo private key for contribution",
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DisableHelpSubcommand),
    setting(AppSettings::DisableVersion)
)]
pub struct GenerateOptions {
    /// Store the seed and private key at the given path.
    #[structopt(name("KEYS_PATH"), parse(from_os_str))]
    pub keys_path: PathBuf,
}
