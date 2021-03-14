pub mod commands;

use clap::AppSettings;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub enum Command {
    Generate(commands::generate::GenerateOptions),
    Contribute(commands::contribute::ContributeOptions),
}

#[derive(StructOpt, Debug)]
#[structopt(
    name = "Aleo Setup Contributor",
    author = "The Aleo Team <hello@aleo.org>",
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DisableHelpSubcommand),
    setting(AppSettings::DisableVersion),
    setting(AppSettings::SubcommandRequiredElseHelp)
)]
pub struct Options {
    #[structopt(subcommand)]
    pub subcommand: Command,
}
