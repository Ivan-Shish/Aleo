use crate::{
    cli::{cli_types::*, CLI},
    errors::CLIError,
};

#[derive(Debug)]
pub struct GenerateCommand;

impl CLI for GenerateCommand {
    type Options = Option<String>;
    type Output = String;

    const ABOUT: AboutType = "Generate a seed and an Aleo private key for contribution";
    const ARGUMENTS: &'static [ArgumentType] = &[
        // (name, description, possible_values, required, index)
        (
            "KEYS_PATH",
            "Store the seed and private key at the given path",
            &[],
            true,
            1u64,
        ),
    ];
    const FLAGS: &'static [FlagType] = &[];
    const NAME: NameType = "generate";
    const OPTIONS: &'static [OptionType] = &[
        // (argument, conflicts, possible_values, requires)
    ];
    const SUBCOMMANDS: &'static [SubCommandType] = &[];

    fn parse(arguments: &clap::ArgMatches) -> Result<Self::Options, CLIError> {
        Ok(arguments.value_of("KEYS_PATH").map(|s| s.to_string()))
    }

    fn output(options: Self::Options) -> Result<Self::Output, CLIError> {
        match options {
            Some(file_path) => Ok(file_path),
            None => Ok("aleo.keys".to_string()),
        }
    }
}
