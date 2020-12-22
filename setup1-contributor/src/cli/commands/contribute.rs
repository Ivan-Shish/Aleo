use crate::{
    cli::{cli_types::*, CLI},
    errors::CLIError,
};

use phase1_coordinator::environment::{Development, Environment, Parameters, Production};

#[inline]
fn development() -> Environment {
    let environment = Development::from(Parameters::TestCustom(64, 16, 512));
    environment.into()
}

#[inline]
fn inner() -> Environment {
    Production::from(Parameters::AleoInner).into()
}

#[inline]
fn outer() -> Environment {
    Production::from(Parameters::AleoOuter).into()
}

#[inline]
fn universal() -> Environment {
    Production::from(Parameters::AleoUniversal).into()
}

#[derive(Debug)]
pub struct ContributeCommand;

impl CLI for ContributeCommand {
    // Format: environment, package_name, version
    type Options = (Option<String>, Option<String>, Option<String>, Option<String>);
    type Output = (Environment, String, String, String);

    const ABOUT: AboutType = "Contribute to Aleo Setup I";
    const ARGUMENTS: &'static [ArgumentType] = &[
        // (name, description, possible_values, required, index)
        (
            "ENVIRONMENT",
            "Specify the contribution environment.",
            &["development", "inner", "outer", "universal"],
            true,
            1u64,
        ),
        (
            "COORDINATOR_API_URL",
            "Specify the URL of the ceremony coordinator",
            &[],
            true,
            2u64,
        ),
        (
            "KEYS_PATH",
            "Read seed and private key at the given path",
            &[],
            true,
            3u64,
        ),
    ];
    const FLAGS: &'static [FlagType] = &[];
    const NAME: NameType = "contribute";
    const OPTIONS: &'static [OptionType] = &[
        // (argument, conflicts, possible_values, requires)
        (
            "[upload_mode] --upload_mode=<upload_mode> 'Specify how the responses are uploaded'",
            &[],
            &["auto", "direct"],
            &[],
        ),
    ];
    const SUBCOMMANDS: &'static [SubCommandType] = &[];

    fn parse(arguments: &clap::ArgMatches) -> Result<Self::Options, CLIError> {
        return Ok((
            arguments.value_of("ENVIRONMENT").map(|s| s.to_string()),
            arguments.value_of("COORDINATOR_API_URL").map(|s| s.to_string()),
            arguments.value_of("KEYS_PATH").map(|s| s.to_string()),
            arguments.value_of("upload_mode").map(|s| s.to_string()),
        ));
    }

    fn output(options: Self::Options) -> Result<Self::Output, CLIError> {
        let (environment, coordinator_api_url, keys_path, upload_mode) = match options {
            (Some(environment), Some(coordinator_api_url), Some(keys_path), upload_mode) => {
                (environment, coordinator_api_url, keys_path, upload_mode)
            }
            _ => return Err(CLIError::MissingContributionParameters),
        };

        let environment = match environment.as_str() {
            "development" => development(),
            "inner" => inner(),
            "outer" => outer(),
            "universal" => universal(),
            _ => panic!("Invalid environment"),
        };

        let upload_mode = match upload_mode {
            Some(mode) => mode,
            None => "auto".to_string(),
        };

        Ok((environment, coordinator_api_url, keys_path, upload_mode))
    }
}
