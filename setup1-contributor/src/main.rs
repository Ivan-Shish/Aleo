use setup1_contributor::{cli::*, commands::*};

use clap::{App, AppSettings};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app = App::new("Aleo Setup Contributor")
        .version("0.3.0")
        .author("The Aleo Team <hello@aleo.org>")
        // .about("")
        .settings(&[
            AppSettings::ColoredHelp,
            AppSettings::DisableHelpSubcommand,
            AppSettings::DisableVersion,
            AppSettings::SubcommandRequiredElseHelp,
        ])
        .subcommands(vec![
            GenerateCommand::new().display_order(0),
            ContributeCommand::new().display_order(1),
        ])
        .set_term_width(0)
        .get_matches();

    match app.subcommand() {
        ("generate", Some(arguments)) => {
            let file_path = GenerateCommand::process(arguments)?;
            generate_keys(&file_path);
        }
        ("contribute", Some(arguments)) => {
            let (environment, coordinator_api_url, keys_path, upload_mode) = ContributeCommand::process(arguments)?;
            start_contributor(environment, coordinator_api_url, keys_path, upload_mode).await;
        }
        _ => {}
    }

    Ok(())
}
