use std::process::Command;

use clap::{Parser, Subcommand, ValueEnum};
use color_eyre::eyre::{Result, bail};

mod bump;
mod release;

#[derive(Parser)]
#[command(name = "xtask")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build and install homebased, or publish a GitHub release
    Release {
        /// Where to put the release
        #[arg(value_enum)]
        place: Place,
    },
    /// Bump the homebased crate version
    Bump {
        /// Semver component to increment
        #[arg(value_enum)]
        part: bump::Part,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Place {
    /// Install to ~/.local/bin/homebased
    Local,
    /// Tag the current Cargo version and push it to GitHub
    Github,
    /// Alias for github
    Gh,
    /// Alias for github
    Public,
}

fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();

    match cli.command {
        Commands::Release { place } => match place {
            Place::Local => release::local(),
            Place::Github | Place::Gh | Place::Public => release::github(),
        },
        Commands::Bump { part } => bump::run(part),
    }
}

pub(crate) fn workspace_root() -> Result<std::path::PathBuf> {
    let output = Command::new("cargo")
        .args(["locate-project", "--workspace", "--message-format=plain"])
        .output()?;

    if !output.status.success() {
        bail!("Failed to locate workspace root");
    }

    let path = String::from_utf8(output.stdout)?;
    let cargo_toml = std::path::PathBuf::from(path.trim());
    let Some(root) = cargo_toml.parent() else {
        bail!("Cargo.toml should have a parent directory");
    };

    Ok(root.to_path_buf())
}

#[cfg(test)]
mod tests {
    use clap::ValueEnum;

    use super::Place;

    #[test]
    fn github_place_aliases() {
        assert!(matches!(Place::from_str("github", true), Ok(Place::Github)));
        assert!(matches!(Place::from_str("gh", true), Ok(Place::Gh)));
        assert!(matches!(Place::from_str("public", true), Ok(Place::Public)));
    }
}
