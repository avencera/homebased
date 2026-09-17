use std::process::ExitCode;

fn main() -> ExitCode {
    if let Err(err) = install_hooks() {
        eprintln!("failed to install error hooks: {err}");
        return ExitCode::from(1);
    }
    init_tracing();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map(|rt| rt.block_on(homebased::cli::run()))
        .unwrap_or_else(|err| {
            eprintln!("failed to start tokio: {err}");
            ExitCode::from(1)
        })
}

fn install_hooks() -> Result<(), color_eyre::Report> {
    let mut builder = color_eyre::config::HookBuilder::new();
    if std::env::var_os("NO_COLOR").is_some()
        || !std::io::IsTerminal::is_terminal(&std::io::stderr())
    {
        builder = builder.theme(color_eyre::config::Theme::new());
    }
    builder.install()?;
    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("homebased=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}
