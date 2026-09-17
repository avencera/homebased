//! `homebased daemon` commands.

use std::path::Path;
use std::process::{Command, ExitCode};
use std::time::Duration;

use crate::callback::{deliver_exit_event, exit_event};
use crate::client::Client;
use crate::domain::CallbackStatus;
use crate::error::AppError;
use crate::install;
use crate::store::{CancelResult, Store};
use clap::Subcommand;
use serde_json::json;

use super::Ctx;

/// Daemon subcommands.
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum DaemonCommand {
    /// Write and enable the host unit.
    Install {
        /// Print the unit or plist instead of installing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Disable and remove the host unit.
    Uninstall {
        /// Cancel in-flight tasks first.
        #[arg(long)]
        yes: bool,
    },
    /// Run the daemon in the foreground.
    Serve,
    /// Restart only `serve`. Allowed with in-flight tasks.
    Restart,
    /// Stop `serve`. Refuses if tasks are in flight unless `--yes`.
    Stop {
        /// Cancel in-flight tasks, wait for callbacks, then stop.
        #[arg(long)]
        yes: bool,
    },
    /// Socket state and in-flight count. Works with the socket down.
    Status,
}

/// Dispatch a daemon command.
pub async fn run(ctx: &Ctx, command: DaemonCommand) -> Result<ExitCode, AppError> {
    match command {
        DaemonCommand::Install { dry_run } => install(ctx, dry_run),
        DaemonCommand::Uninstall { yes } => uninstall(ctx, yes).await,
        DaemonCommand::Serve => {
            crate::daemon::serve(ctx.home.clone()).await?;
            Ok(ExitCode::SUCCESS)
        }
        DaemonCommand::Restart => restart(ctx).await,
        DaemonCommand::Stop { yes } => stop(ctx, yes).await,
        DaemonCommand::Status => status(ctx).await,
    }
}

fn install(ctx: &Ctx, dry_run: bool) -> Result<ExitCode, AppError> {
    let text = install::render(&ctx.home)?;
    if dry_run {
        match ctx.output {
            super::OutputMode::Json => {
                ctx.print_json(json!({
                    "api_version": crate::domain::API_VERSION,
                    "unit": text,
                    "kind": install::kind_name(),
                }))?;
            }
            _ => print!("{text}"),
        }
        return Ok(ExitCode::SUCCESS);
    }
    install::install(&ctx.home)?;
    ctx.print_id(
        "installed",
        &format!("installed {}", install::unit_path().display()),
        json!({
            "api_version": crate::domain::API_VERSION,
            "path": install::unit_path(),
            "kind": install::kind_name(),
        }),
    )?;
    Ok(ExitCode::SUCCESS)
}

async fn uninstall(ctx: &Ctx, yes: bool) -> Result<ExitCode, AppError> {
    stop(ctx, yes).await?;
    install::uninstall()?;
    ctx.print_id(
        "uninstalled",
        "uninstalled host unit (database kept)",
        json!({
            "api_version": crate::domain::API_VERSION,
            "database": ctx.home.db_path(),
        }),
    )?;
    Ok(ExitCode::SUCCESS)
}

async fn status(ctx: &Ctx) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    let socket_up = ctx.home.sock_path().exists() && client.get("/v1/status").await.is_ok();
    let store = Store::open(&ctx.home.db_path())?;
    let in_flight = store.in_flight_count()?;
    match ctx.output {
        super::OutputMode::Quiet => println!("{in_flight}"),
        super::OutputMode::Json => ctx.print_json(json!({
            "api_version": crate::domain::API_VERSION,
            "socket": if socket_up { "up" } else { "down" },
            "in_flight": in_flight,
            "home": ctx.home.root(),
        }))?,
        super::OutputMode::Human => {
            println!(
                "socket: {}  in_flight: {in_flight}  home: {}",
                if socket_up { "up" } else { "down" },
                ctx.home.root().display()
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn stop(ctx: &Ctx, yes: bool) -> Result<ExitCode, AppError> {
    let client = Client::new(ctx.home.sock_path());
    let socket_up = client.get("/v1/status").await.ok();
    ensure_idle_or_cancel(ctx, yes, socket_up.is_some()).await?;
    if let Some(body) = socket_up {
        if let Some(pid) = body.get("pid").and_then(|v| v.as_u64()) {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid as i32),
                nix::sys::signal::Signal::SIGTERM,
            );
        }
    }
    if install::unit_installed() {
        install::host_stop()?;
    }
    wait_socket_gone(&ctx.home.sock_path(), Duration::from_secs(15))?;
    ctx.print_id(
        "stopped",
        "stopped",
        json!({"api_version": crate::domain::API_VERSION, "stopped": true}),
    )?;
    Ok(ExitCode::SUCCESS)
}

async fn restart(ctx: &Ctx) -> Result<ExitCode, AppError> {
    if install::unit_installed() {
        install::host_restart()?;
    } else {
        let client = Client::new(ctx.home.sock_path());
        if let Ok(body) = client.get("/v1/status").await {
            if let Some(pid) = body.get("pid").and_then(|v| v.as_u64()) {
                let _ = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid as i32),
                    nix::sys::signal::Signal::SIGTERM,
                );
            }
            wait_socket_gone(&ctx.home.sock_path(), Duration::from_secs(15))?;
        }
        let exe = std::env::current_exe().map_err(|err| AppError::Internal {
            message: err.to_string(),
        })?;
        let mut cmd = Command::new(exe);
        cmd.arg("daemon")
            .arg("serve")
            .arg("--home")
            .arg(ctx.home.root())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            unsafe {
                cmd.pre_exec(|| {
                    nix::unistd::setsid().map_err(std::io::Error::from)?;
                    Ok(())
                });
            }
        }
        cmd.spawn()?;
        wait_socket_up(&ctx.home.sock_path(), Duration::from_secs(15))?;
    }
    ctx.print_id(
        "restarted",
        "restarted",
        json!({"api_version": crate::domain::API_VERSION, "restarted": true}),
    )?;
    Ok(ExitCode::SUCCESS)
}

async fn ensure_idle_or_cancel(ctx: &Ctx, yes: bool, socket_up: bool) -> Result<(), AppError> {
    let store = Store::open(&ctx.home.db_path())?;
    let in_flight = store.in_flight_count()?;
    if in_flight == 0 {
        return Ok(());
    }
    if !yes {
        return Err(AppError::TasksInFlight { count: in_flight });
    }
    cancel_in_flight(ctx, &store, socket_up).await?;
    wait_terminal_and_callback(&ctx.home.db_path(), Duration::from_secs(60))?;
    Ok(())
}

async fn cancel_in_flight(ctx: &Ctx, store: &Store, socket_up: bool) -> Result<(), AppError> {
    let tasks = store.non_terminal()?;
    if socket_up {
        let client = Client::new(ctx.home.sock_path());
        for row in tasks {
            let _ = client
                .post(&format!("/v1/tasks/{}/cancel", row.id), &json!({}))
                .await;
        }
        return Ok(());
    }
    for row in tasks {
        match store.request_cancel(row.id)? {
            CancelResult::AlreadyTerminal(_) => {}
            CancelResult::CancelledQueued(row) => {
                let reports = store.reports(row.id)?;
                let event = exit_event(&row, &reports, ctx.home.task_dir(row.id));
                deliver_exit_event(store, &ctx.home, &row, &event)?;
            }
            CancelResult::SignalWorker(row) => {
                if let Some(pid) = row.pid {
                    let _ = nix::sys::signal::kill(
                        nix::unistd::Pid::from_raw(pid),
                        nix::sys::signal::Signal::SIGTERM,
                    );
                }
            }
        }
    }
    Ok(())
}

fn wait_terminal_and_callback(db: &Path, budget: Duration) -> Result<(), AppError> {
    let start = std::time::Instant::now();
    loop {
        let store = Store::open(db)?;
        let busy = !store.non_terminal()?.is_empty()
            || store.list_tasks(&[], None)?.iter().any(|row| {
                row.status.is_terminal()
                    && matches!(
                        row.callback_status,
                        CallbackStatus::Pending | CallbackStatus::Sending
                    )
            });
        if !busy {
            return Ok(());
        }
        if start.elapsed() > budget {
            return Err(AppError::Internal {
                message: "timed out waiting for in-flight tasks to finish".into(),
            });
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn wait_socket_gone(path: &std::path::Path, budget: Duration) -> Result<(), AppError> {
    let start = std::time::Instant::now();
    while path.exists() {
        if start.elapsed() > budget {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

fn wait_socket_up(path: &std::path::Path, budget: Duration) -> Result<(), AppError> {
    let start = std::time::Instant::now();
    while !path.exists() {
        if start.elapsed() > budget {
            return Err(AppError::DaemonUnavailable {
                message: "socket did not appear after restart".into(),
            });
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}
