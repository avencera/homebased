//! `homebased daemon` commands.

use std::process::{Command, ExitCode};
use std::time::Duration;

use crate::callback::{deliver_exit_event, exit_event};
use crate::client::Client;
use crate::daemon::web::WebListen;
use crate::error::AppError;
use crate::install::{self, HostUnitState};
use crate::store::{CancelResult, Store};
use clap::Subcommand;
use serde_json::{Value, json};

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
    Serve {
        /// Dashboard listen address, or `off`. Default `127.0.0.1:7677`.
        /// There is no application login or token: any peer that can reach the
        /// dashboard can read task data and every regular file available to the
        /// daemon user. Bind a Tailscale or LAN address only on a trusted
        /// network (for example `100.x.y.z:7677`).
        #[arg(long, env = install::WEB_LISTEN_ENV, default_value_t)]
        web_listen: WebListen,
    },
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

/// How stop/restart reach the selected home's daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleRoute {
    /// Installed unit matches this home; use systemctl/launchctl.
    HostSupervisor,
    /// No matching unit; signal or respawn through the selected socket.
    Standalone,
}

impl LifecycleRoute {
    fn from_unit_state(state: &HostUnitState) -> Self {
        match state {
            HostUnitState::SelectedHome => Self::HostSupervisor,
            HostUnitState::Absent
            | HostUnitState::OtherHome { .. }
            | HostUnitState::Unrecognized => Self::Standalone,
        }
    }
}

/// Dispatch a daemon command.
pub async fn run(ctx: &Ctx, command: DaemonCommand) -> Result<ExitCode, AppError> {
    match command {
        DaemonCommand::Install { dry_run } => install(ctx, dry_run),
        DaemonCommand::Uninstall { yes } => uninstall(ctx, yes).await,
        DaemonCommand::Serve { web_listen } => {
            crate::daemon::serve(ctx.home.clone(), web_listen).await?;
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
    let state = install::inspect_host_unit(&ctx.home)?;
    match &state {
        HostUnitState::OtherHome { configured } => {
            return Err(AppError::HostUnitHomeMismatch {
                selected: ctx.home.root().to_path_buf(),
                configured: configured.clone(),
            });
        }
        HostUnitState::Unrecognized => {
            return Err(AppError::UnitInvalid {
                message: "host unit daemon invocation is unrecognized".into(),
            });
        }
        HostUnitState::Absent | HostUnitState::SelectedHome => {}
    }
    stop_with_route(ctx, yes, &state).await?;
    install::uninstall(&ctx.home)?;
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
    let body = if ctx.home.sock_path().exists() {
        client.get("/v1/status").await.ok()
    } else {
        None
    };
    let socket_up = body.is_some();
    // only the running daemon knows the port it bound, so a down socket means
    // no dashboard URL to print
    let web = body
        .as_ref()
        .and_then(|body| body.get("web"))
        .and_then(Value::as_str);
    let store = Store::open(&ctx.home.db_path())?;
    let in_flight = store.in_flight_count()?;
    match ctx.output {
        super::OutputMode::Quiet => println!("{in_flight}"),
        super::OutputMode::Json => ctx.print_json(json!({
            "api_version": crate::domain::API_VERSION,
            "socket": if socket_up { "up" } else { "down" },
            "in_flight": in_flight,
            "home": ctx.home.root(),
            "web": web,
        }))?,
        super::OutputMode::Human => {
            println!(
                "socket: {}  in_flight: {in_flight}  home: {}  web: {}",
                if socket_up { "up" } else { "down" },
                ctx.home.root().display(),
                web.unwrap_or("down"),
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn stop(ctx: &Ctx, yes: bool) -> Result<ExitCode, AppError> {
    let state = install::inspect_host_unit(&ctx.home)?;
    stop_with_route(ctx, yes, &state).await?;
    ctx.print_id(
        "stopped",
        "stopped",
        json!({"api_version": crate::domain::API_VERSION, "stopped": true}),
    )?;
    Ok(ExitCode::SUCCESS)
}

async fn stop_with_route(ctx: &Ctx, yes: bool, state: &HostUnitState) -> Result<(), AppError> {
    let client = Client::new(ctx.home.sock_path());
    let socket_up = client.get("/v1/status").await.ok();
    ensure_idle_or_cancel(ctx, yes, socket_up.is_some()).await?;
    let route = match LifecycleRoute::from_unit_state(state) {
        LifecycleRoute::HostSupervisor => {
            let current = install::inspect_host_unit(&ctx.home)?;
            LifecycleRoute::from_unit_state(&current)
        }
        LifecycleRoute::Standalone => LifecycleRoute::Standalone,
    };
    match route {
        LifecycleRoute::HostSupervisor => {
            install::host_stop()?;
        }
        LifecycleRoute::Standalone => {
            if let Some(body) = socket_up
                && let Some(pid) = body.get("pid").and_then(Value::as_u64)
            {
                // the daemon may already have exited between the status call and here
                if let Err(err) = nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid as i32),
                    nix::sys::signal::Signal::SIGTERM,
                ) {
                    eprintln!("warning: SIGTERM daemon {pid}: {err}");
                }
            }
        }
    }
    wait_socket_gone(&ctx.home.sock_path(), Duration::from_secs(15))?;
    Ok(())
}

async fn restart(ctx: &Ctx) -> Result<ExitCode, AppError> {
    let state = install::inspect_host_unit(&ctx.home)?;
    let route = match LifecycleRoute::from_unit_state(&state) {
        LifecycleRoute::HostSupervisor => {
            let current = install::inspect_host_unit(&ctx.home)?;
            LifecycleRoute::from_unit_state(&current)
        }
        LifecycleRoute::Standalone => LifecycleRoute::Standalone,
    };
    match route {
        LifecycleRoute::HostSupervisor => {
            install::host_restart()?;
        }
        LifecycleRoute::Standalone => {
            let client = Client::new(ctx.home.sock_path());
            if let Ok(body) = client.get("/v1/status").await {
                if let Some(pid) = body.get("pid").and_then(Value::as_u64) {
                    // the daemon may already have exited between the status call and here
                    if let Err(err) = nix::sys::signal::kill(
                        nix::unistd::Pid::from_raw(pid as i32),
                        nix::sys::signal::Signal::SIGTERM,
                    ) {
                        eprintln!("warning: SIGTERM daemon {pid}: {err}");
                    }
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
    wait_terminal_and_callback(&store, Duration::from_secs(60))?;
    Ok(())
}

async fn cancel_in_flight(ctx: &Ctx, store: &Store, socket_up: bool) -> Result<(), AppError> {
    let tasks = store.non_terminal()?;
    if socket_up {
        let client = Client::new(ctx.home.sock_path());
        for row in tasks {
            // cancel is idempotent and the daemon may be shutting down already
            if let Err(err) = client
                .post(&format!("/v1/tasks/{}/cancel", row.id), &json!({}))
                .await
            {
                eprintln!("warning: cancel {}: {err}", row.id);
            }
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
                if let Some(pid) = row.pid()
                    && let Err(err) = nix::sys::signal::kill(
                        nix::unistd::Pid::from_raw(pid),
                        nix::sys::signal::Signal::SIGTERM,
                    )
                {
                    eprintln!("warning: SIGTERM worker {pid}: {err}");
                }
            }
        }
    }
    Ok(())
}

fn wait_terminal_and_callback(store: &Store, budget: Duration) -> Result<(), AppError> {
    let start = std::time::Instant::now();
    loop {
        let busy = store.in_flight_count()? > 0
            || store
                .list_tasks(&[], None)?
                .iter()
                .any(|row| row.state.is_terminal() && row.callback_outstanding());
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
