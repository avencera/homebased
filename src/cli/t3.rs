//! Commands for `homebased t3`

use std::io::Read;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Subcommand};
use serde_json::{Value, to_value};
use uuid::Uuid;

use crate::client::Client;
use crate::daemon::t3_launch::{
    T3LaunchRequest, T3LaunchResponse, T3ProjectAddRequest, T3ProjectAddResponse, T3ProjectsBody,
    T3ProjectsRequest,
};
use crate::domain::{API_VERSION, THREAD_ENV_VARS, ThreadId};
use crate::error::{AppError, LocalBase};
use crate::t3::{
    CloneSpec, IconChange, IconColor, InteractionMode, LaunchSpec, ModelChoice, ModelOption,
    OptionValue, ProbeReport, ProbeStatus, ProjectIcon, ProjectSpec, RepoKey, RuntimeMode, T3Env,
    Workspace, probe,
};
use crate::thread_ref::Caller;

use super::{Ctx, OutputMode};

/// T3 Code commands
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum T3Command {
    /// Check the installed T3 Code local API
    Check,
    /// Start a new T3 Code thread on this or another Fleet machine
    ///
    /// The thread is an ordinary top-level T3 thread on that machine, as if
    /// started there. With --reply-route, the first message ends with a line
    /// naming the calling thread, so the new agent can report back with
    /// `homebased message send`. With --child, the new thread is a child
    /// thread of the calling T3 thread: homebased records the link, lists the
    /// child under `thread context --children`, keeps the parent `waiting`
    /// until the child reports with `homebased thread report`, and settles
    /// the child after it reports
    Launch(Box<LaunchArgs>),
    /// List the T3 Code projects on this or another Fleet machine
    Projects {
        /// Machine name or UUID; this machine when omitted
        #[arg(long)]
        machine: Option<String>,
    },
    /// Manage the T3 Code projects on this or another Fleet machine
    #[command(subcommand)]
    Project(ProjectCommand),
}

/// `homebased t3 project` commands
#[derive(Debug, Subcommand)]
pub enum ProjectCommand {
    /// Add an existing directory as a T3 Code project
    ///
    /// A directory that already is a project reports that project instead,
    /// with the icon flags applied to it
    Add {
        /// Directory on that machine: absolute, starting with `~/`, or
        /// relative to the current directory, which names the same place
        /// under that machine's home
        path: String,
        /// Project title; the directory name when omitted
        #[arg(long)]
        title: Option<String>,
        #[command(flatten)]
        icon: IconArgs,
        /// Machine name or UUID; this machine when omitted
        #[arg(long)]
        machine: Option<String>,
    },
    /// Set or clear the icon of an existing T3 Code project
    Icon {
        /// Project directory on that machine, as for `add`
        path: String,
        #[command(flatten)]
        icon: IconArgs,
        /// Machine name or UUID; this machine when omitted
        #[arg(long)]
        machine: Option<String>,
    },
}

/// Project icon flags: `--lucide` or `--monogram` with `--color`, `--emoji`, or
/// `--clear-icon`
#[derive(Debug, Args)]
pub struct IconArgs {
    /// Lucide icon name, such as `audio-waveform`; see https://lucide.dev/icons
    #[arg(long, value_name = "NAME")]
    lucide: Option<String>,
    /// Emoji icon
    #[arg(long)]
    emoji: Option<String>,
    /// Letters on a colored tile
    #[arg(long, value_name = "TEXT")]
    monogram: Option<String>,
    /// Tile color of a Lucide or monogram icon
    #[arg(long, value_enum)]
    color: Option<IconColor>,
    /// Remove the icon, so T3 shows the project's favicon again
    #[arg(long)]
    clear_icon: bool,
}

impl IconArgs {
    /// The icon change these flags ask for; `None` when they name none
    fn change(self) -> Result<Option<IconChange>, AppError> {
        let icon = match (
            self.lucide,
            self.emoji,
            self.monogram,
            self.color,
            self.clear_icon,
        ) {
            (None, None, None, None, false) => return Ok(None),
            (None, None, None, None, true) => return Ok(Some(IconChange::Clear)),
            (Some(name), None, None, Some(color), false) => ProjectIcon::Lucide { name, color },
            (None, Some(emoji), None, None, false) => ProjectIcon::Emoji { emoji },
            (None, None, Some(text), Some(color), false) => ProjectIcon::Monogram { text, color },
            _ => {
                return Err(AppError::Usage {
                    message: "give one icon: --lucide or --monogram with --color, --emoji, or --clear-icon".into(),
                });
            }
        };
        Ok(Some(IconChange::Set(icon)))
    }
}

/// `homebased t3 launch` flags
#[derive(Debug, Args)]
pub struct LaunchArgs {
    /// Machine name or UUID; this machine when omitted
    #[arg(long)]
    machine: Option<String>,
    /// T3 project on that machine: its id, its workspace root (absolute or
    /// starting with ~/), its exact title, or the repository its origin
    /// remote names, as owner/repo or a remote URL
    #[arg(long)]
    project: String,
    /// When no project there checks out the --project repository, clone it
    /// and add it as a project first
    #[arg(long)]
    clone_if_missing: bool,
    /// Directory on that machine for --clone-if-missing; ~/code/<repo> when omitted
    #[arg(long, requires = "clone_if_missing")]
    clone_into: Option<String>,
    /// Thread title; derived from the message when omitted
    #[arg(long)]
    title: Option<String>,
    /// First message; the thread starts idle without one
    #[arg(long, conflicts_with = "message_file")]
    message: Option<String>,
    /// Read the first message from this file, or `-` for stdin
    #[arg(long)]
    message_file: Option<PathBuf>,
    /// Work in a new git worktree that starts from this branch or commit
    #[arg(long, conflicts_with = "existing_worktree")]
    worktree: Option<String>,
    /// Branch for the new worktree, or the branch to record with --existing-worktree
    #[arg(long)]
    branch: Option<String>,
    /// Fetch the --worktree base from origin first
    #[arg(long, requires = "worktree")]
    from_origin: bool,
    /// Work in this existing git worktree of the project, a path on that machine
    #[arg(long)]
    existing_worktree: Option<String>,
    /// Provider instance id on that machine, listed under `providers` by `t3 projects`
    #[arg(long, requires = "model")]
    provider: Option<String>,
    /// Model id, such as claude-opus-5-5; the project's T3 default when omitted
    #[arg(long, requires = "provider")]
    model: Option<String>,
    /// Model option as id=value, such as effort=high for Claude or
    /// reasoningEffort=high for Codex; repeatable
    #[arg(long = "model-option", value_parser = parse_model_option)]
    model_options: Vec<ModelOption>,
    /// Permission mode of the thread
    #[arg(long, value_enum, default_value_t = RuntimeMode::FullAccess)]
    runtime_mode: RuntimeMode,
    /// Start the thread in plan mode
    #[arg(long)]
    plan: bool,
    /// Stable UUID for an explicit retry after an unknown outcome
    #[arg(long)]
    request_id: Option<Uuid>,
    /// End the first message with the calling agent's thread and the command
    /// that messages it, so the new agent can report back
    #[arg(long, conflicts_with = "source_thread")]
    reply_route: bool,
    /// Like --reply-route, but name this thread instead of the calling one
    #[arg(long)]
    source_thread: Option<ThreadId>,
    /// Launch a child thread of the calling T3 thread, which reports back to
    /// it with `homebased thread report`; implies --reply-route and
    /// needs a first message
    #[arg(long, conflicts_with = "source_thread")]
    child: bool,
}

/// Run one T3 Code command
pub async fn run(ctx: &Ctx, command: T3Command) -> Result<ExitCode, AppError> {
    match command {
        T3Command::Check => check(ctx).await,
        T3Command::Launch(args) => launch(ctx, *args).await,
        T3Command::Projects { machine } => list_projects(ctx, machine).await,
        T3Command::Project(ProjectCommand::Add {
            path,
            title,
            icon,
            machine,
        }) => {
            let spec = ProjectSpec {
                path: project_path(path)?,
                title,
                icon: icon.change()?,
                existing_only: false,
            };
            add_project(ctx, spec, machine).await
        }
        T3Command::Project(ProjectCommand::Icon {
            path,
            icon,
            machine,
        }) => {
            let spec = ProjectSpec {
                path: project_path(path)?,
                title: None,
                icon: icon.change()?,
                existing_only: true,
            };
            add_project(ctx, spec, machine).await
        }
    }
}

async fn check(ctx: &Ctx) -> Result<ExitCode, AppError> {
    let report = tokio::task::spawn_blocking(|| probe(&T3Env::from_env()))
        .await
        .map_err(|error| AppError::Internal {
            message: format!("T3 probe worker failed: {error}"),
        })?;
    print_report(ctx, &report)?;
    if report.status == ProbeStatus::Changed {
        Ok(ExitCode::from(1))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

async fn launch(ctx: &Ctx, args: LaunchArgs) -> Result<ExitCode, AppError> {
    let workspace = match (args.worktree, args.existing_worktree, args.branch) {
        (Some(base_ref), None, branch) => Workspace::Worktree {
            base_ref,
            branch,
            start_from_origin: args.from_origin,
        },
        (None, Some(path), branch) => Workspace::ExistingWorktree {
            path: home_relative(path),
            branch,
        },
        (None, None, None) => Workspace::Root,
        (None, None, Some(_)) => {
            return Err(AppError::Usage {
                message: "--branch needs --worktree or --existing-worktree".into(),
            });
        }
        (Some(_), Some(_), _) => {
            return Err(AppError::Usage {
                message: "use only one of --worktree or --existing-worktree".into(),
            });
        }
    };
    let message = match (args.message, args.message_file) {
        (Some(message), None) => Some(message),
        (None, Some(path)) => Some(read_message(&path)?),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(AppError::Usage {
                message: "use only one of --message or --message-file".into(),
            });
        }
    };
    let model = args
        .provider
        .zip(args.model)
        .map(|(provider, model)| ModelChoice { provider, model });
    // launches are hand-offs: the user takes over the new thread, so it only
    // learns how to reach this one when asked. A child's route names the
    // caller's T3 thread, which the daemon finds
    let (source_thread, child_of) = match (args.child, args.reply_route, args.source_thread) {
        (true, _, _) => (None, Some(child_caller()?)),
        (false, true, _) => (
            Some(calling_thread().ok_or_else(|| AppError::Usage {
                message: "--reply-route found no calling agent thread; pass --source-thread".into(),
            })?),
            None,
        ),
        (false, false, thread) => (thread, None),
    };
    if child_of.is_some() && message.is_none() {
        return Err(AppError::Usage {
            message: "--child needs --message or --message-file, the work the child thread owns"
                .into(),
        });
    }

    let request = T3LaunchRequest {
        api_version: API_VERSION,
        machine: args.machine,
        source_thread,
        child_of,
        launch: LaunchSpec {
            request_id: args.request_id.unwrap_or_else(Uuid::now_v7),
            project: home_relative(args.project),
            clone: args.clone_if_missing.then(|| CloneSpec {
                into: args.clone_into.map(home_relative),
            }),
            title: args.title,
            message,
            workspace,
            model,
            model_options: args.model_options,
            runtime_mode: args.runtime_mode,
            interaction_mode: if args.plan {
                InteractionMode::Plan
            } else {
                InteractionMode::Default
            },
        },
    };
    request
        .launch
        .validate()
        .map_err(|message| AppError::Usage { message })?;

    let response: T3LaunchResponse = Client::new(ctx.home.sock_path())
        .post_json("/v1/t3/launch", &request)
        .await
        .map_err(|error| match error {
            // the socket can drop after the daemon sent the launch on, and a
            // retry needs this generated id to return the same thread
            AppError::DaemonUnavailable { message } => AppError::T3LaunchOutcomeUnknown {
                request: request.launch.request_id,
                machine: None,
                message,
            },
            // the base may be a branch or commit only this checkout has
            AppError::T3WorktreeBaseMissing {
                base_ref,
                workspace_root,
                fetched,
                repository,
                local: None,
            } => AppError::T3WorktreeBaseMissing {
                local: repository
                    .as_deref()
                    .and_then(|repository| local_base(repository, &base_ref)),
                base_ref,
                workspace_root,
                fetched,
                repository,
            },
            error => error,
        })?;
    match ctx.output {
        OutputMode::Json => ctx.print_json(to_value(&response)?)?,
        OutputMode::Quiet => println!("{}", response.thread.thread_id),
        OutputMode::Human => {
            let verb = if response.thread.resumed {
                "already launched"
            } else {
                "launched"
            };
            println!(
                "{verb} thread {} on {} in {} ({})",
                response.thread.thread_id,
                response.machine_name,
                response.thread.project_title,
                response.thread.workspace_root
            );
            if let Some(link) = &response.thread.link {
                println!("open: {link}");
            }
            if let Some(lineage) = &response.lineage {
                println!(
                    "child of thread_{}; it reports back with `homebased thread report`",
                    lineage.parent.t3_thread
                );
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

async fn list_projects(ctx: &Ctx, machine: Option<String>) -> Result<ExitCode, AppError> {
    let request = T3ProjectsRequest {
        api_version: API_VERSION,
        machine,
    };
    let body: T3ProjectsBody = Client::new(ctx.home.sock_path())
        .post_json("/v1/t3/projects", &request)
        .await?;
    match ctx.output {
        OutputMode::Json => ctx.print_json(to_value(&body)?)?,
        OutputMode::Quiet => {
            for project in &body.projects {
                println!("{}", project.id);
            }
        }
        OutputMode::Human => {
            for project in &body.projects {
                let model = project
                    .default_model
                    .as_ref()
                    .map_or_else(|| "no default model".to_owned(), model_label);
                println!(
                    "{}  {}  {}  {model}",
                    project.title, project.workspace_root, project.id
                );
            }
            if let Some(providers) = &body.providers {
                let ids: Vec<&str> = providers
                    .iter()
                    .map(|provider| provider.id.as_str())
                    .collect();
                println!("enabled providers: {}", ids.join(", "));
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// A project path as the target machine reads it: absolute or under `~/`
fn project_path(path: String) -> Result<String, AppError> {
    let path = if path.starts_with('/') || path.starts_with("~/") {
        path
    } else {
        std::path::absolute(&path)?.to_string_lossy().into_owned()
    };
    Ok(home_relative(path))
}

/// Add a project or change an existing one's icon, as `spec` says
async fn add_project(
    ctx: &Ctx,
    spec: ProjectSpec,
    machine: Option<String>,
) -> Result<ExitCode, AppError> {
    let request = T3ProjectAddRequest {
        api_version: API_VERSION,
        machine,
        project: spec,
    };
    request
        .project
        .validate()
        .map_err(|message| AppError::Usage { message })?;

    let response: T3ProjectAddResponse = Client::new(ctx.home.sock_path())
        .post_json("/v1/t3/projects/add", &request)
        .await?;
    let project = &response.project.project;
    match ctx.output {
        OutputMode::Json => ctx.print_json(to_value(&response)?)?,
        OutputMode::Quiet => println!("{}", project.id),
        OutputMode::Human => {
            let verb = match (request.project.existing_only, response.project.existed) {
                (true, _) => "project",
                (false, true) => "already a project",
                (false, false) => "added project",
            };
            let icon = match &request.project.icon {
                Some(IconChange::Set(_)) => ", icon set",
                Some(IconChange::Clear) => ", icon cleared",
                None => "",
            };
            println!(
                "{verb}: {} on {} ({}, {}){icon}",
                project.title, response.machine_name, project.workspace_root, project.id
            );
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// `instance/model` of a T3 model selection
fn model_label(selection: &Value) -> String {
    let field = |key| selection.get(key).and_then(Value::as_str).unwrap_or("?");
    format!("{}/{}", field("instanceId"), field("model"))
}

/// A path under this machine's home as `~/...`, so another machine
/// resolves it under its own home
///
/// The shell expands an unquoted `~/code/app` to this machine's home before
/// homebased sees it, which names nothing on a machine with another home
pub(super) fn home_relative(value: String) -> String {
    let Some(home) = std::env::home_dir() else {
        return value;
    };
    let home = home.to_string_lossy();
    match value.strip_prefix(home.trim_end_matches('/')) {
        Some(rest) if rest.starts_with('/') => format!("~{rest}"),
        Some("") => "~/".into(),
        _ => value,
    }
}

/// What the checkout in the current directory has of `base_ref`, which the
/// launching machine lacks; `None` when it is not a checkout of `repository`
/// or has nothing of that name either
fn local_base(repository: &str, base_ref: &str) -> Option<LocalBase> {
    let git = |args: &[&str]| {
        std::process::Command::new("git")
            .args(args)
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    };
    // another repository's branch of the same name says nothing
    let origin = git(&["config", "--get", "remote.origin.url"])?;
    if RepoKey::from_remote(&origin)?.as_str() != repository {
        return None;
    }
    let spec = format!("{base_ref}^{{commit}}");
    let commit = git(&[
        "rev-parse",
        "--verify",
        "--quiet",
        "--end-of-options",
        &spec,
    ])?;
    // the launching machine fetches a branch by name, so a branch counts as
    // pushed only when origin has that name, whatever its commit is on
    let branch = format!("refs/heads/{base_ref}");
    if git(&["show-ref", "--verify", "--quiet", &branch]).is_some() {
        let tracking = format!("refs/remotes/origin/{base_ref}");
        let pushed = git(&["show-ref", "--verify", "--quiet", &tracking]).is_some();
        return Some(if pushed {
            LocalBase::Pushed
        } else {
            LocalBase::UnpushedBranch
        });
    }

    // a commit origin already has gets no local hint, since a launch cannot
    // fetch a bare commit by name
    let pushed = git(&[
        "branch",
        "--remotes",
        "--list",
        "origin/*",
        "--contains",
        &commit,
    ])
    .is_some_and(|branches| !branches.is_empty());
    (!pushed).then_some(LocalBase::UnpushedCommit { commit })
}

/// The calling agent, which a child thread reports to
fn child_caller() -> Result<Caller, AppError> {
    let caller = Caller::from_env()?;
    if caller.t3_thread.is_none() && caller.session.is_none() {
        return Err(AppError::Usage {
            message: "--child found no calling agent thread; run it from a T3 Code thread".into(),
        });
    }
    Ok(caller)
}

/// The agent thread this command runs in, from the variables Codex and Claude Code set
fn calling_thread() -> Option<ThreadId> {
    THREAD_ENV_VARS
        .iter()
        .filter_map(std::env::var_os)
        .find_map(|value| value.to_str()?.parse().ok())
}

fn read_message(path: &PathBuf) -> Result<String, AppError> {
    if path.as_os_str() == "-" {
        let mut message = String::new();
        std::io::stdin().read_to_string(&mut message)?;
        return Ok(message);
    }
    std::fs::read_to_string(path).map_err(|error| AppError::FileNotFound {
        message: format!("read launch message {}: {error}", path.display()),
    })
}

fn parse_model_option(raw: &str) -> Result<ModelOption, String> {
    let (id, value) = raw
        .split_once('=')
        .ok_or_else(|| format!("expected id=value, got {raw:?}"))?;
    let value = match value {
        "true" => OptionValue::Flag(true),
        "false" => OptionValue::Flag(false),
        text => OptionValue::Text(text.to_owned()),
    };
    Ok(ModelOption {
        id: id.to_owned(),
        value,
    })
}

fn print_report(ctx: &Ctx, report: &ProbeReport) -> Result<(), AppError> {
    match ctx.output {
        OutputMode::Json => ctx.print_json(to_value(report)?),
        OutputMode::Quiet => {
            println!("{}", report.status.as_str());
            Ok(())
        }
        OutputMode::Human => {
            match report.orchestration_protocol {
                Some(protocol) => println!(
                    "t3 check: {} (orchestration protocol {protocol})",
                    report.status.as_str()
                ),
                None => println!("t3 check: {}", report.status.as_str()),
            }
            for check in &report.checks {
                let result = if check.ok { "ok" } else { "failed" };
                println!("{result} {}: {}", check.name, check.detail);
            }
            Ok(())
        }
    }
}
