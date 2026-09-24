//! Hidden `resource-release-watcher` command run as one authority-bound Homebased task

use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use crate::cli::Ctx;
use crate::client::Client;
use crate::domain::TaskId;
use crate::error::AppError;
use crate::resource::release_watcher::{
    RELEASE_WATCHER_POLL_PATH, RELEASE_WATCHER_PROTOCOL_VERSION, ReleaseWatcherCommand,
    ReleaseWatcherPollOutcome, ReleaseWatcherPollRequest, ReleaseWatcherPollResponse,
};
use crate::resource::{ActionId, ReleaseWatcherTaskId, ResourceId, ResourceRevision};

const TASK_ID_ENV: &str = "HOMEBASED_TASK_ID";

// a checkpoint arrives tens of minutes apart, so a short first poll catches quick
// transitions and the capped backoff keeps the long wait cheap
const WATCHER_POLL_TIMING: PollTiming = PollTiming {
    initial: Duration::from_millis(250),
    max: Duration::from_secs(10),
};

/// Exact release identities from the authority-built watcher command
#[derive(Debug, clap::Args)]
pub struct ReleaseWatcherArgs {
    /// Resource whose release action owns this watcher
    #[arg(long)]
    resource_id: ResourceId,
    /// Stable release action identity
    #[arg(long)]
    action_id: ActionId,
    /// Resource revision saved with the release action
    #[arg(long)]
    state_revision: u64,
    /// Exact registered trainer task
    #[arg(long)]
    trainer_task_id: TaskId,
    /// Preallocated task identity of this watcher
    #[arg(long)]
    watcher_task_id: TaskId,
}

impl ReleaseWatcherArgs {
    fn command(&self) -> ReleaseWatcherCommand {
        ReleaseWatcherCommand {
            resource_id: self.resource_id,
            action_id: self.action_id,
            state_revision: ResourceRevision::new(self.state_revision),
            trainer_task_id: self.trainer_task_id,
            watcher_task_id: ReleaseWatcherTaskId::new(self.watcher_task_id),
        }
    }
}

/// Poll the local authority until this watcher's part of the release action ends
pub async fn run(ctx: &Ctx, args: ReleaseWatcherArgs) -> Result<ExitCode, AppError> {
    let command = args.command();
    check_task_identity(
        std::env::var(TASK_ID_ENV).ok().as_deref(),
        command.watcher_task_id,
    )?;
    let client = Client::new(ctx.home.sock_path());
    let request = ReleaseWatcherPollRequest::new(command);
    let outcome = poll_until_final(&client, &request, ctx.home.root(), WATCHER_POLL_TIMING).await?;

    Ok(report(&outcome))
}

/// Refuse to act unless Homebased runs this process as the bound watcher task
fn check_task_identity(
    task_id: Option<&str>,
    expected: ReleaseWatcherTaskId,
) -> Result<(), AppError> {
    let expected = expected.as_task_id();
    match task_id.map(str::parse::<TaskId>) {
        Some(Ok(task_id)) if task_id == expected => Ok(()),
        Some(Ok(task_id)) => Err(AppError::Usage {
            message: format!("release watcher {expected} is running as task {task_id}"),
        }),
        Some(Err(_)) | None => Err(AppError::Usage {
            message: format!("release watcher {expected} must run as its Homebased task"),
        }),
    }
}

#[derive(Debug, Clone, Copy)]
struct PollTiming {
    initial: Duration,
    max: Duration,
}

/// Send the same poll until the authority returns a final outcome
///
/// Socket outages and unknown replies are retried with the identical request, so
/// a lost reply resolves through the authority's saved decision. Only changed
/// observations are logged, and the backoff resets when the observation changes
async fn poll_until_final(
    client: &Client,
    request: &ReleaseWatcherPollRequest,
    home_root: &Path,
    timing: PollTiming,
) -> Result<ReleaseWatcherPollOutcome, AppError> {
    let mut delay = timing.initial;
    let mut last_observation = None;
    loop {
        let observation = match client
            .post_json::<_, ReleaseWatcherPollResponse>(RELEASE_WATCHER_POLL_PATH, request)
            .await
        {
            Ok(response) if response.protocol_version == RELEASE_WATCHER_PROTOCOL_VERSION => {
                if response.outcome.is_final() {
                    return Ok(response.outcome);
                }
                PollObservation::Waiting(response.outcome)
            }
            Ok(_) => PollObservation::UncertainReply,
            Err(AppError::DaemonUnavailable { .. }) => PollObservation::DaemonUnavailable,
            Err(AppError::Internal { .. }) => PollObservation::UncertainReply,
            Err(error) => return Err(error),
        };
        // an absent state directory cannot come back with this watcher's records
        if observation.is_retry() && !home_root.exists() {
            return Err(AppError::DaemonUnavailable {
                message: format!("state directory {} was removed", home_root.display()),
            });
        }

        if last_observation.as_ref() != Some(&observation) {
            println!("{}", observation.describe());
            last_observation = Some(observation);
            delay = timing.initial;
        }
        tokio::time::sleep(delay).await;
        delay = delay.saturating_mul(2).min(timing.max);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PollObservation {
    Waiting(ReleaseWatcherPollOutcome),
    DaemonUnavailable,
    UncertainReply,
}

impl PollObservation {
    fn is_retry(&self) -> bool {
        matches!(self, Self::DaemonUnavailable | Self::UncertainReply)
    }

    fn describe(&self) -> String {
        match self {
            Self::Waiting(ReleaseWatcherPollOutcome::WatcherNotRunning) => {
                "waiting for this watcher's running task identity".into()
            }
            Self::Waiting(ReleaseWatcherPollOutcome::WaitingForTrainerStart) => {
                "waiting for the trainer task to start".into()
            }
            Self::Waiting(ReleaseWatcherPollOutcome::WaitingForCheckpoint) => {
                "waiting for a new complete checkpoint".into()
            }
            Self::Waiting(ReleaseWatcherPollOutcome::CompletedResultAwaitingTrainerExit) => {
                "final result published; waiting for the trainer to exit without a stop".into()
            }
            Self::Waiting(outcome) => format!("waiting: {outcome:?}"),
            Self::DaemonUnavailable => "daemon socket unavailable; retrying the same poll".into(),
            Self::UncertainReply => "uncertain daemon reply; retrying the same poll".into(),
        }
    }
}

fn report(outcome: &ReleaseWatcherPollOutcome) -> ExitCode {
    match outcome {
        ReleaseWatcherPollOutcome::StopCommitted {
            generation_id,
            cancel_requested_at,
        } => {
            println!(
                "trainer stop committed after checkpoint {generation_id} at {cancel_requested_at}"
            );
            ExitCode::SUCCESS
        }
        ReleaseWatcherPollOutcome::TrainerCompleted => {
            println!("trainer completed with its final result; no stop was requested");
            ExitCode::SUCCESS
        }
        ReleaseWatcherPollOutcome::ReleaseSettled => {
            println!("release action is already settled");
            ExitCode::SUCCESS
        }
        ReleaseWatcherPollOutcome::Attention { reason } => {
            eprintln!("release watcher needs attention: {reason:?}");
            ExitCode::FAILURE
        }
        ReleaseWatcherPollOutcome::WatcherNotRunning
        | ReleaseWatcherPollOutcome::WaitingForTrainerStart
        | ReleaseWatcherPollOutcome::WaitingForCheckpoint
        | ReleaseWatcherPollOutcome::CompletedResultAwaitingTrainerExit => {
            eprintln!("release watcher stopped before a final outcome: {outcome:?}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::Bytes;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::post;
    use clap::Parser;
    use tempfile::tempdir;

    use super::{PollTiming, check_task_identity, poll_until_final};
    use crate::cli::{Cli, Command};
    use crate::client::Client;
    use crate::domain::TaskId;
    use crate::error::AppError;
    use crate::resource::release_watcher::{
        RELEASE_WATCHER_POLL_PATH, RELEASE_WATCHER_PROTOCOL_VERSION, ReleaseWatcherCommand,
        ReleaseWatcherPollOutcome, ReleaseWatcherPollRequest, ReleaseWatcherPollResponse,
    };
    use crate::resource::{ActionId, ReleaseWatcherTaskId, ResourceId, ResourceRevision};
    use std::path::Path;
    use std::time::Duration;

    const FAST: PollTiming = PollTiming {
        initial: Duration::from_millis(10),
        max: Duration::from_millis(40),
    };

    fn command() -> ReleaseWatcherCommand {
        ReleaseWatcherCommand {
            resource_id: ResourceId::new(),
            action_id: ActionId::new(),
            state_revision: ResourceRevision::new(2),
            trainer_task_id: TaskId::new(),
            watcher_task_id: ReleaseWatcherTaskId::new(TaskId::new()),
        }
    }

    #[test]
    fn canonical_argv_parses_as_the_hidden_command() {
        let command = command();
        let argv = command.argv(Path::new("/usr/local/bin/homebased")).unwrap();
        let cli = Cli::try_parse_from(argv).unwrap();
        let Command::ResourceReleaseWatcher(args) = cli.command else {
            panic!("canonical argv must select the hidden watcher command");
        };
        assert_eq!(args.command(), command);
    }

    #[test]
    fn watcher_refuses_to_run_outside_its_bound_task() {
        let watcher = ReleaseWatcherTaskId::new(TaskId::new());
        assert!(check_task_identity(Some(&watcher.as_task_id().to_string()), watcher).is_ok());
        assert!(matches!(
            check_task_identity(Some(&TaskId::new().to_string()), watcher),
            Err(AppError::Usage { .. })
        ));
        assert!(matches!(
            check_task_identity(Some("not-a-task"), watcher),
            Err(AppError::Usage { .. })
        ));
        assert!(matches!(
            check_task_identity(None, watcher),
            Err(AppError::Usage { .. })
        ));
    }

    #[tokio::test]
    async fn socket_outage_and_unknown_replies_retry_the_identical_poll() {
        let directory = tempdir().unwrap();
        let socket = directory.path().join("homebased.sock");
        let request = ReleaseWatcherPollRequest::new(command());
        let bodies = Arc::new(Mutex::new(Vec::<Bytes>::new()));

        let server_socket = socket.clone();
        let server_bodies = bodies.clone();
        let server = tokio::spawn(async move {
            // the daemon socket is absent for the watcher's first polls
            tokio::time::sleep(Duration::from_millis(60)).await;
            let listener = tokio::net::UnixListener::bind(&server_socket).unwrap();
            let router = Router::new().route(
                RELEASE_WATCHER_POLL_PATH,
                post(move |body: Bytes| {
                    let bodies = server_bodies.clone();
                    async move {
                        let count = {
                            let mut bodies = bodies.lock().unwrap();
                            bodies.push(body);
                            bodies.len()
                        };
                        let outcome = match count {
                            1 => {
                                return (StatusCode::INTERNAL_SERVER_ERROR, "lost").into_response();
                            }
                            2 => ReleaseWatcherPollOutcome::WaitingForCheckpoint,
                            _ => ReleaseWatcherPollOutcome::StopCommitted {
                                generation_id: "generation-2".into(),
                                cancel_requested_at: chrono::Utc::now(),
                            },
                        };
                        axum::Json(ReleaseWatcherPollResponse {
                            protocol_version: RELEASE_WATCHER_PROTOCOL_VERSION,
                            outcome,
                        })
                        .into_response()
                    }
                }),
            );
            axum::serve(listener, router).await.unwrap();
        });

        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            poll_until_final(&Client::new(socket), &request, directory.path(), FAST),
        )
        .await
        .unwrap()
        .unwrap();
        server.abort();

        assert!(matches!(
            outcome,
            ReleaseWatcherPollOutcome::StopCommitted { ref generation_id, .. }
                if generation_id == "generation-2"
        ));
        let bodies = bodies.lock().unwrap();
        assert_eq!(bodies.len(), 3);
        for body in bodies.iter() {
            assert_eq!(
                serde_json::from_slice::<ReleaseWatcherPollRequest>(body).unwrap(),
                request
            );
        }
    }

    #[tokio::test]
    async fn removed_state_directory_ends_an_outage_retry() {
        let directory = tempdir().unwrap();
        let home = directory.path().join("home");
        let request = ReleaseWatcherPollRequest::new(command());

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            poll_until_final(
                &Client::new(home.join("homebased.sock")),
                &request,
                &home,
                FAST,
            ),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(AppError::DaemonUnavailable { .. })));
    }
}
