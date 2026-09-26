//! Resource CLI commands over the daemon Unix socket

use crate::resource::CommandSpecError;
use crate::resource::trainer_publication::AttemptBinding;
use std::io::{self, Read};
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use chrono::SecondsFormat;
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::client::Client;
use crate::daemon::fleet_api::MachinesBody;
use crate::domain::{API_VERSION, THREAD_ENV_VARS, TaskEnv, TaskId, ThreadId};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::resource::api::{
    BrowserResourceAction, InitialIdleBody, InitialIdleResponse, OperatorReleaseBody,
    OperatorReleaseResponse, PendingActionList, PendingActionPhase, PendingActionView,
    QueuePlacement, RESOURCE_PENDING_PATH, RESOURCE_REGISTER_PATH, RequestCancelBody,
    ResourceActionBody, ResourceBackgroundSubmitOutcome, ResourceBackgroundSubmitResponse,
    ResourceDetail, ResourceRegisterBody, ResourceRegistration, ResourceRequestSubmitOutcome,
    ResourceRequestSubmitResponse, SupervisorReplacementBody, TrainerAttemptBody,
    TrainerAttemptResponse, UnavailableAuthority,
};
use crate::resource::bound_action::{
    LocalReturnAcceptance, RESOURCE_ACTION_SUBMIT_PATH, ResourceActionChoice, ResourceActionKind,
    ResourceActionRejection, ResourceActionSubmitOutcome, ResourceActionSubmitRequest,
    ResourceActionSubmitResponse,
};
use crate::resource::initial_idle::InitialIdleAttestation;
use crate::resource::operator_release::OperatorGpuFreeAttestation;
use crate::resource::{
    ActionId, CommandSpec, ResourceId, ResourceRevision, ReturnContext, ReturnDecision,
    ReturnDecisionRejection, ReturnLaunch, ReturnWork, SupervisorActionAuthority,
    SupervisorAddress, SupervisorNotice, SupervisorNoticeDelivery,
};
use crate::spec::{self, NormalizedSpec};
use crate::submission::RequestId;

use super::{Ctx, OutputMode};

/// Resource commands. All requests use the local daemon's Unix socket
#[derive(Debug, Subcommand)]
#[command(after_help = crate::cli::AFTER_HELP)]
pub enum ResourceCommand {
    /// Print schemas for resource registration, work, trainer attempts, and operator release
    Schema,
    /// Register a resource on the local authority machine from a JSON spec
    Register {
        /// Registration spec file, or `-` for stdin
        #[arg(long)]
        spec: String,
    },
    /// Record that a resource with no history starts with a free GPU
    ///
    /// Use it once, on the authority machine, for a newly registered resource
    /// that has no background run, loan, or release record, after you inspected
    /// its GPU. Queued requests then serve. This is a human confirmation, not
    /// automatic proof. Reuse the exact same document after an unknown result
    InitialIdle {
        /// Complete InitialIdleAttestation JSON file, or `-` for stdin
        #[arg(long, required = true, value_name = "FILE")]
        spec: String,
    },
    /// Record a human GPU-free confirmation on the local authority machine
    ///
    /// Inspect the authority GPU before you use this command. This is a human
    /// confirmation, not automatic proof that GPU work stopped. Reuse the exact
    /// same document, operation_id, and observation after an unknown result
    OperatorRelease {
        /// Complete OperatorGpuFreeAttestation JSON file, or `-` for stdin
        #[arg(long, required = true, value_name = "FILE")]
        spec: String,
    },
    /// Show one resource and its authoritative state
    Show {
        /// Full resource UUID
        #[arg(value_name = "RESOURCE_ID")]
        resource_id: Uuid,
    },
    /// Change a resource's assigned supervisor
    Supervisor {
        /// Supervisor operation
        #[command(subcommand)]
        command: SupervisorCommand,
    },
    /// Submit command work as a resource background task
    Background {
        /// Background-task operation
        #[command(subcommand)]
        command: BackgroundCommand,
    },
    /// Submit or cancel a resource request
    Request {
        /// Request operation
        #[command(subcommand)]
        command: RequestCommand,
    },
    /// List resource requests in serving order
    Requests {
        /// Full resource UUID
        #[arg(value_name = "RESOURCE_ID")]
        resource_id: Uuid,
    },
    /// List actions assigned to one exact supervisor address
    Pending {
        /// Supervisor machine UUID
        #[arg(long)]
        machine: MachineId,
        /// Exact supervisor thread UUID
        #[arg(long)]
        thread: ThreadId,
    },
    /// Start the watcher for a pending release action on another machine
    ///
    /// When the supervisor runs on the resource authority machine, the authority
    /// starts the watcher itself and this command is refused
    ReleaseWatch {
        /// Stable release action UUID
        #[arg(value_name = "ACTION_ID")]
        action_id: Uuid,
        /// Saved JSON from `resource pending --json`; reuse it for an unknown-result retry
        #[arg(long, required = true, value_name = "FILE")]
        pending_spec: String,
    },
    /// Make an explicit return choice for a pending action, or hold it open longer
    ///
    /// Works on the same machine as the resource authority or on another machine
    /// A retry with the same identities replays the saved result and never starts
    /// the task again; a different choice for the same action is a conflict
    ///
    /// Queued requests take the resource 2 minutes after the return action opens
    /// unless a choice exists. `--hold` moves that deadline to now plus the given
    /// time, up to 10 minutes after the action opened, and is not a choice
    Return {
        /// Stable return action UUID
        #[arg(value_name = "ACTION_ID")]
        action_id: Uuid,
        /// Saved JSON from `resource pending --json`; reuse it for an unknown-result retry
        #[arg(long, required = true, value_name = "FILE")]
        pending_spec: String,
        /// JSON file containing one tagged ReturnWork choice, not a task-submit spec
        #[arg(
            long,
            conflicts_with_all = ["no_resume", "hold"],
            requires_all = ["request_id", "task_id"]
        )]
        resume_spec: Option<String>,
        /// Close the return action without starting background work and record this reason
        #[arg(
            long,
            value_name = "REASON",
            conflicts_with_all = ["resume_spec", "hold"],
            required_unless_present_any = ["resume_spec", "hold"]
        )]
        no_resume: Option<String>,
        /// Keep queued requests waiting this much longer for a choice, such as `5m`
        #[arg(
            long,
            value_name = "DURATION",
            value_parser = humantime::parse_duration,
            conflicts_with_all = ["resume_spec", "no_resume"]
        )]
        hold: Option<Duration>,
        /// Stable retry identity for the return task
        #[arg(long, requires = "resume_spec")]
        request_id: Option<Uuid>,
        /// Preallocated task identity for the return task
        #[arg(long, requires = "resume_spec")]
        task_id: Option<TaskId>,
    },
    /// Resolve a return task that ended before its start was confirmed
    ///
    /// Works on the same machine as the resource authority or on another machine
    /// A retry with the same task and reason replays the saved closure
    Resolve {
        /// Full loan UUID
        #[arg(value_name = "LOAN_ID")]
        loan_id: Uuid,
        /// Saved JSON from `resource pending --json`; reuse it for an unknown-result retry
        #[arg(long, required = true, value_name = "FILE")]
        pending_spec: String,
        /// Exact bound return task UUID
        #[arg(long)]
        task_id: TaskId,
        /// Durable supervisor resolution
        #[arg(long)]
        reason: String,
    },
    /// Retry delivery of the notice for one pending action
    Renotify {
        /// Stable action UUID whose notice must be retried
        #[arg(value_name = "ACTION_ID")]
        action_id: Uuid,
        /// Saved JSON from `resource pending --json`; reuse it for an unknown-result retry
        #[arg(long, required = true, value_name = "FILE")]
        pending_spec: String,
        /// Stable operation UUID. Reuse this value after an unknown response
        #[arg(long, required = true)]
        operation_id: Uuid,
    },
}

/// Supervisor-assignment commands
#[derive(Debug, Subcommand)]
pub enum SupervisorCommand {
    /// Set the exact supervisor machine and thread
    Set {
        /// Full resource UUID
        #[arg(value_name = "RESOURCE_ID")]
        resource_id: Uuid,
        /// Supervisor machine UUID
        #[arg(long)]
        machine: MachineId,
        /// Exact supervisor thread UUID
        #[arg(long)]
        thread: ThreadId,
        /// Compare-and-set revision from `resource show`; reuse it with the same supervisor on retry
        #[arg(long, required = true)]
        expected_revision: u64,
    },
}

/// Background-task commands
#[derive(Debug, Subcommand)]
pub enum BackgroundCommand {
    /// Submit a command as the registered background task
    Submit {
        /// Full resource UUID
        #[arg(value_name = "RESOURCE_ID")]
        resource_id: Uuid,
        /// Stable request UUID. Reuse it after an unknown response
        #[arg(long, required = true)]
        request_id: Uuid,
        /// Task spec file, or `-` for stdin
        #[arg(long)]
        spec: String,
    },
    /// Bind a trainer attempt to one registered Homebased task
    BindAttempt {
        /// Full resource UUID
        #[arg(value_name = "RESOURCE_ID")]
        resource_id: Uuid,
        /// Exact registered Homebased task UUID
        #[arg(long, required = true)]
        task_id: TaskId,
        /// Strict trainer AttemptBinding JSON file, or `-` for stdin
        #[arg(long, required = true, value_name = "FILE")]
        attempt_spec: String,
    },
}

/// Resource request commands
#[derive(Debug, Subcommand)]
pub enum RequestCommand {
    /// Submit one command to the authority-managed request queue
    Submit {
        /// Full resource UUID
        #[arg(value_name = "RESOURCE_ID")]
        resource_id: Uuid,
        /// Stable request UUID. Reuse it after an unknown response
        #[arg(long, required = true)]
        request_id: Uuid,
        /// Task spec file, or `-` for stdin
        #[arg(long)]
        spec: String,
    },
    /// Cancel one queued request before task activation
    Cancel {
        /// Full resource UUID
        #[arg(value_name = "RESOURCE_ID")]
        resource_id: Uuid,
        /// Full request UUID, not a task UUID
        #[arg(value_name = "REQUEST_ID")]
        request_id: Uuid,
        /// Compare-and-set revision from `resource show`
        #[arg(long, required = true)]
        expected_revision: u64,
        /// Stable operation UUID. Reuse it after an unknown response
        #[arg(long, required = true)]
        operation_id: Uuid,
    },
    /// Move one queued request to a new place in the serving order
    Move {
        /// Full resource UUID
        #[arg(value_name = "RESOURCE_ID")]
        resource_id: Uuid,
        /// Full request UUID, not a task UUID
        #[arg(value_name = "REQUEST_ID")]
        request_id: Uuid,
        /// Compare-and-set revision from `resource show`
        #[arg(long, required = true)]
        expected_revision: u64,
        /// Stable operation UUID. Reuse it after an unknown response
        #[arg(long, required = true)]
        operation_id: Uuid,
        /// New place in the queue
        #[command(flatten)]
        placement: PlacementArgs,
    },
}

/// Exactly one queue placement flag of `resource request move`
#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
pub struct PlacementArgs {
    /// Move the request to the front of the queue
    #[arg(long)]
    front: bool,
    /// Move the request to the back of the queue
    #[arg(long)]
    back: bool,
    /// Move the request directly before this queued request
    #[arg(long, value_name = "REQUEST_ID")]
    before: Option<Uuid>,
    /// Move the request directly after this queued request
    #[arg(long, value_name = "REQUEST_ID")]
    after: Option<Uuid>,
}

impl PlacementArgs {
    /// Convert the one flag that clap accepted into a queue placement
    fn placement(&self) -> Result<QueuePlacement, AppError> {
        if let Some(anchor) = self.before {
            validate_uuid("--before", anchor)?;
            return Ok(QueuePlacement::Before {
                request_id: RequestId(anchor),
            });
        }
        if let Some(anchor) = self.after {
            validate_uuid("--after", anchor)?;
            return Ok(QueuePlacement::After {
                request_id: RequestId(anchor),
            });
        }
        // the required single-choice group leaves only front or back here
        Ok(if self.front {
            QueuePlacement::Front
        } else {
            QueuePlacement::Back
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceRegistrationSpec {
    /// Stable identity used to make registration retries idempotent
    id: Uuid,
    /// Human-readable resource name
    display_name: String,
    /// Exact assigned supervisor
    supervisor: SupervisorAddress,
}

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ResourceSubmitBody {
    api_version: u32,
    request_id: RequestId,
    spec: NormalizedSpec,
    env: TaskEnv,
    callback_cwd: PathBuf,
}

struct ActionContext {
    resource_id: ResourceId,
    action_id: ActionId,
    authority: SupervisorActionAuthority,
    phase: PendingActionPhase,
    return_context: Option<ReturnContext>,
    notice: Option<SupervisorNotice>,
}

/// Run one resource command
pub async fn run(ctx: &Ctx, command: ResourceCommand) -> Result<ExitCode, AppError> {
    match command {
        ResourceCommand::Schema => schema(ctx),
        ResourceCommand::Register { spec } => register(ctx, &spec).await,
        ResourceCommand::OperatorRelease { spec } => operator_release(ctx, &spec).await,
        ResourceCommand::InitialIdle { spec } => initial_idle(ctx, &spec).await,
        ResourceCommand::Show { resource_id } => show(ctx, resource_id).await,
        ResourceCommand::Supervisor { command } => supervisor(ctx, command).await,
        ResourceCommand::Background { command } => background(ctx, command).await,
        ResourceCommand::Request { command } => request(ctx, command).await,
        ResourceCommand::Requests { resource_id } => requests(ctx, resource_id).await,
        ResourceCommand::Pending { machine, thread } => pending(ctx, machine, thread).await,
        ResourceCommand::ReleaseWatch {
            action_id,
            pending_spec,
        } => release_watch(ctx, action_id, &pending_spec).await,
        ResourceCommand::Return {
            action_id,
            pending_spec,
            resume_spec,
            no_resume,
            hold,
            request_id,
            task_id,
        } => {
            let choice = match hold {
                Some(hold) => ReturnChoice::Hold(hold),
                None => ReturnChoice::Decide {
                    resume_path: resume_spec.as_deref(),
                    no_resume: no_resume.as_deref(),
                    request_uuid: request_id,
                    task_id,
                },
            };
            return_action(ctx, action_id, &pending_spec, choice).await
        }
        ResourceCommand::Resolve {
            loan_id,
            pending_spec,
            task_id,
            reason,
        } => resolve(ctx, loan_id, &pending_spec, task_id, reason).await,
        ResourceCommand::Renotify {
            action_id,
            pending_spec,
            operation_id,
        } => renotify(ctx, action_id, &pending_spec, operation_id).await,
    }
}

fn schema(ctx: &Ctx) -> Result<ExitCode, AppError> {
    let value = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Homebased resource CLI inputs",
        "description": "ResourceRegistrationSpec is used by resource register. ResourceTaskSubmitSpec is used by resource background submit and resource request submit. ResourceReturnWorkSpec is used by resource return --resume-spec. ResourceTrainerAttemptBinding is used by resource background bind-attempt. OperatorGpuFreeAttestation is a human confirmation after inspecting the authority GPU, not automatic proof. InitialIdleAttestation is used by resource initial-idle for a resource with no history; it is also a human confirmation.",
        "$defs": {
            "ResourceRegistrationSpec": resource_registration_schema(),
            "ResourceTaskSubmitSpec": resource_task_schema()?,
            "ResourceReturnWorkSpec": resource_return_work_schema()?,
            "ResourceTrainerAttemptBinding": resource_trainer_attempt_binding_schema(),
            "OperatorGpuFreeAttestation": operator_gpu_free_attestation_schema(),
            "InitialIdleAttestation": initial_idle_attestation_schema(),
        },
    });
    emit(ctx, value, None, None)?;
    Ok(ExitCode::SUCCESS)
}

fn resource_registration_schema() -> Value {
    json!({
        "title": "ResourceRegistrationSpec",
        "type": "object",
        "additionalProperties": false,
        "required": ["id", "display_name", "supervisor"],
        "properties": {
            "id": { "type": "string", "format": "uuid", "description": "Stable resource UUID. Keep it unchanged when retrying registration." },
            "display_name": { "type": "string", "minLength": 1 },
            "supervisor": {
                "type": "object",
                "additionalProperties": false,
                "required": ["machine", "thread"],
                "properties": {
                    "machine": { "type": "string", "format": "uuid" },
                    "thread": { "type": "string", "format": "uuid" }
                }
            },
        }
    })
}

fn resource_task_schema() -> Result<Value, AppError> {
    let mut schema = spec::schema_json()?;
    let command_schema = schema
        .pointer("/properties/workload/oneOf")
        .and_then(Value::as_array)
        .and_then(|branches| {
            branches.iter().find(|branch| {
                branch
                    .pointer("/properties/type/const")
                    .and_then(Value::as_str)
                    == Some("task")
            })
        })
        .and_then(|branch| branch.pointer("/properties/command"))
        .cloned()
        .ok_or_else(|| invalid_daemon_response("task submit schema has no command workload"))?;
    let properties = schema
        .get_mut("properties")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| invalid_daemon_response("task submit schema has no properties"))?;
    properties.remove("machine");
    properties.insert(
        "workload".into(),
        json!({
            "description": "Finite command, or a container with gpus. Container work is accepted for queued requests and for evaluation_or_next_epoch, new_background_work, and after_ended_run returns.",
            "oneOf": [
                {
                    "title": "ResourceCommandWorkload",
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["type", "command"],
                    "properties": {
                        "type": { "const": "task" },
                        "command": command_schema
                    }
                },
                crate::container::spec::container_workload_schema(true)
            ]
        }),
    );
    if let Some(object) = schema.as_object_mut() {
        object.insert("title".into(), Value::from("ResourceTaskSubmitSpec"));
        object.insert(
            "description".into(),
            Value::from(
                "SubmitSpec with a finite command or container workload and no execution machine.",
            ),
        );
    }
    Ok(schema)
}

fn resource_return_work_schema() -> Result<Value, AppError> {
    let command_spec = resource_task_schema()?;
    Ok(json!({
        "title": "ResourceReturnWorkSpec",
        "description": "Supervisor choice bound to the saved return context. Task identities and recovery references must match the pending action.",
        "oneOf": [
            {
                "title": "same_run_resume",
                "type": "object",
                "additionalProperties": false,
                "required": ["type", "stopped_task", "recovery_ref"],
                "properties": {
                    "type": { "const": "same_run_resume" },
                    "stopped_task": { "type": "string", "format": "uuid" },
                    "recovery_ref": { "type": "string", "minLength": 1 }
                }
            },
            {
                "title": "evaluation_or_next_epoch",
                "type": "object",
                "additionalProperties": false,
                "required": ["type", "completed_task", "spec"],
                "properties": {
                    "type": { "const": "evaluation_or_next_epoch" },
                    "completed_task": { "type": "string", "format": "uuid" },
                    "spec": command_spec
                }
            },
            {
                "title": "new_background_work",
                "type": "object",
                "additionalProperties": false,
                "required": ["type", "spec"],
                "properties": {
                    "type": { "const": "new_background_work" },
                    "spec": command_spec
                }
            },
            {
                "title": "after_ended_run",
                "type": "object",
                "additionalProperties": false,
                "required": ["type", "ended_task", "spec"],
                "properties": {
                    "type": { "const": "after_ended_run" },
                    "ended_task": { "type": "string", "format": "uuid" },
                    "spec": command_spec
                }
            }
        ]
    }))
}

fn resource_trainer_attempt_binding_schema() -> Value {
    let identifier = json!({
        "type": "string",
        "minLength": 1,
        "maxLength": 128,
        "pattern": "^[a-z0-9][a-z0-9._-]{0,127}$"
    });
    json!({
        "title": "ResourceTrainerAttemptBinding",
        "description": "Strict trainer AttemptBinding. Its task_id is the trainer identity, not the Homebased task UUID passed to resource background bind-attempt.",
        "type": "object",
        "additionalProperties": false,
        "required": ["campaign_id", "campaign_revision_id", "task_id", "attempt_id", "attempt_number", "ownership_token"],
        "properties": {
            "campaign_id": identifier,
            "campaign_revision_id": identifier,
            "task_id": {
                "description": "Trainer task identity, not a Homebased TaskId",
                "type": "string",
                "minLength": 1,
                "maxLength": 128,
                "pattern": "^[a-z0-9][a-z0-9._-]{0,127}$"
            },
            "attempt_id": identifier,
            "attempt_number": { "type": "integer", "minimum": 1 },
            "ownership_token": identifier
        }
    })
}

fn operator_gpu_free_attestation_schema() -> Value {
    let uuid = json!({ "type": "string", "format": "uuid" });
    json!({
        "title": "OperatorGpuFreeAttestation",
        "description": "Complete document for resource operator-release. It records a human confirmation after inspecting the authority GPU; it is not automatic proof that GPU work stopped. Keep operation_id and observation unchanged on retry.",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "operation_id",
            "resource_id",
            "authority_machine",
            "task_id",
            "expected_state_revision",
            "state_binding",
            "observation",
            "confirmation"
        ],
        "properties": {
            "operation_id": uuid,
            "resource_id": uuid,
            "authority_machine": uuid,
            "task_id": uuid,
            "expected_state_revision": { "type": "integer", "minimum": 0 },
            "state_binding": {
                "oneOf": [
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["type"],
                        "properties": { "type": { "const": "no_loan" } }
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["type", "loan_id", "action_id"],
                        "properties": {
                            "type": { "const": "awaiting_release" },
                            "loan_id": uuid,
                            "action_id": uuid
                        }
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["type", "request_id"],
                        "description": "No loan; task_id is the launch task of background_launch with status release_unproven",
                        "properties": {
                            "type": { "const": "first_background_launch" },
                            "request_id": uuid
                        }
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["type", "loan_id", "action_id"],
                        "description": "Restoring loan; task_id is its resume_task_id, a direct-segment return task that ended before its confirmed start",
                        "properties": {
                            "type": { "const": "restoring_return" },
                            "loan_id": uuid,
                            "action_id": uuid
                        }
                    },
                    {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["type", "loan_id", "action_id"],
                        "description": "Restoring loan; task_id is its resume_task_id, a native foreground return task that ended or is lost without a confirmed process-group exit",
                        "properties": {
                            "type": { "const": "restoring_foreground_return" },
                            "loan_id": uuid,
                            "action_id": uuid
                        }
                    }
                ]
            },
            "observation": {
                "type": "string",
                "minLength": 1,
                "description": "Human account of what was inspected and why the GPU is free"
            },
            "confirmation": { "const": "operator_confirmed_gpu_free" }
        }
    })
}

async fn operator_release(ctx: &Ctx, path: &str) -> Result<ExitCode, AppError> {
    let attestation: OperatorGpuFreeAttestation = load_json(path)?;
    attestation
        .validate()
        .map_err(|error| invalid_resource_spec("", &error.to_string()))?;
    let resource = attestation.resource_id;
    let operation = attestation.operation_id.as_uuid();
    let retry_identity = operator_release_retry_identity(path, operation);
    let body = OperatorReleaseBody {
        api_version: API_VERSION,
        attestation: attestation.clone(),
    };
    let client = Client::new(ctx.home.sock_path());
    let value = client
        .post(
            &format!("/v1/resources/{}/operator-release", resource.as_uuid()),
            &serde_json::to_value(&body)?,
        )
        .await
        .map_err(|error| {
            operator_release_mutation_error(resource, operation, &retry_identity, error)
        })?;
    check_version(&value).map_err(|error| {
        operator_release_unknown(resource, operation, &retry_identity, error.to_string())
    })?;
    let response: OperatorReleaseResponse = serde_json::from_value(value).map_err(|error| {
        operator_release_unknown(
            resource,
            operation,
            &retry_identity,
            format!("the authority returned an invalid receipt: {error}"),
        )
    })?;
    check_operator_release_response(&response, &attestation, &retry_identity)?;

    let replayed = response.replayed;
    let value = serde_json::to_value(response).map_err(|error| {
        operator_release_unknown(
            resource,
            operation,
            &retry_identity,
            format!("the saved receipt could not be rendered: {error}"),
        )
    })?;
    let human = if replayed {
        format!("saved operator attestation receipt {operation} was replayed")
    } else {
        format!("operator attestation receipt {operation} was saved")
    };
    emit(ctx, value, Some(&operation.to_string()), Some(&human))?;
    Ok(ExitCode::SUCCESS)
}

fn operator_release_retry_identity(path: &str, operation: Uuid) -> String {
    format!(
        "retry with the exact same document, operation_id {operation}, and unchanged observation using `homebased resource operator-release --spec {path}`"
    )
}

fn operator_release_mutation_error(
    resource: ResourceId,
    operation: Uuid,
    retry_identity: &str,
    error: AppError,
) -> AppError {
    match mutation_error(resource, Some(operation), retry_identity, error) {
        AppError::ResourceOutcomeUnknown { message, .. } => AppError::ResourceOutcomeUnknown {
            resource,
            operation: Some(operation),
            message: if message.contains(retry_identity) {
                message
            } else {
                format!("{message}; {retry_identity}")
            },
        },
        error => error,
    }
}

fn operator_release_unknown(
    resource: ResourceId,
    operation: Uuid,
    retry_identity: &str,
    detail: String,
) -> AppError {
    AppError::ResourceOutcomeUnknown {
        resource,
        operation: Some(operation),
        message: format!("{detail}; {retry_identity}"),
    }
}

fn check_operator_release_response(
    response: &OperatorReleaseResponse,
    expected: &OperatorGpuFreeAttestation,
    retry_identity: &str,
) -> Result<(), AppError> {
    if response.api_version == API_VERSION && response.receipt.attestation == *expected {
        return Ok(());
    }

    Err(operator_release_unknown(
        expected.resource_id,
        expected.operation_id.as_uuid(),
        retry_identity,
        "the authority returned a different attestation identity or API version".into(),
    ))
}

fn initial_idle_attestation_schema() -> Value {
    let uuid = json!({ "type": "string", "format": "uuid" });
    json!({
        "title": "InitialIdleAttestation",
        "description": "Complete document for resource initial-idle. Use it once for a registered resource with no registered background task, loan, first background launch, or operator attestation, after inspecting the authority GPU. It records a human confirmation, not automatic proof. Keep operation_id and observation unchanged on retry.",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "operation_id",
            "resource_id",
            "authority_machine",
            "expected_state_revision",
            "observation",
            "confirmation"
        ],
        "properties": {
            "operation_id": uuid,
            "resource_id": uuid,
            "authority_machine": uuid,
            "expected_state_revision": { "type": "integer", "minimum": 0 },
            "observation": {
                "type": "string",
                "minLength": 1,
                "description": "Human account of what was inspected and why no work holds the GPU"
            },
            "confirmation": { "const": "operator_confirmed_gpu_free" }
        }
    })
}

async fn initial_idle(ctx: &Ctx, path: &str) -> Result<ExitCode, AppError> {
    let attestation: InitialIdleAttestation = load_json(path)?;
    attestation
        .validate()
        .map_err(|error| invalid_resource_spec("", &error.to_string()))?;
    let resource = attestation.resource_id;
    let operation = attestation.operation_id.as_uuid();
    let retry_identity = format!(
        "retry with the exact same document and operation_id {operation} using `homebased resource initial-idle --spec {path}`"
    );
    let body = InitialIdleBody {
        api_version: API_VERSION,
        attestation: attestation.clone(),
    };
    let client = Client::new(ctx.home.sock_path());
    let value = client
        .post(
            &format!("/v1/resources/{}/initial-idle", resource.as_uuid()),
            &serde_json::to_value(&body)?,
        )
        .await
        .map_err(|error| {
            operator_release_mutation_error(resource, operation, &retry_identity, error)
        })?;
    check_version(&value).map_err(|error| {
        operator_release_unknown(resource, operation, &retry_identity, error.to_string())
    })?;
    let response: InitialIdleResponse = serde_json::from_value(value).map_err(|error| {
        operator_release_unknown(
            resource,
            operation,
            &retry_identity,
            format!("the authority returned an invalid receipt: {error}"),
        )
    })?;
    if response.api_version != API_VERSION || response.receipt.attestation != attestation {
        return Err(operator_release_unknown(
            resource,
            operation,
            &retry_identity,
            "the authority returned a different attestation identity or API version".into(),
        ));
    }

    let human = if response.replayed {
        format!("saved initial idle receipt {operation} was replayed")
    } else {
        format!("initial idle receipt {operation} was saved")
    };
    let value = serde_json::to_value(response)?;
    emit(ctx, value, Some(&operation.to_string()), Some(&human))?;
    Ok(ExitCode::SUCCESS)
}

async fn register(ctx: &Ctx, path: &str) -> Result<ExitCode, AppError> {
    let spec: ResourceRegistrationSpec = load_json(path)?;
    let resource_id = validate_registration(&spec)?;
    let body = ResourceRegisterBody {
        api_version: API_VERSION,
        spec: ResourceRegistration {
            id: resource_id,
            display_name: spec.display_name,
            supervisor: spec.supervisor,
        },
    };
    let client = Client::new(ctx.home.sock_path());
    let value = post_mutation(
        &client,
        RESOURCE_REGISTER_PATH,
        &body,
        resource_id,
        Some(resource_id.as_uuid()),
        &format!("resource registration id {}", resource_id.as_uuid()),
    )
    .await?;
    check_mutation_resource(
        &value,
        resource_id,
        Some(resource_id.as_uuid()),
        &format!("resource id {}", resource_id.as_uuid()),
    )?;
    emit(ctx, value, Some(&resource_id.as_uuid().to_string()), None)?;
    Ok(ExitCode::SUCCESS)
}

/// Check a registration spec and return its resource identity
fn validate_registration(spec: &ResourceRegistrationSpec) -> Result<ResourceId, AppError> {
    let resource_id = ResourceId::from_uuid(spec.id)
        .map_err(|_| invalid_resource_spec("/id", "resource id must not be nil"))?;
    if spec.display_name.trim().is_empty() {
        return Err(invalid_resource_spec(
            "/display_name",
            "display_name must not be empty",
        ));
    }
    if spec.display_name.chars().count() > 120 || spec.display_name.chars().any(char::is_control) {
        return Err(invalid_resource_spec(
            "/display_name",
            "display_name must be at most 120 characters without control characters",
        ));
    }
    if spec.supervisor.machine.as_uuid().is_nil() || spec.supervisor.thread.0.is_nil() {
        return Err(invalid_resource_spec(
            "/supervisor",
            "supervisor machine and thread ids must not be nil",
        ));
    }
    Ok(resource_id)
}

fn invalid_resource_spec(pointer: &str, message: &str) -> AppError {
    AppError::InvalidSpec {
        pointer: pointer.into(),
        value: Value::Null,
        message: message.into(),
    }
}

/// Wrap the `RESOURCE_ID` argument, refusing the nil UUID
fn resource_id_arg(value: Uuid) -> Result<ResourceId, AppError> {
    ResourceId::from_uuid(value).map_err(|_| AppError::Usage {
        message: "RESOURCE_ID must not be nil".into(),
    })
}

fn validate_uuid(field: &str, value: Uuid) -> Result<(), AppError> {
    if value.is_nil() {
        return Err(AppError::Usage {
            message: format!("{field} must not be nil"),
        });
    }
    Ok(())
}

async fn show(ctx: &Ctx, resource_id: Uuid) -> Result<ExitCode, AppError> {
    let id = resource_id_arg(resource_id)?;
    let client = Client::new(ctx.home.sock_path());
    let value = client
        .get(&format!("/v1/resources/{resource_id}"))
        .await
        .map_err(|error| read_resource_error(id, error))?;
    check_version(&value)?;
    check_read_resource(&value, id)?;
    emit(ctx, value, Some(&resource_id.to_string()), None)?;
    Ok(ExitCode::SUCCESS)
}

async fn supervisor(ctx: &Ctx, command: SupervisorCommand) -> Result<ExitCode, AppError> {
    match command {
        SupervisorCommand::Set {
            resource_id,
            machine,
            thread,
            expected_revision,
        } => {
            let id = resource_id_arg(resource_id)?;
            if machine.as_uuid().is_nil() || thread.0.is_nil() {
                return Err(AppError::Usage {
                    message: "supervisor machine and thread ids must not be nil".into(),
                });
            }
            let body = SupervisorReplacementBody {
                api_version: API_VERSION,
                expected_revision: ResourceRevision::new(expected_revision),
                supervisor: SupervisorAddress { machine, thread },
            };
            let client = Client::new(ctx.home.sock_path());
            let value = post_mutation(
                &client,
                &format!("/v1/resources/{resource_id}/supervisor"),
                &body,
                id,
                None,
                &format!("expected revision {expected_revision} and supervisor {machine}/{thread}"),
            )
            .await?;
            check_mutation_resource(
                &value,
                id,
                None,
                &format!("expected revision {expected_revision} and supervisor {machine}/{thread}"),
            )?;
            emit(
                ctx,
                value,
                Some(&resource_id.to_string()),
                Some(&format!(
                    "supervisor set for resource {resource_id} at expected revision {expected_revision}"
                )),
            )?;
            Ok(ExitCode::SUCCESS)
        }
    }
}

async fn background(ctx: &Ctx, command: BackgroundCommand) -> Result<ExitCode, AppError> {
    match command {
        BackgroundCommand::Submit {
            resource_id,
            request_id,
            spec: path,
        } => submit_command(ctx, resource_id, request_id, &path, true).await,
        BackgroundCommand::BindAttempt {
            resource_id,
            task_id,
            attempt_spec,
        } => bind_trainer_attempt(ctx, resource_id, task_id, &attempt_spec).await,
    }
}

async fn bind_trainer_attempt(
    ctx: &Ctx,
    resource_uuid: Uuid,
    task_id: TaskId,
    path: &str,
) -> Result<ExitCode, AppError> {
    let resource_id = resource_id_arg(resource_uuid)?;
    validate_uuid("--task-id", task_id.0)?;

    let attempt_binding: AttemptBinding = load_json(path)?;
    let retry_identity = trainer_attempt_retry_identity(resource_uuid, task_id, &attempt_binding)?;
    let body = TrainerAttemptBody {
        api_version: API_VERSION,
        attempt_binding: attempt_binding.clone(),
    };
    let endpoint = format!("/v1/resources/{resource_uuid}/background/{task_id}/trainer-attempt");
    let client = Client::new(ctx.home.sock_path());
    let response: TrainerAttemptResponse = client
        .post_json(&endpoint, &body)
        .await
        .map_err(|error| trainer_attempt_error(resource_id, &retry_identity, error))?;
    check_trainer_attempt_response(
        &response,
        resource_id,
        task_id,
        &attempt_binding,
        &retry_identity,
    )?;

    let value = serde_json::to_value(response)?;
    let human = format!(
        "trainer attempt for resource {resource_uuid} was bound to Homebased task {task_id}"
    );
    emit(ctx, value, Some(&task_id.to_string()), Some(&human))?;
    Ok(ExitCode::SUCCESS)
}

fn trainer_attempt_retry_identity(
    resource_uuid: Uuid,
    task_id: TaskId,
    attempt_binding: &AttemptBinding,
) -> Result<String, AppError> {
    let binding_json = serde_json::to_string(attempt_binding)?;
    Ok(format!(
        "retry with resource UUID {resource_uuid}, Homebased task UUID {task_id}, and the identical AttemptBinding JSON: {binding_json}"
    ))
}

fn trainer_attempt_error(resource: ResourceId, retry_identity: &str, error: AppError) -> AppError {
    match error {
        AppError::ResourceOutcomeUnknown { message, .. } => AppError::ResourceOutcomeUnknown {
            resource,
            operation: None,
            message: format!("{message}; {retry_identity}"),
        },
        AppError::Internal { message }
        | AppError::DaemonUnavailable { message }
        | AppError::MachineUnavailable { message, .. }
        | AppError::ResourceAuthorityUnavailable { message, .. }
        | AppError::RemoteSubmissionUnavailable { message } => AppError::ResourceOutcomeUnknown {
            resource,
            operation: None,
            message: format!("{message}; {retry_identity}"),
        },
        error @ AppError::ResourceLookupIncomplete { .. } => AppError::ResourceOutcomeUnknown {
            resource,
            operation: None,
            message: format!("{error}; {retry_identity}"),
        },
        error => error,
    }
}

fn check_trainer_attempt_response(
    response: &TrainerAttemptResponse,
    expected_resource: ResourceId,
    expected_task: TaskId,
    expected_binding: &AttemptBinding,
    retry_identity: &str,
) -> Result<(), AppError> {
    if response.api_version == API_VERSION
        && response.resource.id == expected_resource
        && response.task_id == expected_task
        && response.attempt_binding == *expected_binding
    {
        return Ok(());
    }

    Err(AppError::ResourceOutcomeUnknown {
        resource: expected_resource,
        operation: None,
        message: format!("the trainer-attempt response has a different identity; {retry_identity}"),
    })
}

async fn request(ctx: &Ctx, command: RequestCommand) -> Result<ExitCode, AppError> {
    match command {
        RequestCommand::Submit {
            resource_id,
            request_id,
            spec: path,
        } => submit_command(ctx, resource_id, request_id, &path, false).await,
        RequestCommand::Cancel {
            resource_id,
            request_id,
            expected_revision,
            operation_id,
        } => {
            let id = resource_id_arg(resource_id)?;
            validate_uuid("REQUEST_ID", request_id)?;
            validate_uuid("--operation-id", operation_id)?;
            let body = RequestCancelBody {
                api_version: API_VERSION,
                operation_id,
                expected_revision: ResourceRevision::new(expected_revision),
            };
            let client = Client::new(ctx.home.sock_path());
            let path = format!("/v1/resources/{resource_id}/requests/{request_id}/cancel");
            post_operation(ctx, &client, id, operation_id, &path, &body).await
        }
        RequestCommand::Move {
            resource_id,
            request_id,
            expected_revision,
            operation_id,
            placement,
        } => {
            let id = resource_id_arg(resource_id)?;
            validate_uuid("REQUEST_ID", request_id)?;
            validate_uuid("--operation-id", operation_id)?;
            let body = ResourceActionBody {
                api_version: API_VERSION,
                expected_revision: ResourceRevision::new(expected_revision),
                operation_id,
                action: BrowserResourceAction::MoveQueued {
                    request_id: RequestId(request_id),
                    placement: placement.placement()?,
                },
            };
            let client = Client::new(ctx.home.sock_path());
            let path = format!("/v1/resources/{resource_id}/actions");
            post_operation(ctx, &client, id, operation_id, &path, &body).await
        }
    }
}

async fn submit_command(
    ctx: &Ctx,
    resource_uuid: Uuid,
    request_uuid: Uuid,
    path: &str,
    background: bool,
) -> Result<ExitCode, AppError> {
    let resource_id = resource_id_arg(resource_uuid)?;
    validate_uuid("--request-id", request_uuid)?;
    let spec = load_resource_task_spec(path)?;
    let callback_cwd = std::env::current_dir()?;
    if !callback_cwd.is_absolute() {
        return Err(AppError::Usage {
            message: "the callback working directory must be absolute".into(),
        });
    }
    let request_id = RequestId(request_uuid);
    let body = ResourceSubmitBody {
        api_version: API_VERSION,
        request_id,
        spec,
        env: TaskEnv::capture(),
        callback_cwd,
    };
    let client = Client::new(ctx.home.sock_path());
    if background {
        let path = format!("/v1/resources/{resource_uuid}/background");
        let value = client
            .post(&path, &serde_json::to_value(&body)?)
            .await
            .map_err(|error| {
                mutation_error(
                    resource_id,
                    Some(request_uuid),
                    &format!("request id {request_uuid}"),
                    error,
                )
            })?;
        check_version(&value).map_err(|error| AppError::ResourceOutcomeUnknown {
            resource: resource_id,
            operation: Some(request_uuid),
            message: format!(
                "the background response is invalid: {error}; retry with the same request id {request_uuid}"
            ),
        })?;
        let response: ResourceBackgroundSubmitResponse =
            decode_value(value, "resource background response").map_err(|error| {
                AppError::ResourceOutcomeUnknown {
                    resource: resource_id,
                    operation: Some(request_uuid),
                    message: format!(
                        "the background response is invalid: {error}; retry with the same request id {request_uuid}"
                    ),
                }
            })?;
        check_background_response(&response, resource_id, request_id)?;
        let human = match &response.outcome {
            ResourceBackgroundSubmitOutcome::Inserted => format!(
                "background launch {request_uuid} was inserted as task {}",
                response.task_id
            ),
            ResourceBackgroundSubmitOutcome::Existing { .. } => format!(
                "background launch {request_uuid} reused existing task {}",
                response.task_id
            ),
        };
        emit(
            ctx,
            serde_json::to_value(response)?,
            Some(&request_uuid.to_string()),
            Some(&human),
        )?;
        return Ok(ExitCode::SUCCESS);
    }

    let path = format!("/v1/resources/{resource_uuid}/requests");
    let value = client
        .post(&path, &serde_json::to_value(&body)?)
        .await
        .map_err(|error| {
            mutation_error(
                resource_id,
                Some(request_uuid),
                &format!("request id {request_uuid}"),
                error,
            )
        })?;
    check_version(&value).map_err(|error| AppError::ResourceOutcomeUnknown {
        resource: resource_id,
        operation: Some(request_uuid),
        message: format!(
            "the request response is invalid: {error}; retry with the same request id {request_uuid}"
        ),
    })?;
    let response: ResourceRequestSubmitResponse =
        decode_value(value, "resource request response").map_err(|error| {
            AppError::ResourceOutcomeUnknown {
                resource: resource_id,
                operation: Some(request_uuid),
                message: format!(
                    "the request response is invalid: {error}; retry with the same request id {request_uuid}"
                ),
            }
        })?;
    if response.request_id != request_id
        || response.resource_id != resource_id
        || response.task_id.0.is_nil()
        || response.task_id.0 == request_uuid
        || response.authority_machine.as_uuid().is_nil()
    {
        return Err(AppError::ResourceOutcomeUnknown {
            resource: resource_id,
            operation: Some(request_uuid),
            message: format!(
                "the authority returned another request identity; retry with the same request id {request_uuid}"
            ),
        });
    }
    let human = match &response.outcome {
        ResourceRequestSubmitOutcome::Waiting => {
            format!(
                "resource request {request_uuid} is waiting as task {}",
                response.task_id
            )
        }
        ResourceRequestSubmitOutcome::Activated => {
            format!(
                "resource request {request_uuid} activated task {}",
                response.task_id
            )
        }
        ResourceRequestSubmitOutcome::Rejected { reason } => {
            return Err(AppError::SubmissionRejected {
                request: request_id,
                task: response.task_id,
                reason: reason.clone(),
            });
        }
        ResourceRequestSubmitOutcome::CancelledBeforeLaunch => format!(
            "resource request {request_uuid} was cancelled before task {} launched",
            response.task_id
        ),
    };
    emit(
        ctx,
        serde_json::to_value(response)?,
        Some(&request_uuid.to_string()),
        Some(&human),
    )?;
    Ok(ExitCode::SUCCESS)
}

fn load_resource_task_spec(path: &str) -> Result<NormalizedSpec, AppError> {
    let spec = spec::load_spec(path)?;
    let normalized = spec::normalize(&spec)?;
    CommandSpec::try_from(normalized.clone()).map_err(|error| AppError::InvalidSpec {
        pointer: match error {
            CommandSpecError::ExplicitMachine => "/machine".into(),
            CommandSpecError::AgentWorkload => "/workload/type".into(),
            CommandSpecError::ContainerWithoutGpus => "/workload/gpus".into(),
        },
        value: Value::Null,
        message: error.to_string(),
    })?;
    Ok(normalized)
}

async fn requests(ctx: &Ctx, resource_id: Uuid) -> Result<ExitCode, AppError> {
    let id = resource_id_arg(resource_id)?;
    let client = Client::new(ctx.home.sock_path());
    let detail: ResourceDetail = client
        .get_json(&format!("/v1/resources/{resource_id}"))
        .await
        .map_err(|error| read_resource_error(id, error))?;
    if detail.api_version != API_VERSION {
        return Err(invalid_daemon_response("unexpected resource API version"));
    }
    if detail.resource.id != id {
        return Err(invalid_daemon_response(
            "resource request list returned another resource identity",
        ));
    }
    let value = json!({
        "api_version": detail.api_version,
        "resource_id": detail.resource.id,
        "requests": detail.requests,
    });
    match ctx.output {
        OutputMode::Json => emit(ctx, value, None, None)?,
        OutputMode::Quiet => print_ids(value.get("requests"), "request_id")?,
        OutputMode::Human => emit(ctx, value, None, None)?,
    }
    Ok(ExitCode::SUCCESS)
}

async fn pending(ctx: &Ctx, machine: MachineId, thread: ThreadId) -> Result<ExitCode, AppError> {
    validate_uuid("--machine", machine.as_uuid())?;
    validate_uuid("--thread", thread.0)?;
    let client = Client::new(ctx.home.sock_path());
    let value = pending_value(&client, machine, thread).await?;
    if ctx.output == OutputMode::Quiet {
        let pending: PendingActionList = decode_value(value.clone(), "pending-action list")?;
        if let Some(authority) = pending.unavailable_authorities.first() {
            return Err(unavailable_authority(authority));
        }
    }
    match ctx.output {
        OutputMode::Json => emit(ctx, value, None, None)?,
        OutputMode::Quiet => print_ids(value.get("actions"), "action_id")?,
        OutputMode::Human => emit(ctx, value, None, None)?,
    }
    Ok(ExitCode::SUCCESS)
}

async fn pending_value(
    client: &Client,
    machine: MachineId,
    thread: ThreadId,
) -> Result<Value, AppError> {
    let path = format!("{RESOURCE_PENDING_PATH}?machine={machine}&thread={thread}");
    let pending: PendingActionList = client.get_json(&path).await?;
    if pending.api_version != API_VERSION {
        return Err(invalid_daemon_response(
            "unexpected pending-action API version",
        ));
    }
    let address = SupervisorAddress { machine, thread };
    if pending
        .actions
        .iter()
        .any(|action| action.supervisor != address)
    {
        return Err(invalid_daemon_response(
            "pending-action route returned an action for another supervisor address",
        ));
    }
    serde_json::to_value(pending).map_err(Into::into)
}

async fn release_watch(
    ctx: &Ctx,
    action_uuid: Uuid,
    pending_path: &str,
) -> Result<ExitCode, AppError> {
    validate_uuid("ACTION_ID", action_uuid)?;
    let client = Client::new(ctx.home.sock_path());
    let context = action_context_for_current_supervisor(&client, pending_path, |action| {
        action.action_id.as_uuid() == action_uuid
    })
    .await?;
    let observed_task = match &context.phase {
        PendingActionPhase::ReleaseRequired {
            observed_background_task,
        } => *observed_background_task,
        _ => {
            return Err(AppError::ResourceActionNotAllowed {
                resource: context.resource_id,
                message: "the action is not waiting for a background-task release".into(),
            });
        }
    };
    let request = ResourceActionSubmitRequest {
        api_version: API_VERSION,
        authority: context.authority,
        choice: ResourceActionChoice::ReleaseWatcher {
            observed_background_task: observed_task,
        },
    };
    let response = submit_action(&client, request, context.resource_id, context.action_id).await?;
    emit_action(ctx, response, context.resource_id, context.action_id)
}

/// Parsed `resource return` flags: one decision, or a hold of the decision window
enum ReturnChoice<'a> {
    Decide {
        resume_path: Option<&'a str>,
        no_resume: Option<&'a str>,
        request_uuid: Option<Uuid>,
        task_id: Option<TaskId>,
    },
    Hold(Duration),
}

async fn return_action(
    ctx: &Ctx,
    action_uuid: Uuid,
    pending_path: &str,
    choice: ReturnChoice<'_>,
) -> Result<ExitCode, AppError> {
    validate_uuid("ACTION_ID", action_uuid)?;
    let client = Client::new(ctx.home.sock_path());
    let context = action_context_for_current_supervisor(&client, pending_path, |action| {
        action.action_id.as_uuid() == action_uuid
    })
    .await?;
    if !matches!(&context.phase, PendingActionPhase::ReturnRequired) {
        return Err(AppError::ResourceActionNotAllowed {
            resource: context.resource_id,
            message: "the action is not waiting for a return decision".into(),
        });
    }
    let return_context =
        context
            .return_context
            .as_ref()
            .ok_or_else(|| AppError::ResourceActionNotAllowed {
                resource: context.resource_id,
                message: "the pending return action has no return context".into(),
            })?;
    let choice = match choice {
        ReturnChoice::Hold(hold) => ResourceActionChoice::HoldReturn { hold },
        ReturnChoice::Decide {
            resume_path,
            no_resume,
            request_uuid,
            task_id,
        } => {
            let decision = return_decision(resume_path, no_resume, request_uuid, task_id)?;
            decision
                .validate_for(return_context, context.authority.supervisor.thread)
                .map_err(|error| return_decision_error(context.resource_id, error))?;
            ResourceActionChoice::Return { decision }
        }
    };
    let request = ResourceActionSubmitRequest {
        api_version: API_VERSION,
        authority: context.authority,
        choice,
    };
    let response = submit_action(&client, request, context.resource_id, context.action_id).await?;
    emit_action(ctx, response, context.resource_id, context.action_id)
}

/// Build the one return decision that the `resource return` decision flags name
fn return_decision(
    resume_path: Option<&str>,
    no_resume: Option<&str>,
    request_uuid: Option<Uuid>,
    task_id: Option<TaskId>,
) -> Result<ReturnDecision, AppError> {
    match (resume_path, no_resume, request_uuid, task_id) {
        (Some(path), None, Some(request), Some(task)) => {
            validate_uuid("--request-id", request)?;
            validate_uuid("--task-id", task.0)?;
            let work: ReturnWork = load_json(path)?;
            Ok(ReturnDecision::Launch(Box::new(ReturnLaunch {
                request_id: RequestId(request),
                task_id: task,
                work,
            })))
        }
        (None, Some(reason), None, None) if !reason.trim().is_empty() => {
            Ok(ReturnDecision::NoResume {
                reason: reason.to_string(),
            })
        }
        _ => Err(AppError::Usage {
            message: "choose exactly one of --resume-spec, --no-resume, or --hold; launch needs both --request-id and --task-id".into(),
        }),
    }
}

async fn resolve(
    ctx: &Ctx,
    loan_uuid: Uuid,
    pending_path: &str,
    task_id: TaskId,
    reason: String,
) -> Result<ExitCode, AppError> {
    validate_uuid("LOAN_ID", loan_uuid)?;
    validate_uuid("--task-id", task_id.0)?;
    if reason.trim().is_empty() {
        return Err(AppError::Usage {
            message: "--reason must not be empty".into(),
        });
    }
    let client = Client::new(ctx.home.sock_path());
    let context = action_context_for_current_supervisor(&client, pending_path, |action| {
        action.loan_id.as_uuid() == loan_uuid
    })
    .await?;
    let expected_task = match &context.phase {
        PendingActionPhase::Restoring { resume_task_id } => *resume_task_id,
        _ => {
            return Err(AppError::ResourceActionNotAllowed {
                resource: context.resource_id,
                message: format!("loan {loan_uuid} is not awaiting an ended-restore resolution"),
            });
        }
    };
    if expected_task != task_id {
        return Err(AppError::ResourceActionNotAllowed {
            resource: context.resource_id,
            message: format!(
                "task {task_id} does not match the restoring task {expected_task} for loan {loan_uuid}"
            ),
        });
    }
    let request = ResourceActionSubmitRequest {
        api_version: API_VERSION,
        authority: context.authority,
        choice: ResourceActionChoice::ResolveEndedRestore { task_id, reason },
    };
    let response = submit_action(&client, request, context.resource_id, context.action_id).await?;
    emit_action(ctx, response, context.resource_id, context.action_id)
}

async fn renotify(
    ctx: &Ctx,
    action_uuid: Uuid,
    pending_path: &str,
    operation_id: Uuid,
) -> Result<ExitCode, AppError> {
    validate_uuid("ACTION_ID", action_uuid)?;
    validate_uuid("--operation-id", operation_id)?;
    let client = Client::new(ctx.home.sock_path());
    let context = action_context_for_current_supervisor(&client, pending_path, |action| {
        action.action_id.as_uuid() == action_uuid
    })
    .await?;
    let notice = context
        .notice
        .ok_or_else(|| AppError::ResourceOperationUnavailable {
            resource: context.resource_id,
            message: format!(
                "pending action {} has no retryable supervisor notice",
                context.action_id.as_uuid()
            ),
        })?;
    if !matches!(&notice.delivery, SupervisorNoticeDelivery::Failed { .. }) {
        return Err(AppError::ResourceActionNotAllowed {
            resource: context.resource_id,
            message: "the supervisor notice has no failed delivery to retry".into(),
        });
    }
    let body = ResourceActionBody {
        api_version: API_VERSION,
        expected_revision: context.authority.expected_state_revision,
        operation_id,
        action: BrowserResourceAction::Renotify {
            notice_id: notice.id,
        },
    };
    let path = format!("/v1/resources/{}/actions", context.resource_id.as_uuid());
    post_operation(
        ctx,
        &client,
        context.resource_id,
        operation_id,
        &path,
        &body,
    )
    .await
}

async fn action_context_for_current_supervisor<F>(
    client: &Client,
    pending_path: &str,
    predicate: F,
) -> Result<ActionContext, AppError>
where
    F: Fn(&PendingActionView) -> bool,
{
    let (machine, thread) = current_supervisor_address(client).await?;
    let pending: PendingActionList = load_json(pending_path)?;
    if pending.api_version != API_VERSION {
        return Err(invalid_daemon_response(
            "unexpected pending-action API version",
        ));
    }
    let matches: Vec<_> = pending
        .actions
        .iter()
        .filter(|action| predicate(action))
        .collect();
    if matches.len() != 1 {
        if let Some(authority) = pending.unavailable_authorities.first() {
            return Err(unavailable_authority(authority));
        }
        return Err(AppError::Usage {
            message: if matches.is_empty() {
                "no matching action is present in --pending-spec; save the pending --json output before mutation and reuse it for retries".into()
            } else {
                "more than one pending resource action matched; refresh `resource pending` and use an exact action id".into()
            },
        });
    }
    let action = matches[0];
    if action.supervisor.machine != machine || action.supervisor.thread != thread {
        return Err(AppError::ResourceActionNotAllowed {
            resource: action.resource_id,
            message: "the pending action is not assigned to the current supervisor address".into(),
        });
    }
    action_context(action)
}

/// Bind a pending action to the exact authority identity a decision must present
///
/// Revisions are compare-and-set tokens that the authority checks, and a fresh
/// resource legitimately holds revision 0, so only nil identities are rejected here
fn action_context(action: &PendingActionView) -> Result<ActionContext, AppError> {
    if action.resource_id.as_uuid().is_nil()
        || action.authority_machine.as_uuid().is_nil()
        || action.loan_id.as_uuid().is_nil()
        || action.action_id.as_uuid().is_nil()
        || action.supervisor.machine.as_uuid().is_nil()
        || action.supervisor.thread.0.is_nil()
    {
        return Err(invalid_daemon_response(
            "pending action has an invalid identity",
        ));
    }
    let authority = SupervisorActionAuthority {
        authority_machine: action.authority_machine,
        resource_id: action.resource_id,
        loan_id: action.loan_id,
        action_id: action.action_id,
        expected_state_revision: action.state_revision,
        supervisor: action.supervisor,
        assignment_revision: action.assignment_revision,
    };
    Ok(ActionContext {
        resource_id: action.resource_id,
        action_id: action.action_id,
        authority,
        phase: action.phase.clone(),
        return_context: action.return_context.clone(),
        notice: action.notice.clone(),
    })
}

fn unavailable_authority(authority: &UnavailableAuthority) -> AppError {
    AppError::MachineUnavailable {
        machine: authority.machine,
        message: authority.message.clone(),
    }
}

async fn current_supervisor_address(client: &Client) -> Result<(MachineId, ThreadId), AppError> {
    let thread = THREAD_ENV_VARS
        .into_iter()
        .find_map(|name| std::env::var(name).ok())
        .ok_or_else(|| AppError::Usage {
            message: "set CODEX_THREAD_ID, CODEX_SESSION_ID, or CLAUDE_CODE_SESSION_ID to use a pending resource action"
                .into(),
        })?;
    let thread = ThreadId::from_str(&thread)?;
    let machines: MachinesBody = client.get_json("/v1/fleet/machines").await?;
    if machines.api_version != API_VERSION {
        return Err(invalid_daemon_response("unexpected fleet API version"));
    }
    Ok((machines.local.machine, thread))
}

async fn submit_action(
    client: &Client,
    request: ResourceActionSubmitRequest,
    resource_id: ResourceId,
    action_id: ActionId,
) -> Result<ResourceActionSubmitResponse, AppError> {
    let operation_id = action_id.as_uuid();
    let retry_identity = match &request.choice {
        ResourceActionChoice::ReleaseWatcher { .. } => {
            format!("action id {operation_id}")
        }
        ResourceActionChoice::Return {
            decision: ReturnDecision::Launch(launch),
        } => format!(
            "action id {operation_id}, request id {}, and task id {}",
            launch.request_id.0, launch.task_id
        ),
        ResourceActionChoice::Return {
            decision: ReturnDecision::NoResume { .. },
        }
        | ResourceActionChoice::HoldReturn { .. } => format!("action id {operation_id}"),
        ResourceActionChoice::ResolveEndedRestore { task_id, .. } => {
            format!("action id {operation_id} and task id {task_id}")
        }
    };
    let response = client
        .post_json::<_, ResourceActionSubmitResponse>(RESOURCE_ACTION_SUBMIT_PATH, &request)
        .await
        .map_err(|error| mutation_error(resource_id, Some(operation_id), &retry_identity, error))?;
    if response.api_version != API_VERSION {
        return Err(AppError::ResourceOutcomeUnknown {
            resource: resource_id,
            operation: Some(operation_id),
            message: "the resource action response used an unexpected API version; retry with the same action id".into(),
        });
    }
    check_action_outcome(&request, &response.outcome).map_err(|message| {
        AppError::ResourceOutcomeUnknown {
            resource: resource_id,
            operation: Some(operation_id),
            message: format!("{message}; retry with the same {retry_identity}"),
        }
    })?;
    Ok(response)
}

/// Check that one action outcome names the exact authority, route owner, and task
///
/// A co-located supervisor gets a local receipt and a remote supervisor gets the
/// authority's receipt, so each form is valid only for its own placement
fn check_action_outcome(
    request: &ResourceActionSubmitRequest,
    outcome: &ResourceActionSubmitOutcome,
) -> Result<(), &'static str> {
    let expected = request.authority;
    let co_located = expected.supervisor.machine == expected.authority_machine;
    let (expected_kind, expected_task) = match &request.choice {
        ResourceActionChoice::ReleaseWatcher { .. } => {
            (Some(ResourceActionKind::ReleaseWatcher), None)
        }
        ResourceActionChoice::Return {
            decision: ReturnDecision::Launch(launch),
        } => (
            Some(ResourceActionKind::Return),
            Some((launch.request_id, launch.task_id)),
        ),
        ResourceActionChoice::Return {
            decision: ReturnDecision::NoResume { .. },
        }
        | ResourceActionChoice::ResolveEndedRestore { .. }
        | ResourceActionChoice::HoldReturn { .. } => (None, None),
    };
    let same_loan = |loan: &crate::resource::Loan| {
        loan.id == expected.loan_id && loan.resource_id == expected.resource_id
    };
    match outcome {
        ResourceActionSubmitOutcome::Accepted { receipt, .. } => {
            if co_located
                || receipt.authority != expected
                || expected_kind != Some(receipt.kind)
                || expected_task.is_some_and(|ids| ids != (receipt.request_id, receipt.task_id))
            {
                return Err("the action response has a different authority or task kind");
            }
        }
        ResourceActionSubmitOutcome::LocalReturnAccepted {
            receipt,
            acceptance,
        } => {
            if !co_located
                || receipt.authority != expected
                || expected_task != Some((receipt.request_id, receipt.task_id))
            {
                return Err("the local return response has a different authority or task");
            }
            if let LocalReturnAcceptance::Inserted { loan, .. } = acceptance
                && !same_loan(loan)
            {
                return Err("the local return response has a different loan identity");
            }
        }
        ResourceActionSubmitOutcome::Closed { loan, .. } => {
            if expected_kind.is_some()
                || matches!(request.choice, ResourceActionChoice::HoldReturn { .. })
                || !same_loan(loan)
            {
                return Err("the action response has a different loan identity");
            }
        }
        ResourceActionSubmitOutcome::ReturnHeld { window } => {
            if !matches!(request.choice, ResourceActionChoice::HoldReturn { .. })
                || window.action_id() != expected.action_id
                || window.loan_id() != expected.loan_id
                || window.resource_id() != expected.resource_id
            {
                return Err("the hold response names a different action");
            }
        }
        ResourceActionSubmitOutcome::Rejected { .. } => {}
    }
    Ok(())
}

fn emit_action(
    ctx: &Ctx,
    response: ResourceActionSubmitResponse,
    resource_id: ResourceId,
    action_id: ActionId,
) -> Result<ExitCode, AppError> {
    let human = match &response.outcome {
        ResourceActionSubmitOutcome::Accepted { .. } => format!(
            "resource action {} was accepted by its authority",
            action_id.as_uuid()
        ),
        ResourceActionSubmitOutcome::LocalReturnAccepted {
            receipt,
            acceptance: LocalReturnAcceptance::Inserted { .. },
        } => format!(
            "resource action {} started return task {}",
            action_id.as_uuid(),
            receipt.task_id
        ),
        ResourceActionSubmitOutcome::LocalReturnAccepted {
            receipt,
            acceptance: LocalReturnAcceptance::Existing { state },
        } => format!(
            "resource action {} already bound return task {} ({state:?}); nothing was started again",
            action_id.as_uuid(),
            receipt.task_id
        ),
        ResourceActionSubmitOutcome::Closed { .. } => {
            format!("resource action {} closed its loan", action_id.as_uuid())
        }
        ResourceActionSubmitOutcome::ReturnHeld { window } => format!(
            "resource action {} holds queued requests until {} (limit {})",
            action_id.as_uuid(),
            window
                .deadline_at()
                .to_rfc3339_opts(SecondsFormat::Secs, true),
            window.limit_at().to_rfc3339_opts(SecondsFormat::Secs, true)
        ),
        ResourceActionSubmitOutcome::Rejected { reason } => {
            return Err(action_rejection(resource_id, action_id, reason));
        }
    };
    let value = serde_json::to_value(response)?;
    emit(
        ctx,
        value,
        Some(&action_id.as_uuid().to_string()),
        Some(&human),
    )?;
    Ok(ExitCode::SUCCESS)
}

fn action_rejection(
    resource: ResourceId,
    action_id: ActionId,
    rejection: &ResourceActionRejection,
) -> AppError {
    let operation = Some(action_id.as_uuid());
    let details = format!("{rejection:?}");
    match rejection {
        ResourceActionRejection::NotCurrentSupervisor
        | ResourceActionRejection::ActionNotPending => AppError::ResourceActionNotAllowed {
            resource,
            message: details,
        },
        ResourceActionRejection::StaleRevision { expected, actual } => {
            AppError::ResourceStaleRevision {
                resource,
                expected: expected.get(),
                current: actual.get(),
            }
        }
        ResourceActionRejection::SpecMismatch
        | ResourceActionRejection::IdentityConflict
        | ResourceActionRejection::ConflictingRetry
        | ResourceActionRejection::RouteEvidenceMismatch => AppError::ResourceOperationConflict {
            resource,
            operation,
            message: details,
        },
        ResourceActionRejection::RouteEvidenceMissing
        | ResourceActionRejection::WatcherUnavailable { .. }
        | ResourceActionRejection::DecisionRejected { .. }
        | ResourceActionRejection::RestoreNotResolvable { .. } => {
            AppError::ResourceOperationUnavailable {
                resource,
                message: details,
            }
        }
    }
}

/// Post one resource control keyed by its operation id and print the confirmed detail
async fn post_operation<T: Serialize>(
    ctx: &Ctx,
    client: &Client,
    resource: ResourceId,
    operation_id: Uuid,
    path: &str,
    body: &T,
) -> Result<ExitCode, AppError> {
    let retry_identity = format!("operation id {operation_id}");
    let value = post_mutation(
        client,
        path,
        body,
        resource,
        Some(operation_id),
        &retry_identity,
    )
    .await?;
    check_mutation_resource(&value, resource, Some(operation_id), &retry_identity)?;
    emit(ctx, value, Some(&operation_id.to_string()), None)?;
    Ok(ExitCode::SUCCESS)
}

async fn post_mutation<T: Serialize>(
    client: &Client,
    path: &str,
    body: &T,
    resource: ResourceId,
    operation: Option<Uuid>,
    retry_identity: &str,
) -> Result<Value, AppError> {
    let value = client
        .post(path, &serde_json::to_value(body)?)
        .await
        .map_err(|error| mutation_error(resource, operation, retry_identity, error))?;
    check_version(&value).map_err(|error| AppError::ResourceOutcomeUnknown {
        resource,
        operation,
        message: format!(
            "the mutation response is invalid: {error}; retry with the same {retry_identity}"
        ),
    })?;
    Ok(value)
}

fn return_decision_error(resource: ResourceId, error: ReturnDecisionRejection) -> AppError {
    AppError::ResourceActionNotAllowed {
        resource,
        message: format!("return choice does not match the saved context: {error}"),
    }
}

fn mutation_error(
    resource: ResourceId,
    operation: Option<Uuid>,
    retry_identity: &str,
    error: AppError,
) -> AppError {
    match error {
        error @ AppError::ResourceNotFound { .. }
        | error @ AppError::ResourceLookupIncomplete { .. }
        | error @ AppError::ResourceAuthorityUnavailable { .. }
        | error @ AppError::ResourceOutcomeUnknown { .. }
        | error @ AppError::ResourceStaleRevision { .. }
        | error @ AppError::ResourceOperationConflict { .. }
        | error @ AppError::ResourceActionNotAllowed { .. }
        | error @ AppError::ResourceOperationUnavailable { .. }
        | error @ AppError::Usage { .. }
        | error @ AppError::InvalidSpec { .. }
        | error @ AppError::Permission { .. } => error,
        AppError::Internal { message } => {
            if let Some(error) = map_resource_error(resource, operation, &message) {
                error
            } else if message.starts_with("http 404 ") {
                AppError::ResourceOperationUnavailable {
                    resource,
                    message: format!(
                        "the resource route or resource is unavailable (HTTP 404): {message}"
                    ),
                }
            } else {
                AppError::ResourceOutcomeUnknown {
                    resource,
                    operation,
                    message: format!("{message}; retry with the same {retry_identity}"),
                }
            }
        }
        error @ AppError::DaemonUnavailable { .. }
        | error @ AppError::MachineUnavailable { .. }
        | error @ AppError::RemoteSubmissionUnavailable { .. } => {
            AppError::ResourceOutcomeUnknown {
                resource,
                operation,
                message: format!("{error}; retry with the same {retry_identity}"),
            }
        }
        error => error,
    }
}

fn read_resource_error(resource: ResourceId, error: AppError) -> AppError {
    match error {
        AppError::Internal { message } => {
            if let Some(error) = map_resource_error(resource, None, &message) {
                error
            } else if message.starts_with("http 404 ") {
                AppError::ResourceOperationUnavailable {
                    resource,
                    message: format!("the resource route is unavailable (HTTP 404): {message}"),
                }
            } else {
                AppError::Internal { message }
            }
        }
        error => error,
    }
}

fn map_resource_error(
    resource: ResourceId,
    operation: Option<Uuid>,
    message: &str,
) -> Option<AppError> {
    let message = message
        .strip_prefix("http ")
        .and_then(|response| response.split_once(' '))
        .filter(|(status, _)| status.parse::<u16>().is_ok())
        .map_or(message, |(_, body)| body);
    if message.starts_with("resource not found:") {
        return Some(AppError::ResourceNotFound { resource });
    }
    if let Some(authority) = message.strip_prefix("resource authority ") {
        let (machine, details) = authority.split_once(" unavailable:")?;
        let machine = MachineId::from_str(machine).ok()?;
        return Some(AppError::ResourceAuthorityUnavailable {
            resource,
            machine,
            message: details.trim().to_owned(),
        });
    }
    if message.starts_with("resource lookup incomplete for ") {
        return Some(AppError::ResourceOperationUnavailable {
            resource,
            message: message.into(),
        });
    }
    if message.starts_with("resource operation outcome unknown:") {
        return Some(AppError::ResourceOutcomeUnknown {
            resource,
            operation,
            message: message.into(),
        });
    }
    if let Some(revision) = message.strip_prefix("resource revision is stale: expected ") {
        let (expected, current) = revision.split_once(", current ")?;
        let expected = expected.parse().ok()?;
        let current = current.parse().ok()?;
        return Some(AppError::ResourceStaleRevision {
            resource,
            expected,
            current,
        });
    }
    if message.starts_with("resource operation conflict:") {
        return Some(AppError::ResourceOperationConflict {
            resource,
            operation,
            message: message.into(),
        });
    }
    if message.starts_with("resource action not allowed:") {
        return Some(AppError::ResourceActionNotAllowed {
            resource,
            message: message.into(),
        });
    }
    if message.starts_with("resource operation unavailable:") {
        return Some(AppError::ResourceOperationUnavailable {
            resource,
            message: message.into(),
        });
    }
    None
}

fn load_json<T: DeserializeOwned>(path: &str) -> Result<T, AppError> {
    let bytes = if path == "-" {
        let mut bytes = Vec::new();
        io::stdin()
            .read_to_end(&mut bytes)
            .map_err(|error| AppError::Internal {
                message: format!("failed to read resource spec from stdin: {error}"),
            })?;
        bytes
    } else {
        std::fs::read(path).map_err(|error| AppError::Internal {
            message: format!("failed to read resource spec {path}: {error}"),
        })?
    };
    let value: Value = serde_json::from_slice(&bytes).map_err(|error| AppError::InvalidSpec {
        pointer: String::new(),
        value: Value::Null,
        message: format!("invalid JSON in resource spec: {error}"),
    })?;
    decode_spec_value(&value)
}

fn decode_spec_value<T: DeserializeOwned>(value: &Value) -> Result<T, AppError> {
    serde_path_to_error::deserialize(value).map_err(|error| AppError::InvalidSpec {
        pointer: error.path().to_string(),
        value: value.clone(),
        message: error.inner().to_string(),
    })
}

fn check_version(value: &Value) -> Result<(), AppError> {
    if value.get("api_version").and_then(Value::as_u64) == Some(u64::from(API_VERSION)) {
        return Ok(());
    }
    Err(invalid_daemon_response("unexpected resource API version"))
}

fn check_read_resource(value: &Value, expected: ResourceId) -> Result<(), AppError> {
    let found = value
        .pointer("/resource/id")
        .and_then(Value::as_str)
        .and_then(|id| Uuid::parse_str(id).ok())
        .and_then(|id| ResourceId::from_uuid(id).ok());
    if found == Some(expected) {
        return Ok(());
    }
    Err(invalid_daemon_response(
        "resource detail returned a missing or different resource identity",
    ))
}

fn check_mutation_resource(
    value: &Value,
    expected: ResourceId,
    operation: Option<Uuid>,
    retry_identity: &str,
) -> Result<(), AppError> {
    let found = value
        .pointer("/resource/id")
        .and_then(Value::as_str)
        .and_then(|id| Uuid::parse_str(id).ok())
        .and_then(|id| ResourceId::from_uuid(id).ok());
    if found == Some(expected) {
        return Ok(());
    }
    Err(AppError::ResourceOutcomeUnknown {
        resource: expected,
        operation,
        message: format!(
            "the mutation response has a missing or different resource identity; retry with the same {retry_identity}"
        ),
    })
}

fn check_background_response(
    response: &ResourceBackgroundSubmitResponse,
    expected_resource: ResourceId,
    expected_request: RequestId,
) -> Result<(), AppError> {
    if response.api_version == API_VERSION
        && response.request_id == expected_request
        && response.resource.id == expected_resource
        && !response.task_id.0.is_nil()
        && response.task_id.0 != expected_request.0
    {
        return Ok(());
    }
    Err(AppError::ResourceOutcomeUnknown {
        resource: expected_resource,
        operation: Some(expected_request.0),
        message: format!(
            "the authority returned another background request identity; retry with the same request id {}",
            expected_request.0
        ),
    })
}

fn decode_value<T: DeserializeOwned>(value: Value, description: &str) -> Result<T, AppError> {
    serde_json::from_value(value)
        .map_err(|error| invalid_daemon_response(&format!("invalid {description}: {error}")))
}

fn invalid_daemon_response(message: &str) -> AppError {
    AppError::Internal {
        message: message.into(),
    }
}

fn print_ids(values: Option<&Value>, field: &str) -> Result<(), AppError> {
    let rows = values.and_then(Value::as_array).ok_or_else(|| {
        invalid_daemon_response(format!("resource response has no {field} list").as_str())
    })?;
    for row in rows {
        let id = row
            .get(field)
            .and_then(Value::as_str)
            .ok_or_else(|| invalid_daemon_response(&format!("resource row has no {field}")))?;
        println!("{id}");
    }
    Ok(())
}

fn emit(
    ctx: &Ctx,
    value: Value,
    quiet_id: Option<&str>,
    human: Option<&str>,
) -> Result<(), AppError> {
    if let Some(output) = render_output(ctx.output, value, quiet_id, human)? {
        println!("{output}");
    }
    Ok(())
}

fn render_output(
    mode: OutputMode,
    value: Value,
    quiet_id: Option<&str>,
    human: Option<&str>,
) -> Result<Option<String>, AppError> {
    match mode {
        OutputMode::Json => {
            let mut value = value;
            if let Some(object) = value.as_object_mut() {
                object
                    .entry("api_version")
                    .or_insert(Value::from(API_VERSION));
            }
            Ok(Some(serde_json::to_string_pretty(&value)?))
        }
        OutputMode::Quiet => Ok(quiet_id.map(str::to_string)),
        OutputMode::Human => match human {
            Some(human) => Ok(Some(human.to_string())),
            None => Ok(Some(serde_json::to_string_pretty(&value)?)),
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::resource::operator_release::OperatorObservation;
    use crate::resource::trainer_publication::AttemptBinding;
    use crate::resource::{AssignmentRevision, Resource};
    use clap::Parser;
    use serde_json::json;

    use super::{
        BackgroundCommand, RequestCommand, ResourceCommand, ResourceSubmitBody, SupervisorCommand,
        action_context, check_action_outcome, check_background_response,
        check_operator_release_response, check_trainer_attempt_response, decode_spec_value,
        load_json, mutation_error, operator_gpu_free_attestation_schema, render_output,
        resource_registration_schema, resource_return_work_schema, resource_task_schema,
        resource_trainer_attempt_binding_schema, trainer_attempt_error,
        trainer_attempt_retry_identity,
    };
    use crate::cli::{Cli, Command, OutputMode};
    use crate::domain::{API_VERSION, TaskEnv, TaskId, ThreadId};
    use crate::error::AppError;
    use crate::machine::MachineId;
    use crate::resource::api::{
        BrowserResourceAction, OperatorReleaseResponse, PendingActionPhase, PendingActionView,
        QueuePlacement, RequestCancelBody, ResourceActionBody, ResourceBackgroundSubmitOutcome,
        ResourceBackgroundSubmitResponse, ResourceRegisterBody, ResourceRegistration,
        SupervisorReplacementBody, TrainerAttemptResponse,
    };
    use crate::resource::bound_action::{
        LocalReturnAcceptance, ResourceActionChoice, ResourceActionKind,
        ResourceActionSubmitOutcome, ResourceActionSubmitRequest,
    };
    use crate::resource::operator_release::OperatorGpuFreeAttestation;
    use crate::resource::{
        ActionId, CommandSpec, ResourceId, ResourceRevision, ReturnDecision, ReturnLaunch,
        ReturnWork, SupervisorActionAuthority, SupervisorAddress,
    };
    use crate::spec::{self, NormalizedSpec};
    use crate::submission::RequestId;
    use serde_json::Value;
    use uuid::Uuid;

    fn uuid(value: &str) -> Uuid {
        Uuid::parse_str(value).unwrap()
    }

    fn operator_attestation() -> OperatorGpuFreeAttestation {
        OperatorGpuFreeAttestation {
            operation_id: crate::resource::operator_release::OperatorAttestationId::new(),
            resource_id: ResourceId::new(),
            authority_machine: MachineId::new(),
            task_id: TaskId::new(),
            expected_state_revision: ResourceRevision::new(5),
            state_binding: crate::resource::operator_release::OperatorStateBinding::NoLoan,
            observation: OperatorObservation::try_from(
                "checked the authority GPU and found no trainer process".to_owned(),
            )
            .unwrap(),
            confirmation:
                crate::resource::operator_release::OperatorGpuFreeConfirmation::OperatorConfirmedGpuFree,
        }
    }

    fn operator_release_response(
        attestation: OperatorGpuFreeAttestation,
        api_version: u32,
        replayed: bool,
    ) -> OperatorReleaseResponse {
        OperatorReleaseResponse {
            api_version,
            receipt: crate::resource::operator_release::OperatorGpuFreeReceipt {
                attestation,
                evidence: crate::resource::operator_release::OperatorGpuFreeEvidence {
                    trainer_end: crate::resource::operator_release::AttestedTrainerEnd::Lost,
                    trainer_launch:
                        crate::resource::operator_release::AttestedTrainerLaunch::FirstBackgroundLaunch {
                            request_id: RequestId::new(),
                        },
                    normalized_spec_sha256: serde_json::from_value(json!("42".repeat(32)))
                        .unwrap(),
                    trainer_association:
                        crate::resource::operator_release::AttestedTrainerAssociation::Missing,
                },
                state_revision: ResourceRevision::new(6),
                outcome: crate::resource::operator_release::OperatorGpuFreeOutcome::IdleBoundary,
            },
            replayed,
        }
    }

    #[test]
    fn operator_release_cli_parses_a_complete_spec_path() {
        let cli = Cli::try_parse_from([
            "homebased",
            "resource",
            "operator-release",
            "--spec",
            "attestation.json",
        ])
        .unwrap();
        let Command::Resource {
            command: ResourceCommand::OperatorRelease { spec },
        } = cli.command
        else {
            panic!("operator-release must keep its document path");
        };
        assert_eq!(spec, "attestation.json");
    }

    #[test]
    fn operator_release_document_is_strict_and_keeps_retry_identity_and_observation() {
        let attestation = operator_attestation();
        let value = serde_json::to_value(&attestation).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("attestation.json");
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        let decoded: OperatorGpuFreeAttestation = load_json(path.to_str().unwrap()).unwrap();
        assert_eq!(decoded.operation_id, attestation.operation_id);
        assert_eq!(decoded.observation, attestation.observation);
        assert!(decoded.validate().is_ok());

        let mut unknown_field = value.clone();
        unknown_field["unexpected"] = json!(true);
        assert!(decode_spec_value::<OperatorGpuFreeAttestation>(&unknown_field).is_err());

        let mut missing_confirmation = value.clone();
        missing_confirmation
            .as_object_mut()
            .unwrap()
            .remove("confirmation");
        assert!(decode_spec_value::<OperatorGpuFreeAttestation>(&missing_confirmation).is_err());

        let mut invalid_confirmation = value;
        invalid_confirmation["confirmation"] = json!("confirmed");
        assert!(decode_spec_value::<OperatorGpuFreeAttestation>(&invalid_confirmation).is_err());
    }

    #[test]
    fn operator_release_schema_lists_exactly_the_bindings_the_authority_decodes() {
        use crate::resource::operator_release::OperatorStateBinding;

        let schema = serde_json::to_value(operator_gpu_free_attestation_schema()).unwrap();
        let variants = schema
            .pointer("/properties/state_binding/oneOf")
            .and_then(Value::as_array)
            .unwrap();
        let (loan, action, request) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let mut documented = Vec::new();
        for variant in variants {
            let tag = variant
                .pointer("/properties/type/const")
                .and_then(Value::as_str)
                .unwrap();
            documented.push(tag.to_owned());
            // build the document from the schema's own required fields
            let mut binding = json!({ "type": tag });
            for field in variant["required"].as_array().unwrap() {
                let field = field.as_str().unwrap();
                let value = match field {
                    "type" => continue,
                    "loan_id" => loan,
                    "action_id" => action,
                    "request_id" => request,
                    other => panic!("unexpected binding field {other}"),
                };
                binding[field] = json!(value);
            }
            let mut document = serde_json::to_value(operator_attestation()).unwrap();
            document["state_binding"] = binding.clone();
            let decoded = decode_spec_value::<OperatorGpuFreeAttestation>(&document).unwrap();
            assert!(decoded.validate().is_ok(), "{tag}");
            assert_eq!(
                serde_json::to_value(decoded.state_binding).unwrap(),
                binding
            );

            // every binding refuses extra fields, so a typo cannot drop an identity
            binding["unexpected"] = json!(true);
            document["state_binding"] = binding;
            assert!(
                decode_spec_value::<OperatorGpuFreeAttestation>(&document).is_err(),
                "{tag}"
            );
        }
        assert_eq!(
            documented,
            [
                "no_loan",
                "awaiting_release",
                "first_background_launch",
                "restoring_return",
                "restoring_foreground_return"
            ]
        );

        // a nil launch or restore identity is refused before any request is sent
        let mut attestation = operator_attestation();
        attestation.state_binding = OperatorStateBinding::FirstBackgroundLaunch {
            request_id: RequestId(Uuid::nil()),
        };
        assert!(attestation.validate().is_err());
        for binding in [
            json!({"type": "restoring_return", "loan_id": Uuid::nil(), "action_id": action}),
            json!({"type": "restoring_foreground_return", "loan_id": loan, "action_id": Uuid::nil()}),
        ] {
            let mut document = serde_json::to_value(operator_attestation()).unwrap();
            document["state_binding"] = binding;
            assert!(decode_spec_value::<OperatorGpuFreeAttestation>(&document).is_err());
        }
    }

    #[test]
    fn operator_release_response_requires_the_saved_document_and_version() {
        let attestation = operator_attestation();
        let response = operator_release_response(attestation.clone(), API_VERSION, true);
        assert!(check_operator_release_response(&response, &attestation, "retry command").is_ok());
        let response_value = serde_json::to_value(&response).unwrap();
        assert_eq!(response_value["api_version"], API_VERSION);
        assert_eq!(response_value["replayed"], true);

        let mismatched_document = OperatorGpuFreeAttestation {
            observation: OperatorObservation::try_from("changed observation".to_owned()).unwrap(),
            ..attestation.clone()
        };
        let mismatch =
            check_operator_release_response(&response, &mismatched_document, "retry command")
                .unwrap_err();
        assert_eq!(mismatch.code(), "resource_outcome_unknown");
        assert!(mismatch.to_string().contains("retry command"));
        assert!(matches!(
            mismatch,
            AppError::ResourceOutcomeUnknown {
                operation: Some(operation),
                ..
            } if operation == mismatched_document.operation_id.as_uuid()
        ));

        let wrong_version = operator_release_response(attestation.clone(), API_VERSION + 1, false);
        assert_eq!(
            check_operator_release_response(&wrong_version, &attestation, "retry command")
                .unwrap_err()
                .code(),
            "resource_outcome_unknown"
        );

        let mut unexpected = response_value;
        unexpected["unexpected"] = json!(true);
        assert!(serde_json::from_value::<OperatorReleaseResponse>(unexpected).is_err());
    }

    #[test]
    fn resource_commands_parse_with_distinct_identity_arguments() {
        let resource_id = uuid("019b4f42-0000-7000-8000-000000000001");
        let request_id = uuid("019b4f42-0000-7000-8000-000000000002");
        let operation_id = uuid("019b4f42-0000-7000-8000-000000000003");
        let cli = Cli::try_parse_from([
            "homebased",
            "resource",
            "request",
            "cancel",
            &resource_id.to_string(),
            &request_id.to_string(),
            "--expected-revision",
            "4",
            "--operation-id",
            &operation_id.to_string(),
        ])
        .unwrap();
        let Command::Resource {
            command:
                ResourceCommand::Request {
                    command:
                        RequestCommand::Cancel {
                            resource_id: parsed_resource,
                            request_id: parsed_request,
                            operation_id: parsed_operation,
                            ..
                        },
                },
        } = cli.command
        else {
            panic!("resource request cancel must parse to its typed command");
        };
        assert_eq!(parsed_resource, resource_id);
        assert_eq!(parsed_request, request_id);
        assert_eq!(parsed_operation, operation_id);
    }

    #[test]
    fn resource_request_move_requires_one_placement() {
        let resource_id = "019b4f42-0000-7000-8000-000000000011";
        let request_id = "019b4f42-0000-7000-8000-000000000012";
        let anchor_id = "019b4f42-0000-7000-8000-000000000013";
        let operation_id = "019b4f42-0000-7000-8000-000000000014";
        let parse = |args: &[&str]| {
            Cli::try_parse_from(
                [
                    "homebased",
                    "resource",
                    "request",
                    "move",
                    resource_id,
                    request_id,
                    "--operation-id",
                    operation_id,
                ]
                .into_iter()
                .chain(args.iter().copied()),
            )
        };
        let placement = |args: &[&str]| {
            let Command::Resource {
                command:
                    ResourceCommand::Request {
                        command: RequestCommand::Move { placement, .. },
                    },
            } = parse(args).unwrap().command
            else {
                panic!("resource request move must parse to its typed command");
            };
            placement.placement().unwrap()
        };
        assert_eq!(
            placement(&["--expected-revision", "0", "--before", anchor_id]),
            QueuePlacement::Before {
                request_id: RequestId(uuid(anchor_id)),
            }
        );
        assert_eq!(
            placement(&["--expected-revision", "4", "--after", anchor_id]),
            QueuePlacement::After {
                request_id: RequestId(uuid(anchor_id)),
            }
        );
        assert_eq!(
            placement(&["--expected-revision", "4", "--front"]),
            QueuePlacement::Front
        );
        assert_eq!(
            placement(&["--expected-revision", "4", "--back"]),
            QueuePlacement::Back
        );

        assert!(
            parse(&["--expected-revision", "4"]).is_err(),
            "a placement is required"
        );
        assert!(
            parse(&["--expected-revision", "4", "--front", "--back"]).is_err(),
            "two placements must be refused"
        );
    }

    #[test]
    fn resource_supervisor_set_accepts_zero_expected_revision() {
        let cli = Cli::try_parse_from([
            "homebased",
            "resource",
            "supervisor",
            "set",
            "019b4f42-0000-7000-8000-000000000021",
            "--machine",
            "019b4f42-0000-7000-8000-000000000022",
            "--thread",
            "019b4f42-0000-7000-8000-000000000023",
            "--expected-revision",
            "0",
        ])
        .unwrap();

        let Command::Resource {
            command:
                ResourceCommand::Supervisor {
                    command:
                        SupervisorCommand::Set {
                            expected_revision, ..
                        },
                },
        } = cli.command
        else {
            panic!("resource supervisor set must parse to its typed command");
        };

        assert_eq!(expected_revision, 0);
    }

    #[test]
    fn resource_output_flags_parse_and_conflict() {
        let resource = "019b4f42-0000-7000-8000-000000000021";
        let json =
            Cli::try_parse_from(["homebased", "--json", "resource", "show", resource]).unwrap();
        assert!(json.json);
        assert!(!json.quiet);

        assert!(
            Cli::try_parse_from([
                "homebased",
                "--json",
                "--quiet",
                "resource",
                "show",
                resource,
            ])
            .is_err()
        );
    }

    #[test]
    fn resource_request_submit_requires_and_preserves_the_request_id() {
        let resource = "019b4f42-0000-7000-8000-000000000011";
        let request = "019b4f42-0000-7000-8000-000000000012";
        let cli = Cli::try_parse_from([
            "homebased",
            "resource",
            "request",
            "submit",
            resource,
            "--request-id",
            request,
            "--spec",
            "task.json",
        ])
        .unwrap();
        let Command::Resource {
            command:
                ResourceCommand::Request {
                    command:
                        RequestCommand::Submit {
                            resource_id: parsed_resource,
                            request_id: parsed_request,
                            ..
                        },
                },
        } = cli.command
        else {
            panic!("resource request submit must parse to its typed command");
        };
        assert_eq!(parsed_resource, uuid(resource));
        assert_eq!(parsed_request, uuid(request));
        assert!(
            Cli::try_parse_from([
                "homebased",
                "resource",
                "request",
                "submit",
                resource,
                "--spec",
                "task.json",
            ])
            .is_err()
        );
    }

    #[test]
    fn background_submit_requires_and_preserves_its_request_id() {
        let resource = "019b4f42-0000-7000-8000-000000000041";
        let request = "019b4f42-0000-7000-8000-000000000042";
        let cli = Cli::try_parse_from([
            "homebased",
            "resource",
            "background",
            "submit",
            resource,
            "--request-id",
            request,
            "--spec",
            "trainer.json",
        ])
        .unwrap();
        let Command::Resource {
            command:
                ResourceCommand::Background {
                    command:
                        BackgroundCommand::Submit {
                            resource_id,
                            request_id,
                            ..
                        },
                },
        } = cli.command
        else {
            panic!("resource background submit must preserve its request identity");
        };
        assert_eq!(resource_id, uuid(resource));
        assert_eq!(request_id, uuid(request));
        assert!(
            Cli::try_parse_from([
                "homebased",
                "resource",
                "background",
                "submit",
                resource,
                "--spec",
                "trainer.json",
            ])
            .is_err()
        );
    }

    #[test]
    fn bind_attempt_keeps_homebased_and_trainer_task_ids_separate() {
        let resource = "019b4f42-0000-7000-8000-000000000051";
        let homebased_task = "019b4f42-0000-7000-8000-000000000052";
        let binding_value = json!({
            "campaign_id": "campaign-1",
            "campaign_revision_id": "revision-1",
            "task_id": "trainer-task-1",
            "attempt_id": "attempt-1",
            "attempt_number": 1,
            "ownership_token": "ownership-1"
        });
        let cli = Cli::try_parse_from([
            "homebased",
            "resource",
            "background",
            "bind-attempt",
            resource,
            "--task-id",
            homebased_task,
            "--attempt-spec",
            "attempt.json",
        ])
        .unwrap();
        let Command::Resource {
            command:
                ResourceCommand::Background {
                    command:
                        BackgroundCommand::BindAttempt {
                            resource_id,
                            task_id,
                            attempt_spec,
                        },
                },
        } = cli.command
        else {
            panic!("bind-attempt must preserve its distinct resource and task identities");
        };
        let binding: AttemptBinding = decode_spec_value(&binding_value).unwrap();

        assert_eq!(resource_id, uuid(resource));
        assert_eq!(task_id, TaskId(uuid(homebased_task)));
        assert_eq!(attempt_spec, "attempt.json");
        assert_eq!(binding.task_id, "trainer-task-1");
        assert_ne!(binding.task_id, task_id.to_string());
    }

    #[test]
    fn trainer_attempt_response_requires_exact_version_and_identities() {
        let resource_id =
            ResourceId::from_uuid(uuid("019b4f42-0000-7000-8000-000000000061")).unwrap();
        let other_resource =
            ResourceId::from_uuid(uuid("019b4f42-0000-7000-8000-000000000062")).unwrap();
        let task_id = TaskId(uuid("019b4f42-0000-7000-8000-000000000063"));
        let other_task = TaskId(uuid("019b4f42-0000-7000-8000-000000000064"));
        let binding = test_attempt_binding();
        let retry_identity =
            trainer_attempt_retry_identity(resource_id.as_uuid(), task_id, &binding).unwrap();

        let accepted = trainer_attempt_response(API_VERSION, resource_id, task_id, binding.clone());
        assert!(
            check_trainer_attempt_response(
                &accepted,
                resource_id,
                task_id,
                &binding,
                &retry_identity,
            )
            .is_ok()
        );

        let different_binding = AttemptBinding {
            attempt_id: "attempt-2".into(),
            ..binding.clone()
        };
        let mismatches = [
            trainer_attempt_response(API_VERSION, other_resource, task_id, binding.clone()),
            trainer_attempt_response(API_VERSION, resource_id, other_task, binding.clone()),
            trainer_attempt_response(API_VERSION, resource_id, task_id, different_binding),
            trainer_attempt_response(API_VERSION + 1, resource_id, task_id, binding.clone()),
        ];
        for response in mismatches {
            let error = check_trainer_attempt_response(
                &response,
                resource_id,
                task_id,
                &binding,
                &retry_identity,
            )
            .unwrap_err();
            let AppError::ResourceOutcomeUnknown {
                resource,
                operation,
                message,
            } = error
            else {
                panic!("a mismatched trainer-attempt response must be unknown");
            };
            assert_eq!(resource, resource_id);
            assert_eq!(operation, None);
            assert!(message.contains(&resource_id.as_uuid().to_string()));
            assert!(message.contains(&task_id.to_string()));
            assert!(message.contains(&serde_json::to_string(&binding).unwrap()));
        }
    }

    #[test]
    fn trainer_attempt_binding_json_rejects_unknown_fields_and_invalid_identifiers() {
        let binding = json!({
            "campaign_id": "campaign-1",
            "campaign_revision_id": "revision-1",
            "task_id": "trainer-task-1",
            "attempt_id": "attempt-1",
            "attempt_number": 1,
            "ownership_token": "ownership-1"
        });
        assert!(decode_spec_value::<AttemptBinding>(&binding).is_ok());

        let mut unknown_field = binding.clone();
        unknown_field["unexpected"] = json!(true);
        assert!(decode_spec_value::<AttemptBinding>(&unknown_field).is_err());

        let mut invalid_identifier = binding;
        invalid_identifier["task_id"] = json!("Trainer-Task-1");
        assert!(decode_spec_value::<AttemptBinding>(&invalid_identifier).is_err());
    }

    #[test]
    fn trainer_attempt_transport_error_recommends_the_exact_retry_identity() {
        let resource_uuid = uuid("019b4f42-0000-7000-8000-000000000071");
        let resource_id = ResourceId::from_uuid(resource_uuid).unwrap();
        let task_id = TaskId(uuid("019b4f42-0000-7000-8000-000000000072"));
        let binding = test_attempt_binding();
        let retry_identity =
            trainer_attempt_retry_identity(resource_uuid, task_id, &binding).unwrap();
        let error = trainer_attempt_error(
            resource_id,
            &retry_identity,
            AppError::DaemonUnavailable {
                message: "socket closed".into(),
            },
        );
        let AppError::ResourceOutcomeUnknown {
            resource,
            operation,
            message,
        } = error
        else {
            panic!("a transport error must be an unknown outcome");
        };
        assert_eq!(resource, resource_id);
        assert_eq!(operation, None);
        assert!(message.contains(&resource_uuid.to_string()));
        assert!(message.contains(&task_id.to_string()));
        assert!(message.contains(&serde_json::to_string(&binding).unwrap()));

        let rejection = AppError::ResourceActionNotAllowed {
            resource: resource_id,
            message: "task is not registered".into(),
        };
        assert!(matches!(
            trainer_attempt_error(resource_id, &retry_identity, rejection),
            AppError::ResourceActionNotAllowed { .. }
        ));
    }

    #[test]
    fn return_launch_requires_a_single_choice_and_both_stable_ids() {
        let action = "019b4f42-0000-7000-8000-000000000004";
        assert!(Cli::try_parse_from(["homebased", "resource", "release-watch", action,]).is_err());

        let no_choice = Cli::try_parse_from(["homebased", "resource", "return", action]);
        assert!(no_choice.is_err());

        let both_choices = Cli::try_parse_from([
            "homebased",
            "resource",
            "return",
            action,
            "--pending-spec",
            "pending.json",
            "--resume-spec",
            "work.json",
            "--no-resume",
            "not now",
            "--request-id",
            "019b4f42-0000-7000-8000-000000000005",
            "--task-id",
            "019b4f42-0000-7000-8000-000000000006",
        ]);
        assert!(both_choices.is_err());

        let missing_ids = Cli::try_parse_from([
            "homebased",
            "resource",
            "return",
            action,
            "--pending-spec",
            "pending.json",
            "--resume-spec",
            "work.json",
        ]);
        assert!(missing_ids.is_err());

        let request_id = "019b4f42-0000-7000-8000-000000000025";
        let task_id = "019b4f42-0000-7000-8000-000000000026";
        let launch = Cli::try_parse_from([
            "homebased",
            "resource",
            "return",
            action,
            "--pending-spec",
            "pending.json",
            "--resume-spec",
            "work.json",
            "--request-id",
            request_id,
            "--task-id",
            task_id,
        ])
        .unwrap();
        let Command::Resource {
            command:
                ResourceCommand::Return {
                    action_id,
                    request_id: Some(parsed_request),
                    task_id: Some(parsed_task),
                    ..
                },
        } = launch.command
        else {
            panic!("resource return must preserve all launch identities");
        };
        assert_eq!(action_id, uuid(action));
        assert_eq!(parsed_request, uuid(request_id));
        assert_eq!(parsed_task, TaskId(uuid(task_id)));

        let hold = Cli::try_parse_from([
            "homebased",
            "resource",
            "return",
            action,
            "--pending-spec",
            "pending.json",
            "--hold",
            "5m",
        ])
        .unwrap();
        let Command::Resource {
            command:
                ResourceCommand::Return {
                    hold: Some(parsed_hold),
                    no_resume: None,
                    resume_spec: None,
                    ..
                },
        } = hold.command
        else {
            panic!("resource return --hold must parse as a hold without a decision");
        };
        assert_eq!(parsed_hold, std::time::Duration::from_secs(5 * 60));
        let hold_and_decision = Cli::try_parse_from([
            "homebased",
            "resource",
            "return",
            action,
            "--pending-spec",
            "pending.json",
            "--hold",
            "5m",
            "--no-resume",
            "not now",
        ]);
        assert!(hold_and_decision.is_err());

        let loan_id = "019b4f42-0000-7000-8000-000000000027";
        let task_id = "019b4f42-0000-7000-8000-000000000028";
        let resolve = Cli::try_parse_from([
            "homebased",
            "resource",
            "resolve",
            loan_id,
            "--pending-spec",
            "pending.json",
            "--task-id",
            task_id,
            "--reason",
            "confirmed ended",
        ])
        .unwrap();
        let Command::Resource {
            command:
                ResourceCommand::Resolve {
                    loan_id: parsed_loan,
                    task_id: parsed_task,
                    ..
                },
        } = resolve.command
        else {
            panic!("resource resolve must keep loan and task identities separate");
        };
        assert_eq!(parsed_loan, uuid(loan_id));
        assert_eq!(parsed_task, TaskId(uuid(task_id)));
    }

    #[test]
    fn resource_schema_separates_registration_and_command_specs() {
        let schema = json!({
            "$defs": {
                "ResourceRegistrationSpec": resource_registration_schema(),
                "ResourceTaskSubmitSpec": resource_task_schema().unwrap(),
                "ResourceReturnWorkSpec": resource_return_work_schema().unwrap(),
                "ResourceTrainerAttemptBinding": resource_trainer_attempt_binding_schema(),
                "OperatorGpuFreeAttestation": operator_gpu_free_attestation_schema(),
            }
        });
        assert_eq!(
            schema
                .pointer("/$defs/ResourceRegistrationSpec/required")
                .unwrap(),
            &json!(["id", "display_name", "supervisor"])
        );
        assert_eq!(
            schema
                .pointer(
                    "/$defs/ResourceTaskSubmitSpec/properties/workload/oneOf/0/properties/type/const"
                )
                .unwrap(),
            "task"
        );
        assert_eq!(
            schema
                .pointer(
                    "/$defs/ResourceTaskSubmitSpec/properties/workload/oneOf/1/properties/type/const"
                )
                .unwrap(),
            "container"
        );
        assert!(
            schema
                .pointer("/$defs/ResourceTaskSubmitSpec/properties/workload/oneOf/1/required")
                .and_then(Value::as_array)
                .unwrap()
                .contains(&json!("gpus")),
            "resource containers must name their gpus"
        );
        assert!(
            schema
                .pointer("/$defs/ResourceTaskSubmitSpec/properties/machine")
                .is_none()
        );
        assert_eq!(
            schema
                .pointer("/$defs/ResourceReturnWorkSpec/oneOf/0/properties/type/const")
                .unwrap(),
            "same_run_resume"
        );
        assert_eq!(
            schema
                .pointer("/$defs/ResourceTrainerAttemptBinding/properties/task_id/description")
                .unwrap(),
            "Trainer task identity, not a Homebased TaskId"
        );
        assert_eq!(
            schema
                .pointer("/$defs/ResourceTrainerAttemptBinding/additionalProperties")
                .unwrap(),
            &json!(false)
        );
        assert_eq!(
            schema
                .pointer("/$defs/OperatorGpuFreeAttestation/required")
                .unwrap(),
            &json!([
                "operation_id",
                "resource_id",
                "authority_machine",
                "task_id",
                "expected_state_revision",
                "state_binding",
                "observation",
                "confirmation"
            ])
        );
        assert_eq!(
            schema
                .pointer("/$defs/OperatorGpuFreeAttestation/additionalProperties")
                .unwrap(),
            &json!(false)
        );
    }

    fn test_attempt_binding() -> AttemptBinding {
        AttemptBinding {
            campaign_id: "campaign-1".into(),
            campaign_revision_id: "revision-1".into(),
            task_id: "trainer-task-1".into(),
            attempt_id: "attempt-1".into(),
            attempt_number: 1,
            ownership_token: "ownership-1".into(),
        }
    }

    fn trainer_attempt_response(
        api_version: u32,
        resource_id: ResourceId,
        task_id: TaskId,
        attempt_binding: AttemptBinding,
    ) -> TrainerAttemptResponse {
        let machine = MachineId::from_uuid(uuid("019b4f42-0000-7000-8000-000000000065"));
        TrainerAttemptResponse {
            api_version,
            resource: Resource::new(
                resource_id,
                "gpu-a".into(),
                machine,
                SupervisorAddress {
                    machine,
                    thread: ThreadId(uuid("019b4f42-0000-7000-8000-000000000066")),
                },
                AssignmentRevision::new(1),
                ResourceRevision::new(1),
                Some(task_id),
            ),
            task_id,
            runtime_root: "/runtime".into(),
            attempt_binding,
        }
    }

    #[test]
    fn cancel_body_uses_operation_id_and_revision_from_the_contract() {
        let request_id = uuid("019b4f42-0000-7000-8000-000000000007");
        let body = RequestCancelBody {
            api_version: API_VERSION,
            operation_id: request_id,
            expected_revision: ResourceRevision::new(8),
        };
        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value["api_version"], API_VERSION);
        assert_eq!(value["operation_id"], request_id.to_string());
        assert_eq!(value["expected_revision"], 8);
        assert!(value.get("request_id").is_none());
    }

    #[test]
    fn supervisor_replacement_body_uses_the_compare_and_set_revision() {
        let body = SupervisorReplacementBody {
            api_version: API_VERSION,
            expected_revision: ResourceRevision::new(12),
            supervisor: SupervisorAddress {
                machine: MachineId::from_uuid(uuid("019b4f42-0000-7000-8000-000000000031")),
                thread: ThreadId(uuid("019b4f42-0000-7000-8000-000000000032")),
            },
        };
        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value["api_version"], API_VERSION);
        assert_eq!(value["expected_revision"], 12);
        assert_eq!(
            value["supervisor"]["machine"],
            "019b4f42-0000-7000-8000-000000000031"
        );
        assert!(value.get("operation_id").is_none());
    }

    #[test]
    fn registration_and_renotify_envelopes_keep_contract_fields() {
        let id = uuid("019b4f42-0000-7000-8000-000000000013");
        let body = ResourceRegisterBody {
            api_version: API_VERSION,
            spec: ResourceRegistration {
                id: ResourceId::from_uuid(id).unwrap(),
                display_name: "gpu-a".into(),
                supervisor: SupervisorAddress {
                    machine: MachineId::from_uuid(uuid("019b4f42-0000-7000-8000-000000000014")),
                    thread: ThreadId(uuid("019b4f42-0000-7000-8000-000000000015")),
                },
            },
        };
        let registration = serde_json::to_value(body).unwrap();
        assert_eq!(registration["api_version"], API_VERSION);
        assert_eq!(registration["spec"]["id"], id.to_string());
        assert!(registration["spec"].get("authority_machine").is_none());

        let operation_id = uuid("019b4f42-0000-7000-8000-000000000016");
        let body = ResourceActionBody {
            api_version: API_VERSION,
            expected_revision: ResourceRevision::new(4),
            operation_id,
            action: BrowserResourceAction::Renotify {
                notice_id: crate::resource::NoticeId::from_uuid(uuid(
                    "019b4f42-0000-7000-8000-000000000017",
                ))
                .unwrap(),
            },
        };
        let action = serde_json::to_value(body).unwrap();
        assert_eq!(action["operation_id"], operation_id.to_string());
        assert_eq!(action["expected_revision"], 4);
        assert_eq!(action["action"]["type"], "renotify");
    }

    #[test]
    fn submit_body_keeps_the_same_request_id_and_contract_envelope() {
        let request_id = uuid("019b4f42-0000-7000-8000-00000000000a");
        let temp = tempfile::tempdir().unwrap();
        let source = json!({
            "api_version": API_VERSION,
            "thread": "019b4f42-0000-7000-8000-00000000000b",
            "name": "resource task",
            "cwd": temp.path(),
            "timeout": "1h",
            "workload": { "type": "task", "command": ["true"] }
        });
        let parsed = spec::parse_spec_value(&source).unwrap();
        let normalized = spec::normalize(&parsed).unwrap();
        let body = ResourceSubmitBody {
            api_version: API_VERSION,
            request_id: RequestId(request_id),
            spec: normalized,
            env: TaskEnv {
                path: "/bin".into(),
                home: "/tmp".into(),
            },
            callback_cwd: temp.path().to_path_buf(),
        };
        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value["api_version"], API_VERSION);
        assert_eq!(value["request_id"], request_id.to_string());
        assert_eq!(value["spec"]["workload"]["type"], "task");
        assert!(value.get("resource_id").is_none());
        assert_eq!(value["env"]["path"], "/bin");
        assert!(value["callback_cwd"].is_string());
    }

    #[test]
    fn background_response_keeps_resource_request_and_task_identities_separate() {
        let resource_id =
            ResourceId::from_uuid(uuid("019b4f42-0000-7000-8000-000000000043")).unwrap();
        let request_id = RequestId(uuid("019b4f42-0000-7000-8000-000000000044"));
        let task_id = TaskId(uuid("019b4f42-0000-7000-8000-000000000045"));
        let machine = MachineId::from_uuid(uuid("019b4f42-0000-7000-8000-000000000046"));
        let response = ResourceBackgroundSubmitResponse {
            api_version: API_VERSION,
            request_id,
            task_id,
            resource: Resource::new(
                resource_id,
                "gpu-a".into(),
                machine,
                SupervisorAddress {
                    machine,
                    thread: ThreadId(uuid("019b4f42-0000-7000-8000-000000000047")),
                },
                AssignmentRevision::new(1),
                ResourceRevision::new(2),
                None,
            ),
            outcome: ResourceBackgroundSubmitOutcome::Inserted,
        };
        assert!(check_background_response(&response, resource_id, request_id).is_ok());

        let mismatch = check_background_response(
            &response,
            resource_id,
            RequestId(uuid("019b4f42-0000-7000-8000-000000000048")),
        )
        .unwrap_err();
        assert_eq!(mismatch.code(), "resource_outcome_unknown");

        let reused_id = ResourceBackgroundSubmitResponse {
            task_id: TaskId(request_id.0),
            ..response
        };
        let mismatch = check_background_response(&reused_id, resource_id, request_id).unwrap_err();
        assert_eq!(mismatch.code(), "resource_outcome_unknown");
    }

    #[test]
    fn json_and_quiet_output_keep_api_version_and_a_stable_id() {
        let value = json!({ "request_id": "request-1" });
        let rendered = render_output(OutputMode::Json, value.clone(), None, None).unwrap();
        let json_value: Value = serde_json::from_str(&rendered.unwrap()).unwrap();
        assert_eq!(json_value["api_version"], API_VERSION);
        assert_eq!(
            render_output(OutputMode::Quiet, value.clone(), Some("request-1"), None).unwrap(),
            Some("request-1".into())
        );
        assert_eq!(
            render_output(OutputMode::Quiet, value, None, None).unwrap(),
            None
        );
    }

    #[test]
    fn mutation_errors_keep_conflicts_and_mark_transport_failures_unknown() {
        let resource = ResourceId::from_uuid(uuid("019b4f42-0000-7000-8000-000000000008")).unwrap();
        let operation = uuid("019b4f42-0000-7000-8000-000000000009");
        let conflict = mutation_error(
            resource,
            Some(operation),
            "operation id retry-this",
            AppError::ResourceOperationConflict {
                resource,
                operation: Some(operation),
                message: "changed content".into(),
            },
        );
        assert_eq!(conflict.code(), "resource_operation_conflict");
        let unknown = mutation_error(
            resource,
            Some(operation),
            "operation id retry-this",
            AppError::DaemonUnavailable {
                message: "socket closed after request".into(),
            },
        );
        assert_eq!(unknown.code(), "resource_outcome_unknown");
        assert!(
            unknown
                .to_string()
                .contains("retry with the same operation id retry-this")
        );

        let socket_conflict = mutation_error(
            resource,
            Some(operation),
            "operation id retry-this",
            AppError::Internal {
                message: "http 409 resource operation conflict: identity already used".into(),
            },
        );
        assert_eq!(socket_conflict.code(), "resource_operation_conflict");

        let stale = mutation_error(
            resource,
            Some(operation),
            "operation id retry-this",
            AppError::Internal {
                message: "http 409 resource revision is stale: expected 3, current 4".into(),
            },
        );
        assert_eq!(stale.code(), "resource_stale_revision");
    }

    fn action_authority(co_located: bool) -> SupervisorActionAuthority {
        let authority_machine = MachineId::new();
        SupervisorActionAuthority {
            authority_machine,
            resource_id: ResourceId::new(),
            loan_id: crate::resource::LoanId::new(),
            action_id: ActionId::new(),
            expected_state_revision: ResourceRevision::new(4),
            supervisor: SupervisorAddress {
                machine: if co_located {
                    authority_machine
                } else {
                    MachineId::new()
                },
                thread: ThreadId(Uuid::now_v7()),
            },
            assignment_revision: AssignmentRevision::new(1),
        }
    }

    #[test]
    fn pending_action_on_a_fresh_resource_keeps_revision_zero() {
        let authority = action_authority(true);
        let action = PendingActionView {
            resource_id: authority.resource_id,
            authority_machine: authority.authority_machine,
            loan_id: authority.loan_id,
            action_id: authority.action_id,
            state_revision: ResourceRevision::new(0),
            supervisor: authority.supervisor,
            assignment_revision: AssignmentRevision::new(0),
            phase: PendingActionPhase::ReturnRequired,
            return_context: None,
            notice: None,
            return_window: None,
        };

        let context = action_context(&action).unwrap();

        assert_eq!(
            context.authority.expected_state_revision,
            ResourceRevision::new(0)
        );
        assert_eq!(
            context.authority.assignment_revision,
            AssignmentRevision::new(0)
        );
    }

    fn return_launch_request(
        authority: SupervisorActionAuthority,
    ) -> (ResourceActionSubmitRequest, RequestId, TaskId) {
        let spec: NormalizedSpec = serde_json::from_value(json!({
            "api_version": 1,
            "thread": authority.supervisor.thread,
            "name": "return",
            "cwd": "/tmp",
            "timeout": "4h",
            "workload": { "type": "task", "command": ["/bin/echo", "resume"] }
        }))
        .unwrap();
        let (request_id, task_id) = (RequestId::new(), TaskId::new());
        let request = ResourceActionSubmitRequest {
            api_version: API_VERSION,
            authority,
            choice: ResourceActionChoice::Return {
                decision: ReturnDecision::Launch(Box::new(ReturnLaunch {
                    request_id,
                    task_id,
                    work: ReturnWork::NewBackgroundWork {
                        spec: CommandSpec::try_from(spec).unwrap(),
                    },
                })),
            },
        };
        (request, request_id, task_id)
    }

    #[test]
    fn action_outcome_receipt_must_fit_the_supervisor_placement_and_task() {
        use crate::resource::bound_action::{ActionTaskReceipt, LocalReturnReceipt};

        let local = action_authority(true);
        let (request, request_id, task_id) = return_launch_request(local);
        let local_accepted =
            |authority, request_id, task_id| ResourceActionSubmitOutcome::LocalReturnAccepted {
                receipt: LocalReturnReceipt {
                    authority,
                    request_id,
                    task_id,
                },
                acceptance: LocalReturnAcceptance::Existing {
                    state: crate::domain::ProcessStatus::Queued,
                },
            };
        assert_eq!(
            check_action_outcome(&request, &local_accepted(local, request_id, task_id)),
            Ok(())
        );
        assert!(
            check_action_outcome(&request, &local_accepted(local, request_id, TaskId::new()))
                .is_err()
        );
        let mut stale = local;
        stale.expected_state_revision = ResourceRevision::new(5);
        assert!(
            check_action_outcome(&request, &local_accepted(stale, request_id, task_id)).is_err()
        );
        let digest = serde_json::from_value(json!("0".repeat(64))).unwrap();
        let remote_receipt = |authority| ResourceActionSubmitOutcome::Accepted {
            receipt: ActionTaskReceipt {
                kind: ResourceActionKind::Return,
                authority,
                request_id,
                task_id,
                normalized_spec_sha256: digest,
            },
            last_execution_state: None,
        };
        // a co-located task has no remote receipt
        assert!(check_action_outcome(&request, &remote_receipt(local)).is_err());

        let remote = action_authority(false);
        let (remote_request, request_id, task_id) = return_launch_request(remote);
        let remote_receipt = ResourceActionSubmitOutcome::Accepted {
            receipt: ActionTaskReceipt {
                kind: ResourceActionKind::Return,
                authority: remote,
                request_id,
                task_id,
                normalized_spec_sha256: digest,
            },
            last_execution_state: None,
        };
        assert_eq!(
            check_action_outcome(&remote_request, &remote_receipt),
            Ok(())
        );
        // a remote supervisor owns its own route, so a local receipt is never valid
        assert!(
            check_action_outcome(
                &remote_request,
                &local_accepted(remote, request_id, task_id)
            )
            .is_err()
        );
    }
}
