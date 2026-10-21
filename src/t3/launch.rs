//! Start a new top-level T3 thread on this machine, as T3's own
//! `t3_thread_launch` MCP tool does
//!
//! - `orchestration.launchThread` (`OrchestrationV2ThreadLaunchInput` in
//!   `packages/contracts/src/orchestrationV2.ts`) creates the thread, prepares
//!   its workspace, runs the project setup script, and starts the first
//!   message in one call. It needs the `orchestration:operate` scope. A repeated
//!   `commandId` returns the first thread with `resumed: true` instead of
//!   launching again. A workspace that cannot be prepared, such as a `baseRef`
//!   that does not resolve, fails the run inside the new thread, not the call;
//!   the MCP tool checks those up front, the RPC does not
//! - The MCP endpoint itself accepts only the credential T3 issues each of its
//!   own threads, so a bearer session cannot call `t3_thread_launch`
//! - Projects come from `projection_projects` in `statev2.sqlite`. The default
//!   model comes from `settings.json`: the project's entry under
//!   `projectSettingsOverrides`, then the project's
//!   `default_model_selection_json`, then `defaultModelSelection`. That order is
//!   homebased's choice; T3 documents none
//! - `environment-id` in the user data names this T3 environment, which the
//!   `t3code://threads/<environment>/<thread>` link needs to open a thread on
//!   another machine

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use super::rpc::{self, Exit};
use super::{
    ApiFailure, IssueError, T3Cli, T3Env, connect, deterministic_id, state_connection, state_path,
};

const LAUNCH_METHOD: &str = "orchestration.launchThread";
const LAUNCH_ERROR: &str = "OrchestrationV2ThreadLaunchError";
// the call returns once the thread exists; workspace preparation runs after it
const LAUNCH_TIMEOUT: Duration = Duration::from_secs(30);
/// Longest first message in UTF-8 bytes, below T3's 120,000-character limit
pub const MESSAGE_MAX_BYTES: usize = 100 * 1024;
const TITLE_MAX_CHARS: usize = 80;
const PROJECTS_SQL: &str = "
    SELECT project_id, title, workspace_root, default_model_selection_json
    FROM projection_projects
    WHERE deleted_at IS NULL
    ORDER BY title, project_id";
// projects listed in a not-found error, enough to spot the intended one
const LISTED_PROJECTS: usize = 50;

/// Where a launched thread works
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Workspace {
    /// The project checkout
    #[default]
    Root,
    /// A new git worktree that T3 creates and runs the project setup in
    Worktree {
        /// Branch or commit the worktree starts from
        base_ref: String,
        /// Branch to create; T3 names one from the first message when omitted
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
        /// Fetch `base_ref` from origin before creating the worktree
        #[serde(default)]
        start_from_origin: bool,
    },
    /// One of the project's existing git worktrees
    ExistingWorktree {
        /// Absolute path, or a path starting with `~/`, on the launching machine
        path: String,
        /// Branch to record for the thread
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<String>,
    },
}

/// Permission mode of a launched thread, as T3 names it
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeMode {
    /// Every action that needs approval waits for a person
    ApprovalRequired,
    /// Edits run, other actions wait for approval
    AutoAcceptEdits,
    /// The provider decides what needs approval
    Auto,
    /// Nothing waits for approval; a thread on another machine has nobody to approve it
    #[default]
    FullAccess,
}

/// Whether a launched thread plans before it acts
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum InteractionMode {
    /// Act on the message
    #[default]
    Default,
    /// Plan first
    Plan,
}

/// Provider instance and model for a launched thread
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelChoice {
    /// T3 provider instance id, such as `claudeAgent` or `codex`
    pub provider: String,
    /// Model id, such as `claude-opus-5-5`
    pub model: String,
}

/// One model option, such as `effort` for Claude or `reasoningEffort` for Codex
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelOption {
    /// Option id the provider advertises
    pub id: String,
    /// Selected value
    pub value: OptionValue,
}

/// Value of a model option; T3 options are strings or booleans
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OptionValue {
    /// Boolean option, such as `fastMode`
    Flag(bool),
    /// Select option, such as an effort level
    Text(String),
}

/// What to launch, independent of the machine that launches it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchSpec {
    /// Caller retry identity; a repeat returns the thread the first call made
    pub request_id: Uuid,
    /// T3 project id, its workspace root path, or its exact title
    pub project: String,
    /// Thread title; derived from the message when omitted
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// First message; the thread starts idle without one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Where the thread works
    #[serde(default)]
    pub workspace: Workspace,
    /// Provider and model; the T3 default for the project when omitted
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelChoice>,
    /// Model options, which replace default options with the same id
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_options: Vec<ModelOption>,
    /// Permission mode
    #[serde(default)]
    pub runtime_mode: RuntimeMode,
    /// Interaction mode
    #[serde(default)]
    pub interaction_mode: InteractionMode,
}

impl LaunchSpec {
    /// Check the fields that do not depend on the launching machine
    pub fn validate(&self) -> Result<(), String> {
        let blank = |value: &str| value.trim().is_empty();
        if self.request_id.is_nil() {
            return Err("request id must not be nil".into());
        }
        if blank(&self.project) {
            return Err("project must not be empty".into());
        }
        if self.title.as_deref().is_some_and(blank) {
            return Err("title must not be blank".into());
        }
        if self.message.as_deref().is_some_and(blank) {
            return Err("message must not be blank".into());
        }
        if self.title.is_none() && self.message.is_none() {
            return Err("pass a title, a message, or both".into());
        }
        if self
            .message
            .as_ref()
            .is_some_and(|message| message.len() > MESSAGE_MAX_BYTES)
        {
            return Err(format!(
                "message is longer than {MESSAGE_MAX_BYTES} bytes; put the detail in a file and name it"
            ));
        }

        let branch = match &self.workspace {
            Workspace::Root => None,
            Workspace::Worktree {
                base_ref, branch, ..
            } => {
                if blank(base_ref) {
                    return Err("worktree base ref must not be empty".into());
                }
                branch.as_deref()
            }
            Workspace::ExistingWorktree { path, branch } => {
                if !(path.starts_with('/') || path.starts_with("~/")) {
                    return Err("existing worktree path must be absolute or start with ~/".into());
                }
                branch.as_deref()
            }
        };
        if branch.is_some_and(blank) {
            return Err("branch must not be blank".into());
        }

        if let Some(model) = &self.model
            && (blank(&model.provider) || blank(&model.model))
        {
            return Err("provider and model must not be empty".into());
        }
        if self.model_options.iter().any(|option| blank(&option.id)) {
            return Err("model option ids must not be empty".into());
        }
        Ok(())
    }
}

/// A thread that T3 launched
///
/// Unknown fields are ignored, so a newer peer can add some without its
/// answer to a launch that already happened failing to decode
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchedThread {
    /// T3 thread id, fixed by the request id
    pub thread_id: String,
    /// T3 project id
    pub project_id: String,
    /// T3 project title
    pub project_title: String,
    /// Project checkout on the launching machine
    pub workspace_root: String,
    /// Model selection the thread runs, as T3 stores it
    pub model: Value,
    /// Whether an earlier call with the same request id already launched it
    pub resumed: bool,
    /// Link that opens the thread in T3 Code on any machine connected to this
    /// environment
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
}

/// A T3 project on this machine
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct T3Project {
    /// T3 project id
    pub id: String,
    /// T3 project title
    pub title: String,
    /// Project checkout
    pub workspace_root: String,
    /// Model a launch uses when it names none; `None` means a launch must name one
    pub default_model: Option<Value>,
}

/// Why a launch did not happen, or may have happened
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchError {
    /// No project matches the selector
    ProjectNotFound {
        /// Project the caller named
        selector: String,
        /// Title and root of the projects T3 has, up to 50
        projects: Vec<String>,
    },
    /// The request cannot launch as given, or T3 refused it
    Refused(String),
    /// T3 is not installed or running, or a transport failed before the request left
    Unavailable(String),
    /// The request reached T3 but no reply came back; repeating the request id
    /// is safe because T3 launches once per command id
    Uncertain(String),
    /// A T3 response did not match the pinned contract
    Changed(String),
}

impl From<ApiFailure> for LaunchError {
    fn from(failure: ApiFailure) -> Self {
        match failure {
            ApiFailure::Delivery(failure) => Self::Unavailable(failure.to_string()),
            ApiFailure::Unavailable(detail) => Self::Unavailable(detail),
            ApiFailure::Refused(detail) => Self::Refused(detail),
            ApiFailure::Uncertain(detail) => Self::Uncertain(detail),
            ApiFailure::Changed(detail) => Self::Changed(detail),
        }
    }
}

/// Projects T3 has on this machine, each with the model a launch uses by default
///
/// Reads the state database and settings on disk, so the server need not run
pub fn projects(env: &T3Env) -> Result<Vec<T3Project>, LaunchError> {
    let userdata = env.userdata();
    let state = state_path(&userdata);
    if !state.is_file() {
        return Err(LaunchError::Unavailable(
            "T3 has no state database on this machine".into(),
        ));
    }
    let settings = read_json(&userdata.join("settings.json"));

    let db = state_connection(&state)
        .map_err(|_| LaunchError::Unavailable("statev2.sqlite cannot be read".into()))?;
    let mut statement = db
        .prepare(PROJECTS_SQL)
        .map_err(|_| LaunchError::Changed("T3 project table cannot be read".into()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .and_then(Iterator::collect::<rusqlite::Result<Vec<_>>>)
        .map_err(|_| LaunchError::Changed("T3 project table cannot be read".into()))?;

    Ok(rows
        .into_iter()
        .map(|(id, title, workspace_root, own_default)| {
            let own_default = own_default.and_then(|text| serde_json::from_str(&text).ok());
            T3Project {
                default_model: default_model(&settings, &id, own_default),
                id,
                title,
                workspace_root,
            }
        })
        .collect())
}

/// Launch a thread in T3 on this machine
///
/// Project and model resolve from disk before T3 is contacted, so a request
/// that cannot launch issues no token. The bearer token is revoked on return
pub fn launch_thread(env: &T3Env, spec: &LaunchSpec) -> Result<LaunchedThread, LaunchError> {
    spec.validate().map_err(LaunchError::Refused)?;
    let projects = projects(env)?;
    let project = find_project(&projects, &spec.project, &env.home)?;
    let model = model_selection(spec, project)?;

    let userdata = env.userdata();
    let server = connect(&userdata)?;
    let cli = T3Cli::resolve(env, server.pid).map_err(LaunchError::Unavailable)?;
    let token = cli.issue_token().map_err(|error| match error {
        IssueError::Unavailable(detail) => LaunchError::Unavailable(detail),
        IssueError::Changed(detail) => LaunchError::Changed(detail),
    })?;

    let ids = LaunchIds::new(spec.request_id);
    let payload = launch_payload(spec, project, &model, &ids, &env.home);
    let reply = match rpc::call(
        &server.origin,
        &token.value,
        LAUNCH_METHOD,
        &payload,
        LAUNCH_TIMEOUT,
    ) {
        Ok(Exit::Success(value)) => value,
        Ok(Exit::Failure(cause)) => return Err(launch_failure(&cause)),
        Err(error) if error.sent => return Err(LaunchError::Uncertain(error.failure.detail())),
        Err(error) => return Err(error.failure.into()),
    };

    // the thread id is ours, so a reply for another thread is a changed contract
    if reply.get("threadId").and_then(Value::as_str) != Some(ids.thread.as_str()) {
        return Err(LaunchError::Changed(
            "thread launch replied for another thread".into(),
        ));
    }
    let environment = fs::read_to_string(userdata.join("environment-id"))
        .ok()
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty());

    // a retry with the same request id returns the first launch, whose project
    // and model may differ from this request's, so report what T3 holds
    let launched = reply.pointer("/projection/thread");
    let project = launched
        .and_then(|thread| thread.get("projectId"))
        .and_then(Value::as_str)
        .and_then(|id| projects.iter().find(|project| project.id == id))
        .unwrap_or(project);
    let model = launched
        .and_then(|thread| thread.get("modelSelection"))
        .filter(|selection| is_model_selection(selection))
        .cloned()
        .unwrap_or(model);

    Ok(LaunchedThread {
        link: environment
            .map(|environment| format!("t3code://threads/{environment}/{}", ids.thread)),
        thread_id: ids.thread,
        project_id: project.id.clone(),
        project_title: project.title.clone(),
        workspace_root: project.workspace_root.clone(),
        model,
        resumed: reply
            .get("resumed")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

/// T3 ids for one launch, all fixed by the request id so a retry repeats them
struct LaunchIds {
    command: String,
    thread: String,
    message: String,
}

impl LaunchIds {
    fn new(request: Uuid) -> Self {
        let request = request.to_string();
        Self {
            command: format!("homebased:{request}"),
            thread: deterministic_id("homebased-t3-launch-thread", &request, ""),
            message: deterministic_id("homebased-t3-launch-message", &request, ""),
        }
    }
}

fn launch_payload(
    spec: &LaunchSpec,
    project: &T3Project,
    model: &Value,
    ids: &LaunchIds,
    home: &Path,
) -> Value {
    let strategy = match &spec.workspace {
        Workspace::Root => json!({ "type": "root" }),
        Workspace::Worktree {
            base_ref,
            branch,
            start_from_origin,
        } => {
            let mut strategy = json!({
                "type": "worktree",
                "baseRef": base_ref,
                "startFromOrigin": start_from_origin,
            });
            if let Some(branch) = branch {
                strategy["branch"] = json!(branch);
            }
            strategy
        }
        Workspace::ExistingWorktree { path, branch } => {
            let mut strategy = json!({
                "type": "existing_worktree",
                "worktreePath": expand_home(path, home),
            });
            if let Some(branch) = branch {
                strategy["branch"] = json!(branch);
            }
            strategy
        }
    };
    let (title, generated) = match (&spec.title, &spec.message) {
        (Some(title), _) => (title.trim().to_owned(), false),
        (None, Some(message)) => (title_from(message), true),
        // validation requires a title or a message
        (None, None) => (String::new(), false),
    };

    let mut payload = json!({
        "commandId": ids.command,
        "creationSource": "server",
        "threadId": ids.thread,
        "projectId": project.id,
        "title": title,
        "modelSelection": model,
        "runtimeMode": spec.runtime_mode,
        "interactionMode": spec.interaction_mode,
        "workspaceStrategy": strategy,
    });
    // a placeholder title from the message, which T3 replaces with a generated one
    if generated {
        payload["generateTitle"] = json!(true);
    }
    if let Some(text) = &spec.message {
        payload["initialMessage"] = json!({
            "messageId": ids.message,
            "text": text,
            "attachments": [],
        });
    }
    payload
}

/// First non-blank line of the message, cut to a sidebar-sized title
fn title_from(message: &str) -> String {
    let line = message
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("New thread");
    let mut title: String = line.chars().take(TITLE_MAX_CHARS).collect();
    if line.chars().count() > TITLE_MAX_CHARS {
        title.push('…');
    }
    title
}

/// A typed launch refusal, an uncertain outcome for a defect, or a changed contract
fn launch_failure(cause: &[Value]) -> LaunchError {
    let Some(first) = cause.first() else {
        return LaunchError::Changed("thread launch failed with an empty cause".into());
    };
    match first.get("_tag").and_then(Value::as_str) {
        Some("Fail") => {
            let error = first.get("error");
            let tag = error
                .and_then(|error| error.get("_tag"))
                .and_then(Value::as_str)
                .unwrap_or("an untagged error");
            if tag != LAUNCH_ERROR {
                return LaunchError::Changed(format!("thread launch failed with {tag}"));
            }
            let message = error
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("no reason given");
            LaunchError::Refused(format!("T3 refused the launch: {message}"))
        }
        // a defect or interruption can stop a launch partway; T3 finishes it on a retry
        Some("Die" | "Interrupt") => LaunchError::Uncertain(
            "thread launch stopped before it finished; retry the same request id".into(),
        ),
        _ => LaunchError::Changed("thread launch failed with an unknown cause".into()),
    }
}

/// The one project `selector` names: an id, a workspace root, or a title
fn find_project<'a>(
    projects: &'a [T3Project],
    selector: &str,
    home: &Path,
) -> Result<&'a T3Project, LaunchError> {
    if let Some(project) = projects.iter().find(|project| project.id == selector) {
        return Ok(project);
    }

    let matches: Vec<&T3Project> = if selector.starts_with('/') || selector.starts_with("~/") {
        let wanted = comparable(&expand_home(selector, home));
        projects
            .iter()
            .filter(|project| comparable(Path::new(&project.workspace_root)) == wanted)
            .collect()
    } else {
        projects
            .iter()
            .filter(|project| project.title == selector)
            .collect()
    };
    match matches.as_slice() {
        [project] => Ok(project),
        [] => Err(LaunchError::ProjectNotFound {
            selector: selector.to_owned(),
            projects: projects
                .iter()
                .take(LISTED_PROJECTS)
                .map(|project| format!("{} ({})", project.title, project.workspace_root))
                .collect(),
        }),
        several => Err(LaunchError::Refused(format!(
            "{} projects match {selector:?}; pass one of their ids: {}",
            several.len(),
            several
                .iter()
                .map(|project| project.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// A path as it compares to another: resolved when it exists, else without a trailing slash
fn comparable(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| {
        let text = path.to_string_lossy();
        PathBuf::from(text.trim_end_matches('/'))
    })
}

fn expand_home(path: &str, home: &Path) -> PathBuf {
    match path.strip_prefix("~/") {
        Some(relative) => home.join(relative),
        None => PathBuf::from(path),
    }
}

/// The explicit model, else the project default, with the caller's options applied
fn model_selection(spec: &LaunchSpec, project: &T3Project) -> Result<Value, LaunchError> {
    let mut selection = match &spec.model {
        Some(choice) => json!({ "instanceId": choice.provider, "model": choice.model }),
        None => project.default_model.clone().ok_or_else(|| {
            LaunchError::Refused(format!(
                "pass --provider and --model: T3 names no default model for project {} on this machine",
                project.title
            ))
        })?,
    };
    if spec.model_options.is_empty() {
        return Ok(selection);
    }

    // an explicit model starts without options, since another model's may not apply
    let mut options = match (&spec.model, selection.get("options")) {
        (None, Some(Value::Array(options))) => options.clone(),
        _ => Vec::new(),
    };
    for option in &spec.model_options {
        let value = json!({ "id": option.id, "value": option.value });
        match options
            .iter_mut()
            .find(|existing| existing.get("id").and_then(Value::as_str) == Some(&option.id))
        {
            Some(existing) => *existing = value,
            None => options.push(value),
        }
    }
    selection["options"] = Value::Array(options);
    Ok(selection)
}

/// The first usable default model among the settings and the project's own
fn default_model(settings: &Value, project_id: &str, own: Option<Value>) -> Option<Value> {
    let escaped = project_id.replace('~', "~0").replace('/', "~1");
    let overridden = settings
        .pointer(&format!(
            "/projectSettingsOverrides/{escaped}/defaultModelSelection"
        ))
        .cloned();
    [
        overridden,
        own,
        settings.get("defaultModelSelection").cloned(),
    ]
    .into_iter()
    .flatten()
    .find(is_model_selection)
}

fn is_model_selection(value: &Value) -> bool {
    let named = |key| {
        value
            .get(key)
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty())
    };
    named("instanceId") && named("model")
}

/// JSON in a file, or null when it is missing or invalid
fn read_json(path: &Path) -> Value {
    fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::super::test_support::{
        FakeResponse, FakeT3Server, Fixture, live_pid, rpc_exit, ticket,
    };
    use super::{
        InteractionMode, LaunchError, LaunchIds, LaunchSpec, ModelChoice, ModelOption, OptionValue,
        RuntimeMode, Workspace, launch_thread, projects,
    };

    fn curl_available() -> bool {
        which::which("curl").is_ok()
    }

    fn spec(project: &str) -> LaunchSpec {
        LaunchSpec {
            request_id: Uuid::now_v7(),
            project: project.into(),
            title: None,
            message: Some("Profile the release build\nthen report the hot paths".into()),
            workspace: Workspace::Root,
            model: None,
            model_options: Vec::new(),
            runtime_mode: RuntimeMode::FullAccess,
            interaction_mode: InteractionMode::Default,
        }
    }

    fn launched(thread: &str, resumed: bool, model: &Value) -> FakeResponse {
        FakeResponse::WebSocket(vec![rpc_exit(json!({
            "_tag": "Success",
            "value": {
                "threadId": thread,
                "projection": { "thread": { "projectId": "project-app", "modelSelection": model } },
                "resumed": resumed
            }
        }))])
    }

    fn write_settings(fixture: &Fixture, settings: &Value) {
        fs::write(
            fixture.userdata().join("settings.json"),
            settings.to_string(),
        )
        .unwrap();
    }

    #[test]
    fn a_launch_sends_one_rpc_whose_retry_returns_the_same_thread() {
        if !curl_available() {
            return;
        }
        let mut spec = spec("~/code/app");
        spec.workspace = Workspace::Worktree {
            base_ref: "main".into(),
            branch: Some("perf/release".into()),
            start_from_origin: true,
        };
        spec.model_options = vec![ModelOption {
            id: "effort".into(),
            value: OptionValue::Text("max".into()),
        }];
        let thread = LaunchIds::new(spec.request_id).thread;
        let first_model = json!({
            "instanceId": "claudeAgent",
            "model": "claude-opus-5-5",
            "options": [{ "id": "effort", "value": "max" }, { "id": "contextWindow", "value": "1m" }]
        });
        let server = FakeT3Server::start(
            Some(2),
            vec![
                ticket(),
                launched(&thread, false, &first_model),
                ticket(),
                // T3 answers a repeated request with the thread the first one made
                launched(&thread, true, &first_model),
            ],
        );
        let fixture = Fixture::empty(Some(&server), live_pid());
        let root = fixture.dir.path().join("code/app");
        fs::create_dir_all(&root).unwrap();
        fixture
            .v2_state()
            .launch_project("project-app", "app", &root.to_string_lossy(), None);
        write_settings(
            &fixture,
            &json!({ "defaultModelSelection": {
                "instanceId": "claudeAgent",
                "model": "claude-opus-5-5",
                "options": [{ "id": "effort", "value": "high" }, { "id": "contextWindow", "value": "1m" }]
            }}),
        );
        fs::write(fixture.userdata().join("environment-id"), "env-1\n").unwrap();

        let first = launch_thread(&fixture.env, &spec).unwrap();
        let mut changed = spec.clone();
        changed.model = Some(ModelChoice {
            provider: "codex".into(),
            model: "gpt-6.1-sol".into(),
        });
        let retry = launch_thread(&fixture.env, &changed).unwrap();

        assert_eq!(first.thread_id, thread);
        assert!(!first.resumed);
        assert!(retry.resumed);
        assert_eq!(retry.thread_id, first.thread_id);
        // the retry reports the model T3 launched, not the one it asked for
        assert_eq!(retry.model, first_model);
        assert_eq!(first.project_id, "project-app");
        assert_eq!(
            first.link.as_deref(),
            Some(format!("t3code://threads/env-1/{thread}").as_str())
        );
        let requests = server.requests();
        assert_eq!(requests.len(), 4);
        let rpc: Value = serde_json::from_str(&requests[1].body).unwrap();
        let retried: Value = serde_json::from_str(&requests[3].body).unwrap();
        assert_eq!(rpc["tag"], "orchestration.launchThread");
        assert_eq!(rpc["payload"]["commandId"], retried["payload"]["commandId"]);
        assert_eq!(rpc["payload"]["threadId"], retried["payload"]["threadId"]);
        assert_eq!(
            rpc["payload"]["initialMessage"]["messageId"],
            retried["payload"]["initialMessage"]["messageId"]
        );
        let payload = &rpc["payload"];
        assert_eq!(
            payload["commandId"],
            format!("homebased:{}", spec.request_id)
        );
        assert_eq!(payload["threadId"], thread);
        assert_eq!(payload["projectId"], "project-app");
        assert_eq!(payload["title"], "Profile the release build");
        assert_eq!(payload["generateTitle"], true);
        assert_eq!(payload["runtimeMode"], "full-access");
        assert_eq!(payload["interactionMode"], "default");
        assert_eq!(
            payload["workspaceStrategy"],
            json!({ "type": "worktree", "baseRef": "main", "branch": "perf/release", "startFromOrigin": true })
        );
        // the caller's effort replaces the default's, which keeps its other options
        assert_eq!(
            payload["modelSelection"],
            json!({
                "instanceId": "claudeAgent",
                "model": "claude-opus-5-5",
                "options": [{ "id": "effort", "value": "max" }, { "id": "contextWindow", "value": "1m" }]
            })
        );
        assert_eq!(
            payload["initialMessage"]["text"],
            "Profile the release build\nthen report the hot paths"
        );
        assert_eq!(fixture.revocations(), ["fake-session", "fake-session"]);
    }

    #[test]
    fn a_launch_that_cannot_resolve_contacts_no_t3_and_a_refusal_names_why() {
        if !curl_available() {
            return;
        }
        let refusal = FakeResponse::WebSocket(vec![rpc_exit(json!({
            "_tag": "Failure",
            "cause": [{ "_tag": "Fail", "error": {
                "_tag": "OrchestrationV2ThreadLaunchError",
                "commandId": "homebased:x",
                "projectId": "project-app",
                "message": "Provider instance codex is disabled."
            }}]
        }))]);
        let server = FakeT3Server::start(Some(2), vec![ticket(), refusal]);
        let fixture = Fixture::empty(Some(&server), live_pid());
        fixture
            .v2_state()
            .launch_project("project-app", "app", "/srv/app", None);

        let no_model = launch_thread(&fixture.env, &spec("app"));
        let unknown = launch_thread(&fixture.env, &spec("/srv/missing"));
        assert!(server.requests().is_empty());
        assert!(fixture.revocations().is_empty());

        let mut explicit = spec("project-app");
        explicit.model = Some(ModelChoice {
            provider: "codex".into(),
            model: "gpt-6.1-sol".into(),
        });
        let refused = launch_thread(&fixture.env, &explicit);

        assert!(
            matches!(&no_model, Err(LaunchError::Refused(detail)) if detail.contains("--provider and --model")),
            "{no_model:?}"
        );
        assert_eq!(
            unknown,
            Err(LaunchError::ProjectNotFound {
                selector: "/srv/missing".into(),
                projects: vec!["app (/srv/app)".into()],
            })
        );
        assert_eq!(
            refused,
            Err(LaunchError::Refused(
                "T3 refused the launch: Provider instance codex is disabled.".into()
            ))
        );
        let rpc: Value = serde_json::from_str(&server.requests()[1].body).unwrap();
        assert_eq!(
            rpc["payload"]["modelSelection"],
            json!({ "instanceId": "codex", "model": "gpt-6.1-sol" })
        );
        assert_eq!(fixture.revocations(), ["fake-session"]);
    }

    #[test]
    fn a_project_default_model_prefers_its_settings_override_then_its_own_then_the_global() {
        let fixture = Fixture::empty(None, live_pid());
        let model = |name: &str| json!({ "instanceId": "codex", "model": name });
        fixture
            .v2_state()
            .launch_project("overridden", "a", "/srv/a", Some(&model("own-a")))
            .launch_project("own", "b", "/srv/b", Some(&model("own-b")))
            .launch_project("plain", "c", "/srv/c", None)
            .launch_project(
                "broken",
                "d",
                "/srv/d",
                Some(&json!({ "model": "no-instance" })),
            );
        write_settings(
            &fixture,
            &json!({
                "defaultModelSelection": model("global"),
                "projectSettingsOverrides": {
                    "overridden": { "defaultModelSelection": model("override-a") }
                }
            }),
        );

        let defaults: Vec<(String, Option<Value>)> = projects(&fixture.env)
            .unwrap()
            .into_iter()
            .map(|project| (project.id, project.default_model))
            .collect();

        assert_eq!(
            defaults,
            [
                ("overridden".into(), Some(model("override-a"))),
                ("own".into(), Some(model("own-b"))),
                ("plain".into(), Some(model("global"))),
                ("broken".into(), Some(model("global"))),
            ]
        );
    }
}
