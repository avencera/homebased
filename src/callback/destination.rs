//! Submit-time check that a spec's callback `thread` can receive events
//!
//! Events go to the origin machine's Claude Code session or Codex thread
//! named by `thread`. A thread that no session on this machine owns is
//! accepted by the daemon and only fails when the first event is sent, often
//! hours later, so the submitting CLI checks it before any work starts. The
//! executor of a remote task never sees this check

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use uuid::Uuid;

use crate::domain::{TaskId, ThreadId};
use crate::error::AppError;
use crate::home::Home;
use crate::store::Store;

/// Env var that marks the submitter as a Homebased worker
const TASK_ID_ENV: &str = "HOMEBASED_TASK_ID";

/// Shortest shared run of hex digits at either end that marks a likely typo
///
/// Random ids share 12 leading or trailing hex digits by chance about once in
/// 2^48 pairs. Two UUIDv7 ids share 12 leading digits only when created in the
/// same millisecond
const MIN_SHARED_RUN: usize = 12;

/// Most differing hex digits that still marks a likely typo
const MAX_CHANGED_DIGITS: usize = 4;

/// Most suggestions to return
const MAX_SUGGESTIONS: usize = 3;

/// Kind of session that owns a callback thread on this machine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadDestination {
    /// A Claude Code session with a registry file or transcript
    ClaudeSession,
    /// A Codex thread with a session rollout file
    CodexThread,
}

impl ThreadDestination {
    fn label(self) -> &'static str {
        match self {
            Self::ClaudeSession => "Claude Code session",
            Self::CodexThread => "Codex thread",
        }
    }
}

/// Known thread that differs from the submitted one by a likely typo
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ThreadSuggestion {
    /// Known thread id
    pub thread: ThreadId,
    /// Session kind that owns it
    pub destination: ThreadDestination,
}

/// Task that runs the submitting Homebased worker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ParentTask {
    /// Worker task id from `HOMEBASED_TASK_ID`
    pub(crate) task: TaskId,
    /// Thread that receives the worker task's events
    pub(crate) thread: ThreadId,
}

/// Session stores and worker context of the user who submits a spec
#[derive(Debug, Clone)]
pub(crate) struct SubmitOrigin {
    /// `~/.claude`, which holds the session registry and transcripts
    claude_dir: PathBuf,
    /// `$CODEX_HOME/sessions`, or `~/.codex/sessions`
    codex_sessions: PathBuf,
    /// Homebased task directories, to explain a task id used as a thread
    tasks_dir: PathBuf,
    /// Parent task when the submitter is itself a Homebased worker
    parent: Option<ParentTask>,
}

impl SubmitOrigin {
    /// Read the submitter's session stores and worker task from the environment
    ///
    /// Uses the same `HOME` that the daemon saves for callback delivery
    pub(crate) fn capture(home: &Home) -> Result<Self, AppError> {
        let user_home = std::env::var_os("HOME")
            .filter(|home| !home.is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| AppError::Usage {
                message: "HOME must be set to check the spec thread".into(),
            })?;
        let codex_home = std::env::var_os("CODEX_HOME")
            .filter(|path| !path.is_empty())
            .map_or_else(|| user_home.join(".codex"), PathBuf::from);
        let parent = parent_task(home, std::env::var_os(TASK_ID_ENV))?;

        Ok(Self {
            claude_dir: user_home.join(".claude"),
            codex_sessions: codex_home.join("sessions"),
            tasks_dir: home.tasks_dir(),
            parent,
        })
    }

    /// Check that `thread` can receive events from this machine
    ///
    /// A worker must use its parent task's thread unless `allow_other_thread`
    pub(crate) fn check(
        &self,
        thread: ThreadId,
        allow_other_thread: bool,
    ) -> Result<ThreadDestination, AppError> {
        if let Some(parent) = self.parent
            && parent.thread != thread
            && !allow_other_thread
        {
            return Err(AppError::ThreadMismatch {
                thread,
                parent_task: parent.task,
                parent_thread: parent.thread,
            });
        }

        let known = self.known_threads();
        if let Some(destination) = known.get(&thread.0) {
            return Ok(*destination);
        }

        Err(self.unknown_thread(thread, &known))
    }

    fn unknown_thread(
        &self,
        thread: ThreadId,
        known: &BTreeMap<Uuid, ThreadDestination>,
    ) -> AppError {
        let suggestions = suggestions(thread, known);
        let mut message = format!(
            "spec field thread {thread} names no Claude Code session or Codex thread on this machine, so its events could not be delivered"
        );
        if let Some(best) = suggestions.first() {
            message.push_str(&format!(
                "; did you mean {} ({})?",
                best.thread,
                best.destination.label()
            ));
        }
        if self.tasks_dir.join(thread.to_string()).is_dir() {
            message.push_str(
                "; this id is a Homebased task id, not a session: use the thread of the session that must receive the events",
            );
        }
        if let Some(parent) = self.parent
            && parent.thread != thread
        {
            message.push_str(&format!(
                "; the parent task {} uses thread {}",
                parent.task, parent.thread
            ));
        }
        if self.parent.is_some_and(|parent| parent.thread == thread) {
            message.push_str(
                "; it is the parent task's thread, which lives on the parent's origin machine: this worker cannot send events there",
            );
        }

        AppError::UnknownThread {
            thread,
            suggestions,
            message,
        }
    }

    /// Every thread id that a session store on this machine names
    ///
    /// A Claude session wins over a Codex thread with the same id, matching
    /// the order in which callback delivery looks them up
    fn known_threads(&self) -> BTreeMap<Uuid, ThreadDestination> {
        let mut known = BTreeMap::new();
        for id in codex_rollout_ids(&self.codex_sessions) {
            known.insert(id, ThreadDestination::CodexThread);
        }
        for id in claude_session_ids(&self.claude_dir) {
            known.insert(id, ThreadDestination::ClaudeSession);
        }
        known
    }
}

/// Look up the worker's own task when `HOMEBASED_TASK_ID` is set
///
/// A worker whose task row is not in this state directory has no parent to
/// compare against, so only the thread's existence is checked
fn parent_task(home: &Home, task_env: Option<OsString>) -> Result<Option<ParentTask>, AppError> {
    let Some(raw) = task_env.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let task: TaskId = raw
        .to_str()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| AppError::Usage {
            message: format!(
                "{TASK_ID_ENV} is not a task UUID: {}",
                raw.to_string_lossy()
            ),
        })?;
    if !home.db_path().is_file() {
        return Ok(None);
    }

    let store = Store::open(&home.db_path())?;
    Ok(store.get_task(task)?.map(|row| ParentTask {
        task,
        thread: row.thread,
    }))
}

/// Known ids that share a long run of digits with `thread`, best first
fn suggestions(
    thread: ThreadId,
    known: &BTreeMap<Uuid, ThreadDestination>,
) -> Vec<ThreadSuggestion> {
    let mut scored: Vec<(usize, ThreadSuggestion)> = known
        .iter()
        .filter_map(|(id, destination)| {
            let score = typo_score(thread.0, *id)?;
            Some((
                score,
                ThreadSuggestion {
                    thread: ThreadId(*id),
                    destination: *destination,
                },
            ))
        })
        .collect();
    scored.sort_by_key(|(score, _)| std::cmp::Reverse(*score));
    scored
        .into_iter()
        .take(MAX_SUGGESTIONS)
        .map(|(_, suggestion)| suggestion)
        .collect()
}

/// How closely `known` matches `typed`, or `None` when it is not a likely typo
///
/// The score is the longest agreeing run: a shared prefix, a shared suffix,
/// or all digits except a few changed ones
fn typo_score(typed: Uuid, known: Uuid) -> Option<usize> {
    let typed = typed.simple().to_string().into_bytes();
    let known = known.simple().to_string().into_bytes();
    let pairs = || typed.iter().zip(known.iter());
    let prefix = pairs().take_while(|(a, b)| a == b).count();
    let suffix = pairs().rev().take_while(|(a, b)| a == b).count();
    let changed = pairs().filter(|(a, b)| a != b).count();
    if changed == 0 {
        return None;
    }

    let run = prefix.max(suffix);
    let same = typed.len() - changed;
    if run >= MIN_SHARED_RUN || changed <= MAX_CHANGED_DIGITS {
        return Some(run.max(same));
    }
    None
}

/// Claude session ids from the registry and from transcript file names
///
/// The registry keeps `<pid>.json` for each session, including stopped ones
/// until Claude Code cleans them up. Transcripts live at
/// `projects/<cwd key>/<session id>.jsonl`
fn claude_session_ids(claude_dir: &Path) -> Vec<Uuid> {
    let mut ids = Vec::new();
    for path in read_dir_paths(&claude_dir.join("sessions")) {
        if path.extension().is_some_and(|ext| ext == "json")
            && let Some(id) = registry_session_id(&path)
        {
            ids.push(id);
        }
    }
    for project in read_dir_paths(&claude_dir.join("projects")) {
        for path in read_dir_paths(&project) {
            if path.extension().is_some_and(|ext| ext == "jsonl")
                && let Some(id) = file_stem_uuid(&path)
            {
                ids.push(id);
            }
        }
    }
    ids
}

fn registry_session_id(path: &Path) -> Option<Uuid> {
    #[derive(serde::Deserialize)]
    struct Record {
        #[serde(rename = "sessionId")]
        session_id: Option<String>,
    }

    let body = fs::read_to_string(path).ok()?;
    let record: Record = serde_json::from_str(&body).ok()?;
    Uuid::parse_str(&record.session_id?).ok()
}

/// Codex thread ids from `rollout-<timestamp>-<thread id>.jsonl` file names
fn codex_rollout_ids(sessions: &Path) -> Vec<Uuid> {
    let mut ids = Vec::new();
    let mut pending = vec![sessions.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for path in read_dir_paths(&directory) {
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if let Some(id) = rollout_id(&path) {
                ids.push(id);
            }
        }
    }
    ids
}

fn rollout_id(path: &Path) -> Option<Uuid> {
    let name = path.file_name()?.to_str()?;
    let stem = name.strip_prefix("rollout-")?.strip_suffix(".jsonl")?;
    let start = stem.len().checked_sub(36)?;
    Uuid::parse_str(stem.get(start..)?).ok()
}

fn file_stem_uuid(path: &Path) -> Option<Uuid> {
    Uuid::parse_str(path.file_stem()?.to_str()?).ok()
}

/// Entries of a directory; a missing or unreadable directory has none
fn read_dir_paths(directory: &Path) -> Vec<PathBuf> {
    match fs::read_dir(directory) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            tracing::debug!("read {}: {error}", directory.display());
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const CLAUDE: &str = "15b9a20f-37c0-4376-ba50-c6cb8045aace";
    const CODEX: &str = "01a0bd16-38b7-7f81-ab2b-43d48c197ee6";
    const MISTYPED: &str = "15b9a20f-37c0-4376-ba2b-43d48c197ee6";

    fn thread(id: &str) -> ThreadId {
        id.parse().unwrap()
    }

    fn origin(parent: Option<ParentTask>) -> (TempDir, SubmitOrigin) {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        let project = root.join(".claude/projects/-home-user-code");
        fs::create_dir_all(&project).unwrap();
        fs::write(project.join(format!("{CLAUDE}.jsonl")), "").unwrap();
        let day = root.join(".codex/sessions/2026/09/19");
        fs::create_dir_all(&day).unwrap();
        fs::write(
            day.join(format!("rollout-2026-09-19T23-32-25-{CODEX}.jsonl")),
            "",
        )
        .unwrap();
        fs::create_dir_all(root.join("state/tasks")).unwrap();

        let origin = SubmitOrigin {
            claude_dir: root.join(".claude"),
            codex_sessions: root.join(".codex/sessions"),
            tasks_dir: root.join("state/tasks"),
            parent,
        };
        (dir, origin)
    }

    #[test]
    fn known_claude_transcript_and_codex_rollout_are_accepted() {
        let (_dir, origin) = origin(None);

        assert_eq!(
            origin.check(thread(CLAUDE), false).unwrap(),
            ThreadDestination::ClaudeSession
        );
        assert_eq!(
            origin.check(thread(CODEX), false).unwrap(),
            ThreadDestination::CodexThread
        );
    }

    #[test]
    fn claude_registry_record_is_accepted_without_a_transcript() {
        let (dir, origin) = origin(None);
        let registered = Uuid::now_v7();
        let sessions = dir.path().join(".claude/sessions");
        fs::create_dir_all(&sessions).unwrap();
        fs::write(
            sessions.join("4242.json"),
            serde_json::json!({ "pid": 4242, "sessionId": registered }).to_string(),
        )
        .unwrap();

        assert_eq!(
            origin.check(ThreadId(registered), false).unwrap(),
            ThreadDestination::ClaudeSession
        );
    }

    #[test]
    fn mistyped_thread_is_rejected_with_the_likely_intended_ids() {
        let (_dir, origin) = origin(None);

        let error = origin.check(thread(MISTYPED), false).unwrap_err();

        assert_eq!(error.code(), "unknown_thread");
        assert_eq!(error.exit_code(), 2);
        let message = error.to_string();
        assert!(message.contains(&format!("thread {MISTYPED}")), "{message}");
        assert!(
            message.contains(&format!("did you mean {CLAUDE} (Claude Code session)")),
            "{message}"
        );
        let input = error.input();
        assert_eq!(input["pointer"], "/thread");
        assert_eq!(input["value"], MISTYPED);
        // the typo also shares a 14-digit tail with an unrelated Codex thread
        assert_eq!(input["suggestions"][0]["thread"], CLAUDE);
        assert_eq!(input["suggestions"][1]["thread"], CODEX);
    }

    #[test]
    fn unrelated_unknown_thread_has_no_suggestion() {
        let (_dir, origin) = origin(None);
        let unknown = Uuid::parse_str("7c1e3a52-9f4d-4b6e-8a21-5d0f93c6b7e4").unwrap();

        let error = origin.check(ThreadId(unknown), false).unwrap_err();

        assert_eq!(error.code(), "unknown_thread");
        assert!(!error.to_string().contains("did you mean"));
        assert_eq!(error.input()["suggestions"], serde_json::json!([]));
    }

    #[test]
    fn task_id_used_as_thread_is_explained() {
        let (dir, origin) = origin(None);
        let task = Uuid::now_v7();
        fs::create_dir_all(dir.path().join(format!("state/tasks/{task}"))).unwrap();

        let error = origin.check(ThreadId(task), false).unwrap_err();

        assert!(
            error.to_string().contains("is a Homebased task id"),
            "{error}"
        );
    }

    #[test]
    fn worker_must_use_its_parent_thread_unless_allowed() {
        let parent = ParentTask {
            task: TaskId(Uuid::now_v7()),
            thread: thread(CLAUDE),
        };
        let (_dir, origin) = origin(Some(parent));

        assert!(origin.check(thread(CLAUDE), false).is_ok());
        let error = origin.check(thread(CODEX), false).unwrap_err();
        assert_eq!(error.code(), "thread_mismatch");
        assert_eq!(error.exit_code(), 2);
        let message = error.to_string();
        assert!(message.contains(CODEX), "{message}");
        assert!(message.contains(CLAUDE), "{message}");
        assert!(message.contains("--allow-other-thread"), "{message}");

        assert_eq!(
            origin.check(thread(CODEX), true).unwrap(),
            ThreadDestination::CodexThread
        );
    }

    #[test]
    fn allowed_other_thread_must_still_exist() {
        let parent = ParentTask {
            task: TaskId(Uuid::now_v7()),
            thread: thread(CLAUDE),
        };
        let (_dir, origin) = origin(Some(parent));

        let error = origin.check(thread(MISTYPED), true).unwrap_err();

        assert_eq!(error.code(), "unknown_thread");
        assert!(
            error
                .to_string()
                .contains(&format!("parent task {}", parent.task))
        );
    }

    #[test]
    fn worker_parent_thread_from_another_machine_is_unknown_here() {
        let remote = Uuid::parse_str("7c1e3a52-9f4d-4b6e-8a21-5d0f93c6b7e4").unwrap();
        let parent = ParentTask {
            task: TaskId(Uuid::now_v7()),
            thread: ThreadId(remote),
        };
        let (_dir, origin) = origin(Some(parent));

        let error = origin.check(ThreadId(remote), false).unwrap_err();

        assert_eq!(error.code(), "unknown_thread");
        assert!(
            error.to_string().contains("parent's origin machine"),
            "{error}"
        );
    }

    #[test]
    fn parent_task_comes_from_the_local_store() {
        let dir = TempDir::new().unwrap();
        let home = Home::resolve(Some(dir.path().to_path_buf())).unwrap();
        home.ensure().unwrap();

        assert_eq!(parent_task(&home, None).unwrap(), None);
        assert_eq!(parent_task(&home, Some("".into())).unwrap(), None);
        assert_eq!(
            parent_task(&home, Some("nope".into())).unwrap_err().code(),
            "usage"
        );
        // a worker whose row is not in this state directory has no parent
        let missing = TaskId(Uuid::now_v7()).to_string();
        assert_eq!(parent_task(&home, Some(missing.into())).unwrap(), None);
    }

    #[test]
    fn typo_score_needs_a_long_shared_run_or_few_changes() {
        let base = Uuid::parse_str(CLAUDE).unwrap();
        let one_digit = Uuid::parse_str("15b9a20f-37c0-4376-ba50-c6cb8045aacf").unwrap();

        assert_eq!(typo_score(base, base), None);
        assert_eq!(typo_score(base, one_digit), Some(31));
        assert!(typo_score(base, Uuid::parse_str(MISTYPED).unwrap()).is_some());
        assert_eq!(
            typo_score(
                base,
                Uuid::parse_str("00000000-0000-4000-8000-000000000000").unwrap()
            ),
            None
        );
    }
}
