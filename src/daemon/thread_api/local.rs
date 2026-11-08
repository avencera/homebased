//! Thread lookup on this machine: T3 threads, archived ones included, Claude
//! Code sessions and Codex threads outside T3, and homebased worker sessions
//!
//! Everything here reads files and SQLite, so callers run it off the async
//! runtime. Only the matched threads' transcript or rollout tails are read

use std::collections::HashSet;
use std::path::Path;
use std::sync::OnceLock;

use chrono::{DateTime, Utc};

use crate::callback::destination::claude_session_ids;
use crate::callback::stale_context::{CodexRollouts, ContextUse};
use crate::domain::ThreadId;
use crate::machine::MachineId;
use crate::store::WorkerThread;
use crate::t3::{ProviderThread, T3Env, T3Thread, environment_id, thread_index};
use crate::thread_ref::{
    ACTIVE_WITHOUT_T3, Caller, Provider, ThreadContext, ThreadHost, ThreadRef, ThreadState, t3_link,
};
use crate::thread_title::TitleSources;

/// What the daemon knows about this machine's threads before reading any file
pub(crate) struct LocalSources {
    /// Where this machine's T3, Claude, and Codex state lives
    pub(crate) env: T3Env,
    /// This machine
    pub(crate) machine: MachineId,
    /// Its name
    pub(crate) machine_name: String,
    /// Worker sessions of local tasks
    pub(crate) workers: Vec<WorkerThread>,
    /// Threads that homebased tasks or jobs still owe events
    pub(crate) waiting: HashSet<ThreadId>,
    /// Stores that title sessions outside T3
    pub(crate) titles: Option<TitleSources>,
}

/// One snapshot of this machine's threads
pub(crate) struct LocalIndex {
    sources: LocalSources,
    environment: Option<String>,
    t3: Vec<T3Thread>,
    // walking every Codex session or Claude project is skipped unless a lookup needs it
    rollouts: OnceLock<CodexRollouts>,
    claude: OnceLock<Vec<ThreadId>>,
    now: DateTime<Utc>,
}

/// A thread found here, before its context is read
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Found {
    T3(usize),
    Session(ProviderThread),
}

impl LocalIndex {
    /// Read T3's thread index once
    ///
    /// # Errors
    ///
    /// When T3's state exists but cannot be read now. Without its threads a
    /// lookup would report T3 threads as gone, which ends their watches
    pub(crate) fn load(sources: LocalSources, now: DateTime<Utc>) -> Result<Self, String> {
        let t3 = thread_index(&sources.env)?;
        Ok(Self {
            environment: environment_id(&sources.env),
            sources,
            t3,
            rollouts: OnceLock::new(),
            claude: OnceLock::new(),
            now,
        })
    }

    fn home(&self) -> &Path {
        &self.sources.env.home
    }

    fn rollouts(&self) -> &CodexRollouts {
        self.rollouts
            .get_or_init(|| CodexRollouts::scan(&self.sources.env.home))
    }

    fn claude_sessions(&self) -> &[ThreadId] {
        self.claude.get_or_init(|| {
            let mut ids: Vec<ThreadId> = claude_session_ids(&self.home().join(".claude"))
                .into_iter()
                .map(ThreadId)
                .collect();
            ids.sort_unstable_by_key(|id| id.0);
            ids.dedup();
            ids
        })
    }

    /// Every thread here that `reference` names
    ///
    /// A bare UUID matches a T3 id and a session alike, and both on the same
    /// T3 thread count once. With `prefixes`, UUID-shaped text also matches
    /// the start of T3 ids and session ids
    pub(crate) fn find(&self, reference: &ThreadRef, prefixes: bool) -> Vec<ThreadContext> {
        let mut found: Vec<Found> = Vec::new();
        let mut add = |item: Found| {
            if !found.contains(&item) {
                found.push(item);
            }
        };
        match reference {
            // the resolver maps a chip to its thread before asking any machine
            ThreadRef::Chip { .. } => {}
            ThreadRef::T3 {
                environment,
                thread,
            } => {
                // a link from another machine's T3 names nothing here
                if let (Some(wanted), Some(local)) = (environment, &self.environment)
                    && wanted != local
                {
                    return Vec::new();
                }
                let decoded = percent_decode(thread);
                for (index, t3) in self.t3.iter().enumerate() {
                    if t3.id == *thread || decoded.as_deref() == Some(t3.id.as_str()) {
                        add(Found::T3(index));
                    }
                }
            }
            ThreadRef::Id(id) => {
                for (index, t3) in self.t3.iter().enumerate() {
                    if t3.id == *id {
                        add(Found::T3(index));
                    }
                }
                if let Some(uuid) = reference.uuid() {
                    let session = ThreadId(uuid);
                    let mut owned = false;
                    for (index, t3) in self.t3.iter().enumerate() {
                        if t3.session.map(ProviderThread::id) == Some(session) {
                            owned = true;
                            add(Found::T3(index));
                        }
                    }
                    if !owned {
                        if ContextUse::claude_transcript_exists(self.home(), session) {
                            add(Found::Session(ProviderThread::Claude(session)));
                        } else if self.rollouts().ids().any(|id| id == session) {
                            add(Found::Session(ProviderThread::Codex(session)));
                        } else if let Some(worker) = self.worker(session) {
                            // a Claude worker saves no transcript unless asked,
                            // yet its task knows the session it runs
                            add(Found::Session(match worker.provider {
                                Provider::Claude => ProviderThread::Claude(session),
                                Provider::Codex => ProviderThread::Codex(session),
                            }));
                        }
                    }
                } else if prefixes && let Some(prefix) = reference.uuid_prefix() {
                    // a session id prints in lowercase, while a T3 id is opaque
                    // and keeps its case
                    let session_prefix = prefix.to_ascii_lowercase();
                    let session_prefix = session_prefix.as_str();
                    let mut owned = HashSet::new();
                    for (index, t3) in self.t3.iter().enumerate() {
                        let session = t3.session.map(ProviderThread::id);
                        owned.extend(session);
                        let session_matches =
                            session.is_some_and(|id| id.to_string().starts_with(session_prefix));
                        if t3.id.starts_with(prefix) || session_matches {
                            add(Found::T3(index));
                        }
                    }
                    for session in self.claude_sessions() {
                        if !owned.contains(session)
                            && session.to_string().starts_with(session_prefix)
                        {
                            add(Found::Session(ProviderThread::Claude(*session)));
                        }
                    }
                    for session in self.rollouts().ids() {
                        if !owned.contains(&session)
                            && session.to_string().starts_with(session_prefix)
                        {
                            add(Found::Session(ProviderThread::Codex(session)));
                        }
                    }
                    // a worker with no transcript or rollout yet, as for a whole id
                    for worker in &self.sources.workers {
                        if !owned.contains(&worker.thread)
                            && worker.thread.to_string().starts_with(session_prefix)
                        {
                            add(Found::Session(match worker.provider {
                                Provider::Claude => ProviderThread::Claude(worker.thread),
                                Provider::Codex => ProviderThread::Codex(worker.thread),
                            }));
                        }
                    }
                }
            }
        }
        found.into_iter().map(|item| self.context(item)).collect()
    }

    /// The caller's T3 thread here, by its T3 id or by the session T3 runs for it
    pub(crate) fn caller_t3(&self, caller: &Caller) -> Option<String> {
        if let Some(thread) = &caller.t3_thread {
            return Some(thread.clone());
        }
        let session = caller.session?;
        self.t3
            .iter()
            .find(|t3| t3.session.map(ProviderThread::id) == Some(session))
            .map(|t3| t3.id.clone())
    }

    /// T3 threads here that `parent` started as subagents, newest first; with
    /// `live`, only those not settled, archived, or done with their task
    ///
    /// A fork names its source as its parent too, but it is no delegate, so it
    /// is left out: a `--children` compact watch must never reach it
    pub(crate) fn children(&self, parent: &str, live: bool) -> Vec<ThreadContext> {
        self.t3
            .iter()
            .enumerate()
            .filter(|(_, t3)| t3.delegate && t3.parent.as_deref() == Some(parent))
            .map(|(index, _)| self.context(Found::T3(index)))
            .filter(|child| !live || !child.put_away())
            .collect()
    }

    /// The task worker of `session`; follow-ups share a Codex thread, and the
    /// one that runs is the one whose state counts
    fn worker(&self, session: ThreadId) -> Option<&WorkerThread> {
        self.sources
            .workers
            .iter()
            .filter(|worker| worker.thread == session)
            .max_by_key(|worker| worker.running)
    }

    fn context(&self, found: Found) -> ThreadContext {
        let (t3, session) = match found {
            Found::T3(index) => (Some(&self.t3[index]), self.t3[index].session),
            Found::Session(session) => (None, Some(session)),
        };
        let usage = session.and_then(|session| match session {
            ProviderThread::Claude(id) => ContextUse::read_claude_with_compaction(self.home(), id),
            ProviderThread::Codex(id) => self.rollouts().context_with_compaction(id),
        });
        let session_id = session.map(ProviderThread::id);
        let worker = session_id.and_then(|id| self.worker(id));
        // usage needs a request in the tail, so it cannot tell a missing record
        // apart; a running worker's session is live with or without one
        let session_exists = session.map(|session| {
            worker.is_some_and(|worker| worker.running)
                || match session {
                    ProviderThread::Claude(id) => {
                        usage.is_some() || ContextUse::claude_transcript_exists(self.home(), id)
                    }
                    ProviderThread::Codex(id) => {
                        usage.is_some() || self.rollouts().ids().any(|known| known == id)
                    }
                }
        });
        let waiting = session_id.is_some_and(|id| self.sources.waiting.contains(&id));
        let host = match (t3, worker) {
            (Some(_), _) => ThreadHost::T3,
            (None, Some(_)) => ThreadHost::Worker,
            (None, None) => ThreadHost::Terminal,
        };
        let state = match (t3, &usage) {
            (Some(t3), _) if t3.archived => ThreadState::Archived,
            // a homebased task can resume a T3 thread's session, and runs it
            // while T3 records no run
            (Some(t3), _) if t3.running || worker.is_some_and(|worker| worker.running) => {
                ThreadState::Running
            }
            (Some(_), _) if waiting => ThreadState::Waiting,
            (Some(t3), _) if t3.settled => ThreadState::Settled,
            (Some(_), _) => ThreadState::Idle,
            // a running task's agent can sit in a long tool call without writing
            (None, _) if worker.is_some_and(|worker| worker.running) => ThreadState::Running,
            (None, None) => ThreadState::Unknown,
            (None, Some(usage)) if self.now - usage.last_active() < ACTIVE_WITHOUT_T3 => {
                ThreadState::Running
            }
            (None, Some(_)) if waiting => ThreadState::Waiting,
            (None, Some(_)) => ThreadState::Idle,
        };
        let title = match (t3, session_id, &self.sources.titles) {
            (Some(t3), _, _) => Some(t3.title.clone()),
            (None, Some(id), Some(titles)) => titles.titles(&[id]).into_iter().next().flatten(),
            (None, _, _) => None,
        };
        let tokens = usage.as_ref().map(ContextUse::tokens);
        let context_window = usage.as_ref().and_then(ContextUse::context_window);
        let percent = tokens
            .zip(context_window)
            .filter(|(_, window)| *window > 0)
            // display only, so float rounding is fine
            .map(|(tokens, window)| tokens as f64 * 100.0 / window as f64);
        ThreadContext {
            reference: None,
            machine: self.sources.machine,
            machine_name: self.sources.machine_name.clone(),
            host,
            provider: session.map(Into::into),
            session: session_id,
            session_exists,
            t3_thread: t3.map(|t3| t3.id.clone()),
            t3_link: t3
                .zip(self.environment.as_deref())
                .map(|(t3, environment)| t3_link(environment, &t3.id)),
            title,
            state,
            task_finished: t3.is_some_and(|t3| t3.task_finished),
            settled: t3.is_some_and(|t3| t3.settled),
            tokens,
            tokens_estimated: usage.as_ref().is_some_and(ContextUse::tokens_estimated),
            context_window,
            percent,
            last_active: usage.as_ref().map(ContextUse::last_active),
            cache_warm: usage.as_ref().map(|usage| usage.cache_warm(self.now)),
            compacted_at: usage.as_ref().and_then(ContextUse::compacted_at),
            parent_t3_thread: t3.and_then(|t3| t3.parent.clone()),
            details: t3.map(|t3| t3.details.clone()).unwrap_or_default(),
        }
    }
}

/// `text` with `%XX` escapes decoded, when it has any and they decode to UTF-8
///
/// Chip text and links can carry an id escaped, while T3 stores it raw
fn percent_decode(text: &str) -> Option<String> {
    if !text.contains('%') {
        return None;
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && let Some(hex) = text.get(index + 1..index + 3)
            && let Ok(byte) = u8::from_str_radix(hex, 16)
        {
            out.push(byte);
            index += 3;
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::fs;
    use std::path::Path;

    use chrono::{TimeDelta, Utc};
    use serde_json::json;

    use super::{LocalIndex, LocalSources};
    use crate::callback::stale_context::codex_rollout::{rollout, token_count};
    use crate::domain::{TaskId, ThreadId};
    use crate::machine::MachineId;
    use crate::store::WorkerThread;
    use crate::t3::T3Env;
    use crate::t3::test_support::V2State;
    use crate::thread_ref::{Caller, Provider, ThreadContext, ThreadHost, ThreadRef, ThreadState};

    const A: &str = "0199aaaa-1111-7000-8000-000000000001";
    const SESSION_A: &str = "0199aaaa-2222-7000-8000-000000000002";
    const B: &str = "mcp:launch:8bd44cf5";
    const SESSION_B: &str = "0199bbbb-2222-7000-8000-000000000003";
    const C: &str = "thread:delegated-task:a:b";
    const SESSION_C: &str = "0199cccc-2222-7000-8000-000000000004";
    const D: &str = "mcp:0199dddd-1111-7000-8000-000000000005";
    const F: &str = "mcp:launch:feed";
    // a fork of A, which T3 also records with A as its parent
    const G: &str = "mcp:launch:fork";
    // delegates of A whose tasks ended: one done, one sent a new task since,
    // and one whose run still finishes up after its task ended
    const DONE: &str = "thread:delegated-task:done";
    const RESENT: &str = "thread:delegated-task:resent";
    const WRAPPING: &str = "thread:delegated-task:wrapping";
    const TERMINAL: &str = "0199eeee-2222-7000-8000-000000000006";
    const WORKER: &str = "0199ffff-2222-7000-8000-000000000007";

    /// Claude transcript whose last request carried `tokens` `minutes_ago`
    fn transcript(home: &Path, session: &str, minutes_ago: i64, tokens: u64) {
        let project = home.join(".claude/projects/-work");
        fs::create_dir_all(&project).unwrap();
        let line = json!({
            "type": "assistant",
            "timestamp": (Utc::now() - TimeDelta::minutes(minutes_ago)).to_rfc3339(),
            "message": { "usage": { "input_tokens": 2, "cache_read_input_tokens": tokens } }
        });
        fs::write(project.join(format!("{session}.jsonl")), line.to_string()).unwrap();
    }

    fn index(home: &Path) -> LocalIndex {
        let userdata = home.join(".t3/userdata");
        fs::create_dir_all(&userdata).unwrap();
        fs::write(userdata.join("environment-id"), "env-here\n").unwrap();
        let child_of_a = |settled: bool| {
            let mut payload = json!({ "lineage": {
                "parentThreadId": A, "relationshipToParent": "subagent"
            }});
            if settled {
                payload["settledOverride"] = json!("settled");
            }
            payload
        };
        V2State::create(&state_path(home))
            .thread(A, "uuid thread", false)
            .native("codex", SESSION_A, A)
            .thread(B, "launched", false)
            .native("claudeAgent", SESSION_B, B)
            .run(B, 1, "running", "go")
            .thread(C, "delegated", false)
            .payload(C, &child_of_a(true))
            .native("claudeAgent", SESSION_C, C)
            .thread(D, "archived child", true)
            .payload(D, &child_of_a(false))
            // a T3 id that is also another thread's session, which a bare UUID cannot tell apart
            .thread(SESSION_B, "lookalike", false)
            .thread(F, "fresh child", false)
            .payload(F, &child_of_a(false))
            .thread(G, "fork of A", false)
            .payload(
                G,
                &json!({ "lineage": { "parentThreadId": A, "relationshipToParent": "fork" } }),
            )
            .thread(DONE, "done child", false)
            .payload(DONE, &child_of_a(false))
            .task(A, DONE, "node:done", "completed", "2026-11-08T10:00:00Z")
            .thread(RESENT, "resent child", false)
            .payload(RESENT, &child_of_a(false))
            .task(
                A,
                RESENT,
                "node:resent-1",
                "completed",
                "2026-11-08T10:00:00Z",
            )
            .task(
                A,
                RESENT,
                "node:resent-2",
                "running",
                "2026-11-08T11:00:00Z",
            )
            .thread(WRAPPING, "wrapping child", false)
            .payload(WRAPPING, &child_of_a(false))
            .task(
                A,
                WRAPPING,
                "node:wrapping",
                "failed",
                "2026-11-08T10:00:00Z",
            )
            .run(WRAPPING, 1, "running", "go")
            // a follow-up queued behind that run and cancelled hides nothing
            .run(WRAPPING, 2, "cancelled", "more");
        rollout(home, SESSION_A, &[token_count(Utc::now(), 120_000, 0)]);
        transcript(home, SESSION_B, 1, 90_000);
        transcript(home, SESSION_C, 30, 10_000);
        transcript(home, TERMINAL, 10, 200_000);
        rollout(
            home,
            WORKER,
            &[token_count(Utc::now() - TimeDelta::hours(1), 5_000, 0)],
        );
        LocalIndex::load(sources(home), Utc::now()).unwrap()
    }

    fn sources(home: &Path) -> LocalSources {
        LocalSources {
            env: T3Env::new(home.to_path_buf(), "/bin".into()),
            machine: MachineId::new(),
            machine_name: "here".into(),
            workers: vec![WorkerThread {
                thread: WORKER.parse().unwrap(),
                task: TaskId::new(),
                running: true,
                provider: crate::thread_ref::Provider::Codex,
            }],
            waiting: HashSet::from([SESSION_A.parse::<ThreadId>().unwrap()]),
            titles: None,
        }
    }

    fn state_path(home: &Path) -> std::path::PathBuf {
        home.join(".t3/userdata/statev2.sqlite")
    }

    /// What a lookup found, as (T3 id or session, host, state)
    fn found(
        index: &LocalIndex,
        text: &str,
        prefixes: bool,
    ) -> Vec<(String, ThreadHost, ThreadState)> {
        let reference = ThreadRef::parse(text).unwrap();
        // the resolver maps a chip before the index sees it; with no record, by its `thread_` form
        let reference = reference.chip_fallback().unwrap_or(reference);
        index
            .find(&reference, prefixes)
            .into_iter()
            .map(|thread| (key(&thread), thread.host, thread.state))
            .collect()
    }

    fn key(thread: &ThreadContext) -> String {
        thread
            .t3_thread
            .clone()
            .or_else(|| thread.session.map(|session| session.to_string()))
            .unwrap()
    }

    fn t3(id: &str, state: ThreadState) -> Vec<(String, ThreadHost, ThreadState)> {
        vec![(id.to_owned(), ThreadHost::T3, state)]
    }

    #[test]
    fn a_t3_session_a_running_task_resumes_reads_as_running() {
        let home = tempfile::tempdir().unwrap();
        drop(index(home.path()));
        // T3 records no run for a session a homebased task resumed
        let mut sources = sources(home.path());
        sources.workers.push(WorkerThread {
            thread: SESSION_A.parse().unwrap(),
            task: TaskId::new(),
            running: true,
            provider: crate::thread_ref::Provider::Codex,
        });
        let index = LocalIndex::load(sources, Utc::now()).unwrap();
        assert_eq!(found(&index, A, false), t3(A, ThreadState::Running));
    }

    #[test]
    fn every_reference_form_finds_its_thread() {
        let home = tempfile::tempdir().unwrap();
        let index = index(home.path());

        // T3 chip text, `thread_<id>`, both link schemes, and bare T3 ids of every shape
        assert_eq!(
            found(
                &index,
                &format!("[Thread: delegated; ref=thread_{C}]"),
                false
            ),
            t3(C, ThreadState::Settled)
        );
        assert_eq!(
            found(&index, "thread_thread%3Adelegated-task%3Aa%3Ab", false),
            t3(C, ThreadState::Settled)
        );
        assert_eq!(
            found(&index, &format!("thread_{B}"), false),
            t3(B, ThreadState::Running)
        );
        assert_eq!(
            found(&index, &format!("t3-thread://v2/env-here/{A}"), false),
            t3(A, ThreadState::Waiting)
        );
        assert_eq!(
            found(&index, &format!("t3code://threads/env-here/{B}"), false),
            t3(B, ThreadState::Running)
        );
        assert!(found(&index, &format!("t3-thread://v2/env-elsewhere/{A}"), false).is_empty());
        assert_eq!(found(&index, A, false), t3(A, ThreadState::Waiting));
        assert_eq!(found(&index, B, false), t3(B, ThreadState::Running));
        assert_eq!(found(&index, D, false), t3(D, ThreadState::Archived));

        // a provider UUID names the T3 thread that runs it, once
        assert_eq!(found(&index, SESSION_A, false), t3(A, ThreadState::Waiting));
        // and a UUID that is one thread's id and another's session names both
        assert_eq!(found(&index, SESSION_B, false).len(), 2);

        // sessions outside T3: a terminal session, and a running task's worker
        assert_eq!(
            found(&index, TERMINAL, false),
            vec![(TERMINAL.to_owned(), ThreadHost::Terminal, ThreadState::Idle)]
        );
        assert_eq!(
            found(&index, WORKER, false),
            vec![(WORKER.to_owned(), ThreadHost::Worker, ThreadState::Running)]
        );
        assert!(found(&index, "0199aaaa-9999-7000-8000-000000000009", false).is_empty());

        // prefixes match only when asked, and only UUID-shaped text
        assert!(found(&index, &TERMINAL[..8], false).is_empty());
        assert_eq!(
            found(&index, &TERMINAL[..8], true),
            vec![(TERMINAL.to_owned(), ThreadHost::Terminal, ThreadState::Idle)]
        );
        assert_eq!(
            found(&index, "mcp:0199dddd", true),
            t3(D, ThreadState::Archived)
        );
        assert!(found(&index, "mcp:launch:8bd4", true).is_empty());

        let codex = index.find(&ThreadRef::parse(A).unwrap(), false).remove(0);
        assert_eq!(codex.provider, Some(Provider::Codex));
        assert_eq!(codex.tokens, Some(120_000));
        assert_eq!(codex.context_window, Some(258_400));
        assert_eq!(
            codex.t3_link.as_deref(),
            Some(format!("t3-thread://v2/env-here/{A}").as_str())
        );
    }

    #[test]
    fn children_of_a_caller_are_its_delegates_finished_too_unless_live() {
        let home = tempfile::tempdir().unwrap();
        let index = index(home.path());
        let caller = Caller {
            t3_thread: None,
            session: Some(SESSION_A.parse().unwrap()),
        };
        let parent = index.caller_t3(&caller).unwrap();
        assert_eq!(parent, A);

        let all: HashSet<(String, ThreadState, bool)> = index
            .children(&parent, false)
            .iter()
            .map(|child| (key(child), child.state, child.task_finished))
            .collect();
        // the fork G is the user's own thread, never a delegate to watch or
        // compact; the ledger keeps finished children and tells them apart
        assert_eq!(
            all,
            HashSet::from([
                (C.to_owned(), ThreadState::Settled, false),
                (D.to_owned(), ThreadState::Archived, false),
                (F.to_owned(), ThreadState::Idle, false),
                (DONE.to_owned(), ThreadState::Idle, true),
                (RESENT.to_owned(), ThreadState::Idle, false),
                (WRAPPING.to_owned(), ThreadState::Running, false),
            ])
        );
        let mut live = index.children(&parent, true);
        live.sort_by_key(key);
        let live_keys: Vec<String> = live.iter().map(key).collect();
        // F has no task record, so its states alone decide, as before
        assert_eq!(live_keys, [F, RESENT, WRAPPING]);
        assert_eq!(live[0].parent_t3_thread.as_deref(), Some(A));
        // a child T3 has not started yet has no session and no context
        assert_eq!((live[0].session, live[0].tokens), (None, None));
        let done = index.find(&ThreadRef::parse(DONE).unwrap(), false);
        assert!(
            done[0].human_line().contains(", finished]"),
            "{}",
            done[0].human_line()
        );
    }

    #[test]
    fn a_done_child_that_homebased_work_still_owes_events_is_not_live() {
        const FINISHED: &str = "thread:delegated-task:finished-waiting";
        const SESSION_FINISHED: &str = "0199abcd-3333-7000-8000-000000000001";
        const SETTLED: &str = "thread:delegated-task:settled-waiting";
        const SESSION_SETTLED: &str = "0199abcd-3333-7000-8000-000000000002";
        const WORKING: &str = "thread:delegated-task:working-waiting";
        const SESSION_WORKING: &str = "0199abcd-3333-7000-8000-000000000003";
        let home = tempfile::tempdir().unwrap();
        let home = home.path();
        fs::create_dir_all(home.join(".t3/userdata")).unwrap();
        let child = |settled: bool| {
            let mut payload = json!({ "lineage": {
                "parentThreadId": A, "relationshipToParent": "subagent"
            }});
            if settled {
                payload["settledOverride"] = json!("settled");
            }
            payload
        };
        V2State::create(&state_path(home))
            .thread(A, "root", false)
            .native("codex", SESSION_A, A)
            .thread(FINISHED, "finished", false)
            .payload(FINISHED, &child(false))
            .native("codex", SESSION_FINISHED, FINISHED)
            .task(
                A,
                FINISHED,
                "node:finished",
                "completed",
                "2026-11-08T10:00:00Z",
            )
            .thread(SETTLED, "settled", false)
            .payload(SETTLED, &child(true))
            .native("codex", SESSION_SETTLED, SETTLED)
            .thread(WORKING, "working", false)
            .payload(WORKING, &child(false))
            .native("codex", SESSION_WORKING, WORKING);
        // a homebased task owes each child events, so each shows as waiting
        let mut sources = sources(home);
        sources.waiting = [SESSION_FINISHED, SESSION_SETTLED, SESSION_WORKING]
            .into_iter()
            .map(|session| session.parse().unwrap())
            .collect();
        let index = LocalIndex::load(sources, Utc::now()).unwrap();

        let all: HashSet<(String, ThreadState, bool)> = index
            .children(A, false)
            .iter()
            .map(|child| (key(child), child.state, child.done()))
            .collect();
        assert_eq!(
            all,
            HashSet::from([
                (FINISHED.to_owned(), ThreadState::Waiting, true),
                (SETTLED.to_owned(), ThreadState::Waiting, true),
                (WORKING.to_owned(), ThreadState::Waiting, false),
            ])
        );
        let live: HashSet<String> = index.children(A, true).iter().map(key).collect();
        assert_eq!(live, HashSet::from([WORKING.to_owned()]));
    }

    #[test]
    fn a_t3_without_task_tables_reads_every_child_by_its_states() {
        let home = tempfile::tempdir().unwrap();
        drop(index(home.path()));
        rusqlite::Connection::open(state_path(home.path()))
            .unwrap()
            .execute_batch("DROP TABLE orchestration_v2_projection_subagents")
            .unwrap();
        let index = LocalIndex::load(sources(home.path()), Utc::now()).unwrap();
        let live: HashSet<String> = index.children(A, true).iter().map(key).collect();
        assert!(live.contains(DONE), "{live:?}");
    }

    #[test]
    fn a_task_table_that_cannot_be_read_fails_the_lookup() {
        let home = tempfile::tempdir().unwrap();
        drop(index(home.path()));
        // the tables are there, so this is a failed read, not an older T3; a
        // blob keeps its type in a text column, and reads as no thread id
        rusqlite::Connection::open(state_path(home.path()))
            .unwrap()
            .execute(
                "UPDATE orchestration_v2_projection_subagents SET child_thread_id = X'2a'
                 WHERE child_thread_id = ?1",
                [DONE],
            )
            .unwrap();
        assert!(LocalIndex::load(sources(home.path()), Utc::now()).is_err());
    }

    #[test]
    fn a_t3_state_that_cannot_be_read_fails_the_lookup() {
        let home = tempfile::tempdir().unwrap();
        drop(index(home.path()));
        // the thread query reads the runs table, which the schema check does not
        rusqlite::Connection::open(state_path(home.path()))
            .unwrap()
            .execute_batch("DROP TABLE orchestration_v2_projection_runs")
            .unwrap();
        // an empty index would report every T3 thread as deleted
        assert!(LocalIndex::load(sources(home.path()), Utc::now()).is_err());

        // a schema without the provider-thread table fails the schema check itself
        std::fs::remove_file(state_path(home.path())).unwrap();
        drop(index(home.path()));
        rusqlite::Connection::open(state_path(home.path()))
            .unwrap()
            .execute_batch("DROP TABLE orchestration_v2_projection_provider_threads")
            .unwrap();
        assert!(LocalIndex::load(sources(home.path()), Utc::now()).is_err());

        // without T3 at all, sessions outside it are still found
        std::fs::remove_file(state_path(home.path())).unwrap();
        let index = LocalIndex::load(sources(home.path()), Utc::now()).unwrap();
        assert_eq!(found(&index, TERMINAL, false).len(), 1);
    }

    #[test]
    fn a_t3_thread_reads_the_session_t3_runs_next() {
        const SWITCHED: &str = "mcp:launch:switched";
        const OLD: &str = "0199abab-2222-7000-8000-00000000000a";
        const NEW: &str = "0199abab-2222-7000-8000-00000000000b";
        const PENDING: &str = "mcp:launch:pending";
        const PENDING_OLD: &str = "0199abab-2222-7000-8000-00000000000c";
        let home = tempfile::tempdir().unwrap();
        let home = home.path();
        fs::create_dir_all(home.join(".t3/userdata")).unwrap();
        V2State::create(&state_path(home))
            // switched from Claude to Codex, and has run in Codex since
            .thread(SWITCHED, "switched", false)
            .native("claudeAgent", OLD, SWITCHED)
            .instance("codex", "codex", NEW, SWITCHED)
            // switched too, but the next turn has not started its session yet
            .thread(PENDING, "pending", false)
            .native("claudeAgent", PENDING_OLD, PENDING)
            .select("codex", PENDING);
        transcript(home, OLD, 5, 300_000);
        rollout(home, NEW, &[token_count(Utc::now(), 50_000, 0)]);
        transcript(home, PENDING_OLD, 5, 300_000);
        let index = LocalIndex::load(sources(home), Utc::now()).unwrap();

        let switched = index.find(&ThreadRef::parse(SWITCHED).unwrap(), false);
        assert_eq!(switched.len(), 1);
        assert_eq!(switched[0].session, Some(NEW.parse().unwrap()));
        assert_eq!(switched[0].provider, Some(Provider::Codex));
        assert_eq!(switched[0].tokens, Some(50_000));

        // the old session's size says nothing about the next turn, which starts fresh
        let pending = index.find(&ThreadRef::parse(PENDING).unwrap(), false);
        assert_eq!(pending[0].session, None);
        assert_eq!(pending[0].tokens, None);

        // the session T3 left behind is only a transcript now
        assert_eq!(
            found(&index, OLD, false),
            vec![(OLD.to_owned(), ThreadHost::Terminal, ThreadState::Idle)]
        );
    }

    #[test]
    fn a_session_whose_record_is_removed_is_marked_gone_while_t3_still_names_it() {
        let home = tempfile::tempdir().unwrap();
        let index = index(home.path());
        let lookup = |index: &LocalIndex| {
            index
                .find(&ThreadRef::parse(SESSION_B).unwrap(), false)
                .into_iter()
                .find(|thread| thread.t3_thread.as_deref() == Some(B))
                .unwrap()
        };
        assert_eq!(lookup(&index).session_exists, Some(true));

        fs::remove_file(
            home.path()
                .join(format!(".claude/projects/-work/{SESSION_B}.jsonl")),
        )
        .unwrap();
        let index = LocalIndex::load(sources(home.path()), Utc::now()).unwrap();
        let gone = lookup(&index);
        assert_eq!(gone.session, Some(SESSION_B.parse().unwrap()));
        assert_eq!(gone.session_exists, Some(false));
    }
}
