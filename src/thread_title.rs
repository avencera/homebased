//! Human titles of agent threads, read from the local agent apps
//!
//! A thread title lives only on the machine that runs the thread, in files that
//! the agent apps own. T3 Code wraps Codex and Claude Code threads under its own
//! title, so its title wins; the agent's own title is the fallback. Every read is
//! read-only and best effort: a missing or unreadable store gives no title

use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Deserialize;
use tracing::debug;

use crate::domain::ThreadId;

/// Time a read waits for a store that another process is writing
const SQLITE_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

// T3 maps native ids through provider threads; threads it imported from the
// previous orchestrator resolve only through the copied session table. See
// `crate::t3::v2`
const T3_TITLE_SQL: &str = "
    SELECT t.title
    FROM orchestration_v2_projection_provider_threads p
    JOIN orchestration_v2_projection_threads t ON t.thread_id = p.thread_id
    WHERE t.deleted_at IS NULL
      AND CASE WHEN json_valid(p.payload_json)
          THEN json_extract(p.payload_json, '$.nativeThreadRef.nativeId') = ?1
          ELSE 0 END
    ORDER BY p.updated_at DESC
    LIMIT 1";
const T3_IMPORTED_TITLE_SQL: &str = "
    SELECT t.title
    FROM provider_session_runtime r
    JOIN orchestration_v2_projection_threads t ON t.thread_id = r.thread_id
    WHERE t.deleted_at IS NULL
      AND CASE WHEN json_valid(r.resume_cursor_json)
          THEN json_extract(r.resume_cursor_json, '$.threadId') = ?1
            OR json_extract(r.resume_cursor_json, '$.resume') = ?1
          ELSE 0 END
    ORDER BY r.last_seen_at DESC
    LIMIT 1";

// `name` is the title the Codex app shows; `title` is the first user message
const CODEX_TITLE_SQL: &str = "SELECT name FROM threads WHERE id = ?1";

/// Local stores that can name a thread
#[derive(Debug, Clone)]
pub struct TitleSources {
    /// T3 Code user data, which holds `statev2.sqlite`
    t3_userdata: PathBuf,
    /// Codex home, `$CODEX_HOME` or `~/.codex`
    codex_home: PathBuf,
    /// Claude Code config directory, `$CLAUDE_CONFIG_DIR` or `~/.claude`
    claude_home: PathBuf,
}

impl TitleSources {
    /// Standard store locations for the user that runs this process
    ///
    /// `None` when `HOME` is not set
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let home = std::env::var_os("HOME")
            .filter(|home| !home.is_empty())
            .map(PathBuf::from)?;
        let env_dir = |name: &str, fallback: &str| {
            std::env::var_os(name)
                .filter(|value| !value.is_empty())
                .map_or_else(|| home.join(fallback), PathBuf::from)
        };

        Some(Self {
            t3_userdata: home.join(".t3/userdata"),
            codex_home: env_dir("CODEX_HOME", ".codex"),
            claude_home: env_dir("CLAUDE_CONFIG_DIR", ".claude"),
        })
    }

    /// Stores under explicit roots
    #[must_use]
    pub fn new(t3_userdata: PathBuf, codex_home: PathBuf, claude_home: PathBuf) -> Self {
        Self {
            t3_userdata,
            codex_home,
            claude_home,
        }
    }

    /// Title of each thread, in input order. Blocking: reads SQLite and JSONL files
    #[must_use]
    pub fn titles(&self, threads: &[ThreadId]) -> Vec<Option<String>> {
        let t3 = open_read_only(&self.t3_userdata.join("statev2.sqlite"));
        let codex = latest_codex_state(&self.codex_home)
            .as_deref()
            .and_then(open_read_only);

        threads
            .iter()
            .map(|thread| {
                let id = thread.to_string();
                t3.as_ref()
                    .and_then(|db| {
                        query_title(db, T3_TITLE_SQL, &id)
                            .or_else(|| query_title(db, T3_IMPORTED_TITLE_SQL, &id))
                    })
                    .or_else(|| {
                        codex
                            .as_ref()
                            .and_then(|db| query_title(db, CODEX_TITLE_SQL, &id))
                    })
                    .or_else(|| claude_title(&self.claude_home, &id))
            })
            .collect()
    }
}

fn open_read_only(path: &Path) -> Option<Connection> {
    if !path.is_file() {
        return None;
    }

    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let connection = Connection::open_with_flags(path, flags)
        .inspect_err(|error| debug!("open {}: {error}", path.display()))
        .ok()?;

    connection.busy_timeout(SQLITE_BUSY_TIMEOUT).ok()?;
    Some(connection)
}

fn query_title(db: &Connection, sql: &str, id: &str) -> Option<String> {
    let title: Option<Option<String>> = db
        .query_row(sql, [id], |row| row.get(0))
        .optional()
        .inspect_err(|error| debug!("thread title query failed: {error}"))
        .ok()?;

    non_empty(title.flatten()?)
}

/// Newest Codex state database: `state_<n>.sqlite` with the highest `n`
fn latest_codex_state(codex_home: &Path) -> Option<PathBuf> {
    fs::read_dir(codex_home)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let name = entry.file_name();
            let version = name
                .to_str()?
                .strip_prefix("state_")?
                .strip_suffix(".sqlite")?
                .parse::<u32>()
                .ok()?;

            Some((version, entry.path()))
        })
        .max_by_key(|(version, _)| *version)
        .map(|(_, path)| path)
}

/// Title line of a Claude Code session transcript
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum ClaudeTitleLine {
    /// Title the user set with a rename
    CustomTitle {
        #[serde(rename = "customTitle")]
        custom_title: String,
    },
    /// Title Claude Code generated from the conversation
    AiTitle {
        #[serde(rename = "aiTitle")]
        ai_title: String,
    },
}

/// Latest user-set title of a Claude Code session, else its latest generated title
fn claude_title(claude_home: &Path, session: &str) -> Option<String> {
    let transcript = claude_transcript(claude_home, session)?;
    let reader = BufReader::new(File::open(transcript).ok()?);
    let mut custom = None;
    let mut generated = None;
    for line in reader.lines().map_while(Result::ok) {
        // most lines are messages; parse only the small title lines
        if !line.contains("-title\"") {
            continue;
        }

        match serde_json::from_str(&line) {
            Ok(ClaudeTitleLine::CustomTitle { custom_title }) => custom = Some(custom_title),
            Ok(ClaudeTitleLine::AiTitle { ai_title }) => generated = Some(ai_title),
            Err(_) => {}
        }
    }

    custom
        .and_then(non_empty)
        .or_else(|| generated.and_then(non_empty))
}

/// `<claude home>/projects/<project>/<session>.jsonl`
fn claude_transcript(claude_home: &Path, session: &str) -> Option<PathBuf> {
    let file_name = format!("{session}.jsonl");
    fs::read_dir(claude_home.join("projects"))
        .ok()?
        .filter_map(Result::ok)
        .map(|project| project.path().join(&file_name))
        .find(|path| path.is_file())
}

fn non_empty(title: String) -> Option<String> {
    let trimmed = title.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::str::FromStr;

    use rusqlite::Connection;
    use tempfile::TempDir;

    use super::TitleSources;
    use crate::domain::ThreadId;
    use crate::t3::test_support::V2State;

    const CODEX_THREAD: &str = "01a0d118-ad25-7f62-9f45-e3548a3fd998";
    const CLAUDE_SESSION: &str = "1bbb2f98-291e-4f40-897d-7169ae99acdd";

    struct Stores {
        dir: TempDir,
    }

    impl Stores {
        fn new() -> Self {
            let dir = TempDir::new().unwrap();
            fs::create_dir_all(dir.path().join("t3")).unwrap();
            fs::create_dir_all(dir.path().join("codex")).unwrap();
            fs::create_dir_all(dir.path().join("claude/projects/-work-app")).unwrap();
            Self { dir }
        }

        fn sources(&self) -> TitleSources {
            TitleSources::new(
                self.dir.path().join("t3"),
                self.dir.path().join("codex"),
                self.dir.path().join("claude"),
            )
        }

        fn codex(&self, version: u32, name: Option<&str>) {
            let path = self
                .dir
                .path()
                .join(format!("codex/state_{version}.sqlite"));
            let db = Connection::open(path).unwrap();
            db.execute_batch(
                "CREATE TABLE threads (id TEXT PRIMARY KEY, title TEXT NOT NULL, name TEXT)",
            )
            .unwrap();
            db.execute(
                "INSERT INTO threads VALUES (?1, 'first user message', ?2)",
                rusqlite::params![CODEX_THREAD, name],
            )
            .unwrap();
        }

        fn claude(&self, lines: &[&str]) {
            let path = self
                .dir
                .path()
                .join(format!("claude/projects/-work-app/{CLAUDE_SESSION}.jsonl"));
            fs::write(path, lines.join("\n")).unwrap();
        }
    }

    fn title(sources: &TitleSources, thread: &str) -> Option<String> {
        sources
            .titles(&[ThreadId::from_str(thread).unwrap()])
            .pop()
            .flatten()
    }

    #[test]
    fn t3_title_wins_over_the_agent_title() {
        let stores = Stores::new();
        V2State::create(&stores.dir.path().join("t3/statev2.sqlite"))
            .thread("t3-a", "T3 codex", false)
            .native("codex", CODEX_THREAD, "t3-a")
            .thread("t3-b", "T3 claude", false)
            .native("claudeAgent", CLAUDE_SESSION, "t3-b");
        stores.codex(5, Some("Codex name"));
        stores.claude(&[r#"{"type":"ai-title","aiTitle":"Claude title"}"#]);
        let sources = stores.sources();

        assert_eq!(title(&sources, CODEX_THREAD).as_deref(), Some("T3 codex"));
        assert_eq!(
            title(&sources, CLAUDE_SESSION).as_deref(),
            Some("T3 claude")
        );
    }

    #[test]
    fn t3_title_covers_imported_threads() {
        let stores = Stores::new();
        V2State::create(&stores.dir.path().join("t3/statev2.sqlite"))
            .thread("t3-old", "Renamed in T3", false)
            .legacy("claudeAgent", "resume", CLAUDE_SESSION, "t3-old");
        stores.claude(&[r#"{"type":"ai-title","aiTitle":"Claude title"}"#]);
        let sources = stores.sources();

        assert_eq!(
            title(&sources, CLAUDE_SESSION).as_deref(),
            Some("Renamed in T3")
        );
    }

    #[test]
    fn codex_name_from_the_newest_state_database() {
        let stores = Stores::new();
        stores.codex(4, Some("old store"));
        stores.codex(5, Some("Codex name"));

        assert_eq!(
            title(&stores.sources(), CODEX_THREAD).as_deref(),
            Some("Codex name")
        );
    }

    #[test]
    fn codex_first_message_is_not_a_title() {
        let stores = Stores::new();
        stores.codex(5, None);

        assert_eq!(title(&stores.sources(), CODEX_THREAD), None);
    }

    #[test]
    fn claude_custom_title_wins_over_later_generated_titles() {
        let stores = Stores::new();
        stores.claude(&[
            r#"{"type":"ai-title","aiTitle":"first guess"}"#,
            r#"{"type":"user","message":"hi"}"#,
            r#"{"type":"custom-title","customTitle":"Renamed"}"#,
            r#"{"type":"ai-title","aiTitle":"later guess"}"#,
        ]);

        assert_eq!(
            title(&stores.sources(), CLAUDE_SESSION).as_deref(),
            Some("Renamed")
        );
    }

    #[test]
    fn claude_latest_generated_title() {
        let stores = Stores::new();
        stores.claude(&[
            r#"{"type":"ai-title","aiTitle":"first guess"}"#,
            r#"{"type":"ai-title","aiTitle":"better guess"}"#,
        ]);

        assert_eq!(
            title(&stores.sources(), CLAUDE_SESSION).as_deref(),
            Some("better guess")
        );
    }

    #[test]
    fn missing_stores_give_no_title() {
        let stores = Stores::new();

        assert_eq!(title(&stores.sources(), CODEX_THREAD), None);
    }
}
